package io.github.elevateddev.lattice;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.GraphPlan;
import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import io.github.elevateddev.lattice.metrics.PlacementStatus;
import io.github.elevateddev.lattice.metrics.StageMetrics;
import io.github.elevateddev.lattice.metrics.WorkerState;
import io.github.elevateddev.lattice.placement.MemoryMode;
import io.github.elevateddev.lattice.placement.PinPolicy;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlacementPlanAndMetricsContractTest {

    @Test
    void graphPlanIsAnImmutableInspectableSnapshot() {
        final StaticGraph graph = StaticGraph.builder("plan-contract")
            .source("ingress", Integer.class, SourceMode.SINGLE_PRODUCER)
            .stage("plus-one", Integer.class, Integer.class, (value, out, ctx) -> out.push(value + 1),
                StageSpec.singleThreaded().pin(PinPolicy.cpuSet(1, 3)))
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded().pin(PinPolicy.numaNode(2)))
            .edge("ingress", "plus-one", EdgeSpec.mpscRing(8))
            .edge("plus-one", "egress", EdgeSpec.spscRing(8))
            .build();

        final GraphPlan plan = graph.plan();

        assertEquals("plan-contract", plan.name());
        assertEquals(List.of("plus-one", "egress"), plan.workerOrder());
        assertEquals(GraphPlan.NodeKind.SOURCE, plan.node("ingress").orElseThrow().kind());
        assertEquals(GraphPlan.NodeKind.STAGE, plan.node("plus-one").orElseThrow().kind());
        assertEquals(GraphPlan.NodeKind.SINK, plan.node("egress").orElseThrow().kind());
        assertTrue(plan.node("missing").isEmpty());

        final GraphPlan.Edge ingress = plan.edge("ingress", "plus-one").orElseThrow();
        assertEquals(Integer.class, ingress.messageType());
        assertEquals(EdgeSpec.EdgeKind.SPSC_RING, ingress.spec().kind());
        assertEquals("plus-one", ingress.allocationOwner());
        assertEquals(0, ingress.branchIndex());
        assertFalse(ingress.redirectOnly());
        assertTrue(plan.edge("plus-one", "missing").isEmpty());

        assertThrows(UnsupportedOperationException.class, () -> plan.nodes().add(null));
        assertThrows(UnsupportedOperationException.class, () -> plan.edges().clear());
        assertThrows(UnsupportedOperationException.class, () -> plan.workerOrder().add("later"));
        assertThrows(UnsupportedOperationException.class, () -> plan.placements().clear());
    }

    @Test
    void graphPlanPlacementExpectedCpuSetsAreDefensiveCopies() {
        final BitSet cpus = new BitSet();
        cpus.set(2);
        final GraphPlan.Placement placement = GraphPlan.Placement.from("worker", PinPolicy.cpuSet(cpus));
        cpus.set(5);

        assertEquals("worker", placement.stageName());
        assertEquals(PinPolicy.PinKind.CPU_SET, placement.pinPolicy().kind());
        assertEquals(-1, placement.expectedCpu());
        assertEquals(-1, placement.expectedNumaNode());
        assertTrue(placement.expectedCpuSet().get(2));
        assertFalse(placement.expectedCpuSet().get(5));

        final BitSet returned = placement.expectedCpuSet();
        returned.set(7);
        assertFalse(placement.expectedCpuSet().get(7));

        assertEquals(4, GraphPlan.Placement.from("cpu", PinPolicy.cpu(4)).expectedCpu());
        assertEquals(4, GraphPlan.Placement.from("core", PinPolicy.core(4)).expectedCpu());
        assertEquals(1, GraphPlan.Placement.from("numa", PinPolicy.numaNode(1)).expectedNumaNode());
        assertTrue(GraphPlan.Placement.from("inherit", PinPolicy.inheritCpuset()).inheritsCpuset());
        assertThrows(IllegalArgumentException.class, () -> new GraphPlan(" ", List.of(), List.of(), List.of()));
        assertThrows(IllegalArgumentException.class,
            () -> new GraphPlan.Node(" ", GraphPlan.NodeKind.SOURCE, null, Integer.class, null));
        assertThrows(IllegalArgumentException.class,
            () -> new GraphPlan.Edge(" ", "to", Integer.class, EdgeSpec.spscRing(1)));
        assertThrows(IllegalArgumentException.class,
            () -> GraphPlan.Placement.from(" ", PinPolicy.none()));
    }

    @Test
    void metricsHotCountersAreExplicitOptInButStateMetadataAlwaysRecords() {
        final MetricsSpec off = MetricsSpec.off();
        final StageMetrics stage = new StageMetrics("stage", off);
        final EdgeMetrics edge = new EdgeMetrics("from", "to", "owner", MemoryMode.MemoryKind.OFF_HEAP_HANDLES, off);
        final GraphMetrics graph = graphMetrics("metrics-off", stage, edge, off);

        stage.recordConsume();
        stage.recordEmit();
        stage.recordException();
        stage.recordBatch(3, 99);
        stage.recordSpin();
        stage.recordPlacement(PlacementStatus.APPLIED, "ok", 1, 1, -1, -1, "owner", true, true);
        stage.workerState(WorkerState.RUNNING);
        stage.markStarted();
        stage.markStopped();

        edge.recordEmit();
        edge.recordConsume();
        edge.recordFailedOffer();
        edge.recordDroppedOldest();
        edge.recordSpin();
        edge.recordResidenceNanos(99);
        edge.recordFirstTouch(100);

        graph.recordEmit();
        graph.recordConsume();
        graph.recordFailedOffer();
        graph.recordStageException();
        graph.activateOverload();
        graph.recordDroppedMessage();
        graph.markStarted();
        graph.markStopped();

        assertFalse(stage.hotCounters());
        assertEquals(0, stage.consumedCount());
        assertEquals(0, stage.emittedCount());
        assertEquals(0, stage.stageExceptions());
        assertEquals(0, stage.processedMessages());
        assertEquals(0, stage.spinCount());
        assertEquals(0, stage.affinityViolations());
        assertEquals(PlacementStatus.APPLIED, stage.placementStatus());
        assertEquals("ok", stage.placementMessage());
        assertEquals(1, stage.expectedCpu());
        assertEquals("owner", stage.allocationOwner());
        assertEquals(WorkerState.RUNNING, stage.workerState());
        assertNotNull(stage.startTime());
        assertNotNull(stage.stopTime());
        assertEquals(0.0d, new StageMetrics("new").processRatePerSecond());

        assertFalse(edge.hotCounters());
        assertFalse(edge.residenceTiming());
        assertEquals("owner", edge.allocationOwner());
        assertEquals(MemoryMode.MemoryKind.OFF_HEAP_HANDLES, edge.memoryKind());
        assertEquals(0, edge.emittedCount());
        assertEquals(0, edge.consumedCount());
        assertEquals(0, edge.failedOffers());
        assertEquals(0, edge.droppedOldest());
        assertEquals(0, edge.spinCount());
        assertEquals(0, edge.residenceSamples());
        assertEquals(0, edge.firstTouchCount());

        assertFalse(graph.hotCounters());
        assertEquals(0, graph.emittedCount());
        assertEquals(0, graph.consumedCount());
        assertEquals(0, graph.failedOffers());
        assertEquals(0, graph.stageExceptions());
        assertEquals(0, graph.droppedMessages());
        assertTrue(graph.overloaded());
        graph.clearOverload();
        assertFalse(graph.overloaded());
        assertNotNull(graph.startTime());
        assertNotNull(graph.stopTime());
    }

    @Test
    void metricsMapsReportsAndHistogramsAreDefensiveSnapshots() {
        final MetricsSpec metricsSpec = MetricsSpec.off()
            .hotCounters(true)
            .residenceTiming(true)
            .stageHistograms(true);
        final StageMetrics stage = new StageMetrics("stage", metricsSpec);
        final EdgeMetrics edge = new EdgeMetrics("from", "to", "", MemoryMode.MemoryKind.ON_HEAP_SLOTS, metricsSpec);
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        stages.put("stage", stage);
        edges.put("from->to", edge);
        final GraphMetrics graph = new GraphMetrics("metrics", stages, edges, metricsSpec);
        stages.clear();
        edges.clear();

        assertEquals(stage, graph.stage("stage"));
        assertEquals(edge, graph.edge("from", "to"));
        assertNull(graph.stage("missing"));
        assertNull(graph.edge("missing", "to"));
        assertThrows(UnsupportedOperationException.class, () -> graph.stages().put("other", stage));
        assertThrows(UnsupportedOperationException.class, () -> graph.edges().clear());

        stage.recordBatch(2, 1_000);
        edge.recordEmit();
        edge.recordEmit();
        edge.recordConsume();
        edge.recordDroppedOldest();
        edge.recordResidenceNanos(1_000);
        edge.recordDepth(9);
        stage.recordPlacement(PlacementStatus.DEGRADED, "fallback", 1, 2, 3, 4, "owner", true, true);

        assertEquals(1, stage.batchesProcessed());
        assertEquals(2, stage.processedMessages());
        assertEquals(1, stage.batchSizeHistogram().getTotalCount());
        assertEquals(1, stage.serviceTimeNanosHistogram().getTotalCount());
        assertEquals(1, edge.residenceSamples());
        assertEquals(1, edge.residenceTimeNanosHistogram().getTotalCount());
        assertEquals(0, edge.depth());
        assertEquals(9, edge.highWaterMark());

        final Histogram batchSnapshot = stage.batchSizeHistogram();
        batchSnapshot.recordValue(5);
        assertEquals(1, stage.batchSizeHistogram().getTotalCount());
        final Histogram residenceSnapshot = edge.residenceTimeNanosHistogram();
        residenceSnapshot.recordValue(5);
        assertEquals(1, edge.residenceTimeNanosHistogram().getTotalCount());

        final List<GraphMetrics.StagePlacement> report = graph.placementReport();
        assertEquals(1, report.size());
        assertEquals("stage", report.get(0).stageName());
        assertEquals(PlacementStatus.DEGRADED, report.get(0).status());
        assertEquals("fallback", report.get(0).message());
        assertEquals(1, report.get(0).expectedCpu());
        assertEquals(2, report.get(0).observedCpu());
        assertEquals(3, report.get(0).expectedNumaNode());
        assertEquals(4, report.get(0).observedNumaNode());
        assertEquals(1, report.get(0).affinityViolations());
        assertEquals(1, report.get(0).numaViolations());
        assertThrows(UnsupportedOperationException.class, () -> report.add(report.get(0)));
    }

    private static GraphMetrics graphMetrics(
        final String name,
        final StageMetrics stage,
        final EdgeMetrics edge,
        final MetricsSpec metricsSpec
    ) {
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put(stage.name(), stage);
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put(edge.from() + "->" + edge.to(), edge);
        return new GraphMetrics(name, stages, edges, metricsSpec);
    }
}
