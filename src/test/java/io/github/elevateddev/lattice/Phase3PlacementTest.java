package io.github.elevateddev.lattice;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.GraphPlan;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.PlacementStatus;
import io.github.elevateddev.lattice.placement.MemoryMode;
import io.github.elevateddev.lattice.placement.PinPolicy;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Phase3PlacementTest {
    private static final MetricsSpec TEST_METRICS = MetricsSpec.off().hotCounters(true);


    @Test
    void planExposesPlacementAndAllocationOwners() {
        final StaticGraph graph = StaticGraph.builder("phase3-plan")
            .source("ingress", Integer.class)
            .stage("double", Integer.class, Integer.class, (value, out, ctx) -> out.push(value * 2),
                StageSpec.singleThreaded().pin(PinPolicy.cpu(1)))
            .sink("egress", Integer.class, ignored -> { },
                StageSpec.singleThreaded().pin(PinPolicy.numaNode(0)))
            .edge("ingress", "double", EdgeSpec.mpscRing(32).memory(MemoryMode.offHeapHandles()))
            .edge("double", "egress", EdgeSpec.spscRing(32))
            .build();

        final GraphPlan plan = graph.plan();

        assertEquals(2, plan.placements().size());
        assertEquals(1, plan.placement("double").orElseThrow().expectedCpu());
        assertEquals(0, plan.placement("egress").orElseThrow().expectedNumaNode());
        assertEquals("double", plan.edge("ingress", "double").orElseThrow().allocationOwner());
        assertEquals("double", plan.edge("double", "egress").orElseThrow().allocationOwner());
        assertEquals(MemoryMode.MemoryKind.OFF_HEAP_HANDLES,
            plan.edge("ingress", "double").orElseThrow().spec().memoryMode().kind());
    }

    @Test
    void placementFallbackAndFirstTouchRunEndToEnd() throws Exception {
        final List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = StaticGraph.builder("phase3-run")
            .metrics(TEST_METRICS)
            .source("ingress", Integer.class)
            .stage("double", Integer.class, Integer.class, (value, out, ctx) -> out.push(value * 2),
                StageSpec.singleThreaded().pin(PinPolicy.inheritCpuset()))
            .sink("egress", Integer.class, consumed::add,
                StageSpec.singleThreaded().pin(PinPolicy.cpuSet(0)))
            .edge("ingress", "double", EdgeSpec.mpscRing(64).memory(MemoryMode.offHeapHandles()))
            .edge("double", "egress", EdgeSpec.spscRing(64))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 16; i++) {
            ingress.emit(i);
        }
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(16, consumed.size());
        assertTrue(consumed.contains(30));

        final EdgeMetrics ingressEdge = graph.metrics().edge("ingress", "double");
        final EdgeMetrics egressEdge = graph.metrics().edge("double", "egress");
        assertEquals("double", ingressEdge.allocationOwner());
        assertEquals("double", egressEdge.allocationOwner());
        assertEquals(MemoryMode.MemoryKind.OFF_HEAP_HANDLES, ingressEdge.memoryKind());
        assertEquals(1, ingressEdge.firstTouchCount());
        assertEquals(1, egressEdge.firstTouchCount());
        assertTrue(graph.metrics().placementReport().stream()
            .anyMatch(placement -> placement.stageName().equals("egress")));
        assertNotNull(graph.metrics().stage("double").placementStatus());
        assertTrue(graph.metrics().stage("egress").placementStatus() == PlacementStatus.APPLIED
            || graph.metrics().stage("egress").placementStatus() == PlacementStatus.DEGRADED
            || graph.metrics().stage("egress").placementStatus() == PlacementStatus.UNAVAILABLE);
    }
}
