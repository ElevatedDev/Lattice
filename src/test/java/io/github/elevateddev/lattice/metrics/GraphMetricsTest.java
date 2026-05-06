package io.github.elevateddev.lattice.metrics;

import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.placement.MemoryMode;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphMetricsTest {

    @Test
    void constructorSnapshotsTopologyMapsAndExposesImmutableViews() {
        final StageMetrics firstStage = new StageMetrics("first");
        final EdgeMetrics edge = new EdgeMetrics("first", "second");
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        stages.put(firstStage.name(), firstStage);
        edges.put("first->second", edge);

        final GraphMetrics metrics = new GraphMetrics("graph", stages, edges);
        stages.put("later", new StageMetrics("later"));
        edges.clear();

        assertEquals("graph", metrics.graphName());
        assertFalse(metrics.hotCounters());
        assertSame(firstStage, metrics.stage("first"));
        assertSame(edge, metrics.edge("first", "second"));
        assertNull(metrics.stage("missing"));
        assertNull(metrics.edge("first", "missing"));
        assertEquals(List.of("first"), List.copyOf(metrics.stages().keySet()));
        assertEquals(List.of("first->second"), List.copyOf(metrics.edges().keySet()));
        assertThrows(UnsupportedOperationException.class, () -> metrics.stages().put("x", firstStage));
        assertThrows(UnsupportedOperationException.class, () -> metrics.edges().clear());
    }

    @Test
    void hotCounterUpdatesAreIgnoredWhenMetricsAreOffButStateRecords() {
        final GraphMetrics metrics = new GraphMetrics("graph", Map.of(), Map.of(), MetricsSpec.off());

        metrics.recordEmit();
        metrics.recordConsume();
        metrics.recordConsume(3);
        metrics.recordFailedOffer();
        metrics.recordFailedOffers(2);
        metrics.recordBlockedOffer();
        metrics.recordBackpressureNanos(11);
        metrics.recordStageException();
        metrics.recordDroppedMessage();
        metrics.recordRedirectedMessage();
        metrics.recordCoalescedMessage();
        metrics.activateOverload();
        metrics.markStarted();
        metrics.markStopped();

        assertEquals(0, metrics.emittedCount());
        assertEquals(0, metrics.consumedCount());
        assertEquals(0, metrics.failedOffers());
        assertEquals(0, metrics.blockedOffers());
        assertEquals(0, metrics.backpressureNanos());
        assertEquals(0, metrics.stageExceptions());
        assertEquals(0, metrics.droppedMessages());
        assertEquals(0, metrics.redirectedMessages());
        assertEquals(0, metrics.coalescedMessages());
        assertEquals(0, metrics.overloadActivations());
        assertTrue(metrics.overloaded());
        assertNotNull(metrics.startTime());
        assertNotNull(metrics.stopTime());

        metrics.clearOverload();

        assertFalse(metrics.overloaded());
    }

    @Test
    void hotCountersRecordAggregateGraphMetrics() {
        final GraphMetrics metrics = new GraphMetrics(
            "graph",
            Map.of(),
            Map.of(),
            MetricsSpec.off().hotCounters(true)
        );

        metrics.recordEmit();
        metrics.recordConsume();
        metrics.recordConsume(2);
        metrics.recordConsume(0);
        metrics.recordFailedOffer();
        metrics.recordFailedOffers(3);
        metrics.recordFailedOffers(0);
        metrics.recordBlockedOffer();
        metrics.recordBackpressureNanos(11);
        metrics.recordBackpressureNanos(0);
        metrics.recordStageException();
        metrics.recordDroppedMessage();
        metrics.recordRedirectedMessage();
        metrics.recordCoalescedMessage();
        metrics.activateOverload();
        metrics.activateOverload();
        metrics.clearOverload();
        metrics.activateOverload();

        assertTrue(metrics.hotCounters());
        assertEquals(1, metrics.emittedCount());
        assertEquals(3, metrics.consumedCount());
        assertEquals(4, metrics.failedOffers());
        assertEquals(1, metrics.blockedOffers());
        assertEquals(11, metrics.backpressureNanos());
        assertEquals(1, metrics.stageExceptions());
        assertEquals(1, metrics.droppedMessages());
        assertEquals(1, metrics.redirectedMessages());
        assertEquals(1, metrics.coalescedMessages());
        assertTrue(metrics.overloaded());
        assertEquals(2, metrics.overloadActivations());
    }

    @Test
    void lifecycleTimestampsRecordFirstStartAndStopOnly() {
        final GraphMetrics metrics = new GraphMetrics("graph", Map.of(), Map.of());

        metrics.markStarted();
        final Instant firstStart = metrics.startTime();
        metrics.markStarted();
        metrics.markStopped();
        final Instant firstStop = metrics.stopTime();
        metrics.markStopped();

        assertNotNull(firstStart);
        assertNotNull(firstStop);
        assertSame(firstStart, metrics.startTime());
        assertSame(firstStop, metrics.stopTime());
    }

    @Test
    void placementReportReflectsStagePlacementInTopologyOrder() {
        final MetricsSpec hotCounters = MetricsSpec.off().hotCounters(true);
        final StageMetrics alpha = new StageMetrics("alpha", hotCounters);
        final StageMetrics beta = new StageMetrics("beta", hotCounters);
        alpha.recordPlacement(PlacementStatus.APPLIED, "pinned", 1, 1, 0, 0, "alpha", false, false);
        beta.recordPlacement(PlacementStatus.DEGRADED, "fallback", 2, 3, 1, 2, "beta", true, true);
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put(alpha.name(), alpha);
        stages.put(beta.name(), beta);

        final GraphMetrics metrics = new GraphMetrics(
            "graph",
            stages,
            Map.of("alpha->beta", new EdgeMetrics(
                "alpha",
                "beta",
                "",
                MemoryMode.MemoryKind.ON_HEAP_SLOTS,
                hotCounters
            )),
            hotCounters
        );

        final List<GraphMetrics.StagePlacement> report = metrics.placementReport();

        assertEquals(2, report.size());
        assertEquals("alpha", report.get(0).stageName());
        assertSame(PlacementStatus.APPLIED, report.get(0).status());
        assertEquals("pinned", report.get(0).message());
        assertEquals(1, report.get(0).expectedCpu());
        assertEquals(1, report.get(0).observedCpu());
        assertEquals(0, report.get(0).expectedNumaNode());
        assertEquals(0, report.get(0).observedNumaNode());
        assertEquals(0, report.get(0).affinityViolations());
        assertEquals(0, report.get(0).numaViolations());
        assertEquals("beta", report.get(1).stageName());
        assertSame(PlacementStatus.DEGRADED, report.get(1).status());
        assertEquals("fallback", report.get(1).message());
        assertEquals(2, report.get(1).expectedCpu());
        assertEquals(3, report.get(1).observedCpu());
        assertEquals(1, report.get(1).expectedNumaNode());
        assertEquals(2, report.get(1).observedNumaNode());
        assertEquals(1, report.get(1).affinityViolations());
        assertEquals(1, report.get(1).numaViolations());
        assertThrows(UnsupportedOperationException.class, () -> report.add(report.get(0)));
    }
}
