package com.lattice.metrics;

import com.lattice.graph.MetricsSpec;
import com.lattice.placement.MemoryMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WaitMetricsTest {

    @Test
    void stageMetricsImplementsWaitCounterSinkWhenHotCountersAreEnabled() {
        final StageMetrics stage = new StageMetrics("stage", MetricsSpec.off().hotCounters(true));
        final WaitMetrics waitMetrics = stage;

        waitMetrics.recordSpin();
        waitMetrics.recordYield();
        waitMetrics.recordPark();

        assertEquals(1, stage.spinCount());
        assertEquals(1, stage.yieldCount());
        assertEquals(1, stage.parkCount());
    }

    @Test
    void edgeMetricsImplementsWaitCounterSinkWhenHotCountersAreEnabled() {
        final EdgeMetrics edge = new EdgeMetrics(
            "source",
            "sink",
            "",
            MemoryMode.MemoryKind.ON_HEAP_SLOTS,
            MetricsSpec.off().hotCounters(true)
        );
        final WaitMetrics waitMetrics = edge;

        waitMetrics.recordSpin();
        waitMetrics.recordYield();
        waitMetrics.recordPark();

        assertEquals(1, edge.spinCount());
        assertEquals(1, edge.yieldCount());
        assertEquals(1, edge.parkCount());
    }

    @Test
    void waitCounterSinkIsNoOpWhenHotCountersAreDisabled() {
        final StageMetrics stage = new StageMetrics("stage", MetricsSpec.off());
        final EdgeMetrics edge = new EdgeMetrics("source", "sink");

        recordOneWaitCycle(stage);
        recordOneWaitCycle(edge);

        assertEquals(0, stage.spinCount());
        assertEquals(0, stage.yieldCount());
        assertEquals(0, stage.parkCount());
        assertEquals(0, edge.spinCount());
        assertEquals(0, edge.yieldCount());
        assertEquals(0, edge.parkCount());
    }

    private static void recordOneWaitCycle(final WaitMetrics waitMetrics) {
        waitMetrics.recordSpin();
        waitMetrics.recordYield();
        waitMetrics.recordPark();
    }
}
