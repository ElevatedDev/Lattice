package com.lattice.metrics;

import com.lattice.graph.MetricsSpec;
import com.lattice.placement.MemoryMode;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeMetricsTest {

    @Test
    void defaultConstructorCreatesOnHeapMetricsWithObservabilityOff() {
        final EdgeMetrics metrics = new EdgeMetrics("source", "sink");

        assertEquals("source", metrics.from());
        assertEquals("sink", metrics.to());
        assertEquals("", metrics.allocationOwner());
        assertEquals(MemoryMode.MemoryKind.ON_HEAP_SLOTS, metrics.memoryKind());
        assertFalse(metrics.hotCounters());
        assertFalse(metrics.residenceTiming());
        assertFalse(EdgeMetrics.hotCountersEnabled());
        assertFalse(EdgeMetrics.residenceTimingEnabled());
        assertEquals(0, metrics.residenceTimeNanosHistogram().getTotalCount());
    }

    @Test
    void hotCounterUpdatesAreIgnoredWhenMetricsAreOff() {
        final EdgeMetrics metrics = new EdgeMetrics(
            "source",
            "sink",
            "owner",
            MemoryMode.MemoryKind.OFF_HEAP_HANDLES,
            MetricsSpec.off()
        );

        metrics.recordEmit();
        metrics.recordConsume();
        metrics.recordConsume(3);
        metrics.recordFailedOffer();
        metrics.recordFailedOffers(2);
        metrics.recordBlockedOffer();
        metrics.recordBackpressureNanos(11);
        metrics.recordDroppedLatest();
        metrics.recordDroppedOldest();
        metrics.recordCoalescedOffer();
        metrics.recordRedirectedOffer();
        metrics.recordBranchIsolationAction();
        metrics.recordLaneSelection();
        metrics.recordHotKeySignal();
        metrics.recordSpin();
        metrics.recordYield();
        metrics.recordPark();
        metrics.recordFirstTouch(17);
        metrics.recordDepth(19);

        assertEquals("owner", metrics.allocationOwner());
        assertEquals(MemoryMode.MemoryKind.OFF_HEAP_HANDLES, metrics.memoryKind());
        assertEquals(0, metrics.emittedCount());
        assertEquals(0, metrics.consumedCount());
        assertEquals(0, metrics.failedOffers());
        assertEquals(0, metrics.blockedOffers());
        assertEquals(0, metrics.backpressureNanos());
        assertEquals(0, metrics.droppedLatest());
        assertEquals(0, metrics.droppedOldest());
        assertEquals(0, metrics.coalescedOffers());
        assertEquals(0, metrics.redirectedOffers());
        assertEquals(0, metrics.branchIsolationActions());
        assertEquals(0, metrics.laneSelections());
        assertEquals(0, metrics.hotKeySignals());
        assertEquals(0, metrics.spinCount());
        assertEquals(0, metrics.yieldCount());
        assertEquals(0, metrics.parkCount());
        assertEquals(0, metrics.firstTouchCount());
        assertEquals(0, metrics.firstTouchNanos());
        assertEquals(0, metrics.depth());
        assertEquals(0, metrics.highWaterMark());
    }

    @Test
    void hotCountersRecordMonotonicEdgeAndWaitMetrics() {
        final EdgeMetrics metrics = new EdgeMetrics(
            "source",
            "sink",
            "owner",
            MemoryMode.MemoryKind.OFF_HEAP_HANDLES,
            MetricsSpec.off().hotCounters(true)
        );

        metrics.recordEmit();
        metrics.recordEmit();
        metrics.recordEmit();
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
        metrics.recordDroppedLatest();
        metrics.recordDroppedOldest();
        metrics.recordCoalescedOffer();
        metrics.recordRedirectedOffer();
        metrics.recordBranchIsolationAction();
        metrics.recordLaneSelection();
        metrics.recordHotKeySignal();
        metrics.recordSpin();
        metrics.recordYield();
        metrics.recordPark();
        metrics.recordFirstTouch(0);
        metrics.recordFirstTouch(17);
        metrics.recordDepth(9);
        metrics.recordDepth(3);

        assertTrue(metrics.hotCounters());
        assertEquals(4, metrics.emittedCount());
        assertEquals(3, metrics.consumedCount());
        assertEquals(4, metrics.failedOffers());
        assertEquals(1, metrics.blockedOffers());
        assertEquals(11, metrics.backpressureNanos());
        assertEquals(1, metrics.droppedLatest());
        assertEquals(1, metrics.droppedOldest());
        assertEquals(1, metrics.coalescedOffers());
        assertEquals(1, metrics.redirectedOffers());
        assertEquals(1, metrics.branchIsolationActions());
        assertEquals(1, metrics.laneSelections());
        assertEquals(1, metrics.hotKeySignals());
        assertEquals(1, metrics.spinCount());
        assertEquals(1, metrics.yieldCount());
        assertEquals(1, metrics.parkCount());
        assertEquals(2, metrics.firstTouchCount());
        assertEquals(17, metrics.firstTouchNanos());
        assertEquals(0, metrics.depth());
        assertEquals(9, metrics.highWaterMark());
        assertTrue(metrics.offerRatePerSecond() > 0.0d);
        assertTrue(metrics.pollRatePerSecond() > 0.0d);
    }

    @Test
    void residenceHistogramIsOptInAndReturnedAsDefensiveCopy() {
        final EdgeMetrics disabled = new EdgeMetrics(
            "source",
            "sink",
            "",
            MemoryMode.MemoryKind.ON_HEAP_SLOTS,
            MetricsSpec.off()
        );
        disabled.recordResidenceNanos(99);

        assertEquals(0, disabled.residenceSamples());
        assertEquals(0, disabled.residenceTimeNanosHistogram().getTotalCount());

        final EdgeMetrics enabled = new EdgeMetrics(
            "source",
            "sink",
            "",
            MemoryMode.MemoryKind.ON_HEAP_SLOTS,
            MetricsSpec.off().residenceTiming(true)
        );
        enabled.recordResidenceNanos(0);
        enabled.recordResidenceNanos(99);
        enabled.recordResidenceNanos(Long.MAX_VALUE);

        assertTrue(enabled.residenceTiming());
        assertEquals(2, enabled.residenceSamples());
        assertEquals(2, enabled.residenceTimeNanosHistogram().getTotalCount());

        final Histogram snapshot = enabled.residenceTimeNanosHistogram();
        snapshot.recordValue(7);

        assertEquals(2, enabled.residenceTimeNanosHistogram().getTotalCount());
    }
}
