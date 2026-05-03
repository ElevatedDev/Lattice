package com.lattice.internal.wait;

import com.lattice.metrics.WaitMetrics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class WaitStrategyTest {

    @Test
    void functionalInterfaceReceivesIdleCountAndMetrics() {
        final RecordingWaitMetrics metrics = new RecordingWaitMetrics();
        final WaitStrategy strategy = (idleCount, observedMetrics) -> {
            assertSame(metrics, observedMetrics);
            observedMetrics.recordYield();
            return idleCount + 7;
        };

        assertEquals(10, strategy.idle(3, metrics));
        assertEquals(1, metrics.yields);
    }

    private static final class RecordingWaitMetrics implements WaitMetrics {
        private int yields;

        @Override
        public void recordSpin() {
        }

        @Override
        public void recordYield() {
            yields++;
        }

        @Override
        public void recordPark() {
        }
    }
}
