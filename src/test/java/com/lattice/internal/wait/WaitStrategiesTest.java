package com.lattice.internal.wait;

import com.lattice.metrics.WaitMetrics;
import com.lattice.wait.WaitSpec;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WaitStrategiesTest {

    @Test
    void busySpinRecordsSpinAndWrapsIdleCount() {
        final RecordingWaitMetrics metrics = new RecordingWaitMetrics();
        final WaitStrategy strategy = WaitStrategies.busySpin();

        assertEquals(1, strategy.idle(0, metrics));
        assertEquals(0, strategy.idle(Integer.MAX_VALUE, metrics));
        strategy.idle(0, null);

        assertEquals(2, metrics.spins);
        assertEquals(0, metrics.yields);
        assertEquals(0, metrics.parks);
    }

    @Test
    void phasedStrategyTransitionsThroughSpinYieldAndPark() {
        final RecordingWaitMetrics metrics = new RecordingWaitMetrics();
        final WaitStrategy strategy = WaitStrategies.from(WaitSpec.phased(2, 1, Duration.ofNanos(1)));

        assertEquals(1, strategy.idle(0, metrics));
        assertEquals(2, strategy.idle(1, metrics));
        assertEquals(3, strategy.idle(2, metrics));
        assertEquals(4, strategy.idle(3, metrics));
        assertEquals(3, strategy.idle(Integer.MAX_VALUE, metrics));

        assertEquals(2, metrics.spins);
        assertEquals(1, metrics.yields);
        assertEquals(2, metrics.parks);
    }

    @Test
    void zeroParkPhasedStrategyDoesNotRecordParks() {
        final RecordingWaitMetrics metrics = new RecordingWaitMetrics();
        final WaitStrategy strategy = WaitStrategies.from(WaitSpec.phased(1, 1, Duration.ZERO));

        assertEquals(1, strategy.idle(0, metrics));
        assertEquals(2, strategy.idle(1, metrics));
        assertEquals(3, strategy.idle(2, metrics));

        assertEquals(1, metrics.spins);
        assertEquals(1, metrics.yields);
        assertEquals(0, metrics.parks);
    }

    @Test
    void blockingStrategyAlwaysParksAndWrapsToZero() {
        final RecordingWaitMetrics metrics = new RecordingWaitMetrics();
        final WaitStrategy strategy = WaitStrategies.from(WaitSpec.blocking());

        assertEquals(1, strategy.idle(0, metrics));
        assertEquals(0, strategy.idle(Integer.MAX_VALUE, metrics));

        assertEquals(0, metrics.spins);
        assertEquals(0, metrics.yields);
        assertEquals(2, metrics.parks);
    }

    private static final class RecordingWaitMetrics implements WaitMetrics {
        private int spins;
        private int yields;
        private int parks;

        @Override
        public void recordSpin() {
            spins++;
        }

        @Override
        public void recordYield() {
            yields++;
        }

        @Override
        public void recordPark() {
            parks++;
        }
    }
}
