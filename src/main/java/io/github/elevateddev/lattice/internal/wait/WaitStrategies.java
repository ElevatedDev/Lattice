package io.github.elevateddev.lattice.internal.wait;

import io.github.elevateddev.lattice.metrics.WaitMetrics;
import io.github.elevateddev.lattice.wait.WaitSpec;
import java.util.concurrent.locks.LockSupport;

public final class WaitStrategies {

    private WaitStrategies() {
    }

    public static WaitStrategy from(final WaitSpec spec) {
        return switch (spec.kind()) {
            case BUSY_SPIN -> busySpin();
            case PHASED -> new PhasedWaitStrategy(spec.spins(), spec.yields(), spec.parkNanos().toNanos());
            case BLOCKING -> new BlockingWaitStrategy(spec.parkNanos().toNanos());
        };
    }

    public static WaitStrategy busySpin() {
        return (idleCount, metrics) -> {
            if (metrics != null) {
                metrics.recordSpin();
            }
            Thread.onSpinWait();
            return idleCount == Integer.MAX_VALUE ? 0 : idleCount + 1;
        };
    }

    private static final class PhasedWaitStrategy implements WaitStrategy {
        private final int spins;
        private final int yields;
        private final long parkNanos;

        private PhasedWaitStrategy(final int spins, final int yields, final long parkNanos) {
            this.spins = spins;
            this.yields = yields;
            this.parkNanos = parkNanos;
        }

        @Override
        public int idle(final int idleCount, final WaitMetrics metrics) {
            if (idleCount < spins) {
                if (metrics != null) {
                    metrics.recordSpin();
                }
                Thread.onSpinWait();
            } else if (idleCount < spins + yields) {
                if (metrics != null) {
                    metrics.recordYield();
                }
                Thread.yield();
            } else if (parkNanos > 0) {
                if (metrics != null) {
                    metrics.recordPark();
                }
                LockSupport.parkNanos(parkNanos);
            }

            return idleCount == Integer.MAX_VALUE ? spins + yields : idleCount + 1;
        }
    }

    private static final class BlockingWaitStrategy implements WaitStrategy {

        private final long parkNanos;

        private BlockingWaitStrategy(final long parkNanos) {
            this.parkNanos = Math.max(1L, parkNanos);
        }

        @Override
        public int idle(final int idleCount, final WaitMetrics metrics) {
            if (metrics != null) {
                metrics.recordPark();
            }
            LockSupport.parkNanos(parkNanos);
            return idleCount == Integer.MAX_VALUE ? 0 : idleCount + 1;
        }
    }
}
