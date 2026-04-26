package com.lattice.wait;

import java.time.Duration;
import java.util.Objects;

/**
 * Describes how a worker or edge writer waits when no immediate progress is
 * possible.
 * <p>
 * Wait specs are immutable and shared safely across graph specs. Busy waiting
 * favors latency and CPU residency; blocking favors lower CPU use; phased
 * waiting moves through spins, yields, and short parks.
 */
public final class WaitSpec {

    private static final Duration DEFAULT_PARK = Duration.ofNanos(500);

    private final WaitKind kind;
    private final int spins;
    private final int yields;
    private final Duration parkNanos;

    private WaitSpec(final WaitKind kind, final int spins, final int yields, final Duration parkNanos) {
        if (spins < 0) {
            throw new IllegalArgumentException("spins must not be negative");
        }
        if (yields < 0) {
            throw new IllegalArgumentException("yields must not be negative");
        }
        if (parkNanos.isNegative()) {
            throw new IllegalArgumentException("parkNanos must not be negative");
        }
        this.kind = Objects.requireNonNull(kind, "kind");
        this.spins = spins;
        this.yields = yields;
        this.parkNanos = Objects.requireNonNull(parkNanos, "parkNanos");
    }

    /**
     * Spins until progress is possible.
     * <p>
     * This is the lowest-latency option when a dedicated CPU is available.
     */
    public static WaitSpec busySpin() {
        return new WaitSpec(WaitKind.BUSY_SPIN, 0, 0, Duration.ZERO);
    }

    /**
     * Returns the default phased wait strategy.
     */
    public static WaitSpec phasedDefault() {
        return phased(10_000, 50, DEFAULT_PARK);
    }

    /**
     * Spins, then yields, then parks for the supplied duration while waiting.
     *
     * @param spins number of spin iterations before yielding
     * @param yields number of yields before parking
     * @param parkNanos park duration; the value is expressed as a
     *                  {@link Duration} and may be zero
     * @return wait spec
     */
    public static WaitSpec phased(final int spins, final int yields, final Duration parkNanos) {
        return new WaitSpec(WaitKind.PHASED, spins, yields, parkNanos);
    }

    /**
     * Uses a blocking wait strategy.
     */
    public static WaitSpec blocking() {
        return new WaitSpec(WaitKind.BLOCKING, 0, 0, Duration.ofMillis(1));
    }

    /**
     * Returns the wait strategy family.
     */
    public WaitKind kind() {
        return kind;
    }

    /**
     * Returns the spin count used by phased waiting.
     */
    public int spins() {
        return spins;
    }

    /**
     * Returns the yield count used by phased waiting.
     */
    public int yields() {
        return yields;
    }

    /**
     * Returns the park duration used by phased and blocking waiting.
     */
    public Duration parkNanos() {
        return parkNanos;
    }

    /**
     * Wait strategy families.
     */
    public enum WaitKind {
        /**
         * Spin without yielding or parking.
         */
        BUSY_SPIN,
        /**
         * Spin, then yield, then park.
         */
        PHASED,
        /**
         * Park while waiting.
         */
        BLOCKING
    }
}
