package com.lattice.wait;

import java.time.Duration;
import java.util.Objects;

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

    public static WaitSpec busySpin() {
        return new WaitSpec(WaitKind.BUSY_SPIN, 0, 0, Duration.ZERO);
    }

    public static WaitSpec phasedDefault() {
        return phased(10_000, 50, DEFAULT_PARK);
    }

    public static WaitSpec phased(final int spins, final int yields, final Duration parkNanos) {
        return new WaitSpec(WaitKind.PHASED, spins, yields, parkNanos);
    }

    public static WaitSpec blocking() {
        return new WaitSpec(WaitKind.BLOCKING, 0, 0, Duration.ofMillis(1));
    }

    public WaitKind kind() {
        return kind;
    }

    public int spins() {
        return spins;
    }

    public int yields() {
        return yields;
    }

    public Duration parkNanos() {
        return parkNanos;
    }

    public enum WaitKind {
        BUSY_SPIN,
        PHASED,
        BLOCKING
    }
}
