package com.lattice.stage;

import java.time.Duration;
import java.util.Objects;

public final class BatchPolicy {

    private static final int DEFAULT_LINGER_MAX_ITEMS = 64;

    private final BatchKind kind;
    private final int maxItems;
    private final Duration linger;

    private BatchPolicy(final BatchKind kind, final int maxItems, final Duration linger) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.maxItems = maxItems;
        this.linger = linger;
    }

    public static BatchPolicy disabled() {
        return new BatchPolicy(BatchKind.DISABLED, 0, Duration.ZERO);
    }

    public static BatchPolicy maxItems(final int maxItems) {
        if (maxItems <= 0) {
            throw new IllegalArgumentException("maxItems must be positive");
        }
        return new BatchPolicy(BatchKind.MAX_ITEMS, maxItems, Duration.ZERO);
    }

    public static BatchPolicy linger(final Duration linger) {
        return linger(DEFAULT_LINGER_MAX_ITEMS, linger);
    }

    public static BatchPolicy linger(final int maxItems, final Duration linger) {
        if (maxItems <= 0) {
            throw new IllegalArgumentException("maxItems must be positive");
        }
        final Duration value = Objects.requireNonNull(linger, "linger");
        if (value.isNegative()) {
            throw new IllegalArgumentException("linger must not be negative");
        }
        return new BatchPolicy(BatchKind.LINGER, maxItems, value);
    }

    public BatchKind kind() {
        return kind;
    }

    public int maxItems() {
        return maxItems;
    }

    public Duration linger() {
        return linger;
    }

    public enum BatchKind {
        DISABLED,
        MAX_ITEMS,
        LINGER
    }
}
