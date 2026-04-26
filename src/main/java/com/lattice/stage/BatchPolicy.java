package com.lattice.stage;

import java.time.Duration;
import java.util.Objects;

/**
 * Describes how a batch stage collects input before invoking user logic.
 * <p>
 * A disabled policy delivers single messages. Max-item and linger policies
 * allow the runtime to deliver up to {@link #maxItems()} messages per callback.
 */
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

    /**
     * Disables batch delivery.
     */
    public static BatchPolicy disabled() {
        return new BatchPolicy(BatchKind.DISABLED, 0, Duration.ZERO);
    }

    /**
     * Delivers a batch once up to {@code maxItems} are available.
     */
    public static BatchPolicy maxItems(final int maxItems) {
        if (maxItems <= 0) {
            throw new IllegalArgumentException("maxItems must be positive");
        }
        return new BatchPolicy(BatchKind.MAX_ITEMS, maxItems, Duration.ZERO);
    }

    /**
     * Delivers up to the default item limit, waiting no longer than
     * {@code linger} for more input.
     */
    public static BatchPolicy linger(final Duration linger) {
        return linger(DEFAULT_LINGER_MAX_ITEMS, linger);
    }

    /**
     * Delivers up to {@code maxItems}, waiting no longer than {@code linger}
     * for more input.
     */
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

    /**
     * Returns the batching mode.
     */
    public BatchKind kind() {
        return kind;
    }

    /**
     * Returns the configured max item count, or {@code 0} when batching is
     * disabled.
     */
    public int maxItems() {
        return maxItems;
    }

    /**
     * Returns the linger duration, or {@link Duration#ZERO} when the policy does
     * not wait for more items.
     */
    public Duration linger() {
        return linger;
    }

    /**
     * Batch collection modes.
     */
    public enum BatchKind {
        /**
         * Deliver one item at a time.
         */
        DISABLED,
        /**
         * Deliver when a maximum item count is reached.
         */
        MAX_ITEMS,
        /**
         * Deliver when either item count or linger time is reached.
         */
        LINGER
    }
}
