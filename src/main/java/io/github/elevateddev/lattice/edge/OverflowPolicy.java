package io.github.elevateddev.lattice.edge;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

/**
 * Immutable policy for writes to a full edge.
 * <p>
 * Policies are interpreted by the runtime edge writer. Some policies are lossy
 * by design and are reflected in {@link io.github.elevateddev.lattice.metrics.EdgeMetrics} and
 * {@link io.github.elevateddev.lattice.metrics.GraphMetrics}.
 */
public final class OverflowPolicy {

    private final OverflowKind kind;
    private final Duration timeout;
    private final Function<Object, ?> coalescingKey;
    private final String redirectTarget;

    private OverflowPolicy(final OverflowKind kind, final Duration timeout) {
        this(kind, timeout, null, null);
    }

    private OverflowPolicy(
        final OverflowKind kind,
        final Duration timeout,
        final Function<Object, ?> coalescingKey,
        final String redirectTarget
    ) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.timeout = timeout;
        this.coalescingKey = coalescingKey;
        this.redirectTarget = redirectTarget;
    }

    /**
     * Blocks until the edge accepts the item.
     */
    public static OverflowPolicy block() {
        return new OverflowPolicy(OverflowKind.BLOCK, null);
    }

    /**
     * Fails the offer immediately when the edge is full.
     */
    public static OverflowPolicy failFast() {
        return new OverflowPolicy(OverflowKind.FAIL_FAST, null);
    }

    /**
     * Alias for {@link #dropLatest()}.
     */
    public static OverflowPolicy dropNewest() {
        return dropLatest();
    }

    /**
     * Drops the offered item when the edge is full.
     */
    public static OverflowPolicy dropLatest() {
        return new OverflowPolicy(OverflowKind.DROP_LATEST, null);
    }

    /**
     * Drops the oldest queued item to make room for the offered item.
     */
    public static OverflowPolicy dropOldest() {
        return new OverflowPolicy(OverflowKind.DROP_OLDEST, null);
    }

    /**
     * Coalesces full-edge offers by a caller-supplied key.
     *
     * @param keyExtractor extracts the coalescing key from offered items
     * @param <T> item type accepted by the edge
     * @return overflow policy
     */
    @SuppressWarnings("unchecked")
    public static <T> OverflowPolicy coalesceBy(final Function<? super T, ?> keyExtractor) {
        Objects.requireNonNull(keyExtractor, "keyExtractor");
        return new OverflowPolicy(OverflowKind.COALESCE, null, item -> keyExtractor.apply((T) item), null);
    }

    /**
     * Redirects full-edge offers to another graph node.
     *
     * @param targetNode target node name
     * @return overflow policy
     */
    public static OverflowPolicy redirectTo(final String targetNode) {
        final String target = Objects.requireNonNull(targetNode, "targetNode").trim();
        if (target.isEmpty()) {
            throw new IllegalArgumentException("targetNode must not be blank");
        }
        return new OverflowPolicy(OverflowKind.REDIRECT, null, null, target);
    }

    /**
     * Blocks for at most the supplied timeout before failing the offer.
     */
    public static OverflowPolicy blockFor(final Duration timeout) {
        final Duration value = Objects.requireNonNull(timeout, "timeout");
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        return new OverflowPolicy(OverflowKind.BLOCK_FOR, value);
    }

    /**
     * Returns the overflow policy kind.
     */
    public OverflowKind kind() {
        return kind;
    }

    /**
     * Returns the timeout for {@link OverflowKind#BLOCK_FOR}, or {@code null}
     * for policies without a timeout.
     */
    public Duration timeout() {
        return timeout;
    }

    /**
     * Returns the coalescing key extractor for {@link OverflowKind#COALESCE},
     * or {@code null} for other policies.
     */
    public Function<Object, ?> coalescingKey() {
        return coalescingKey;
    }

    /**
     * Returns the redirect target for {@link OverflowKind#REDIRECT}, or
     * {@code null} for other policies.
     */
    public String redirectTarget() {
        return redirectTarget;
    }

    /**
     * Full-edge behaviors.
     */
    public enum OverflowKind {
        /**
         * Wait indefinitely for capacity.
         */
        BLOCK,
        /**
         * Fail immediately.
         */
        FAIL_FAST,
        /**
         * Wait up to a timeout.
         */
        BLOCK_FOR,
        /**
         * Drop the newly offered item.
         */
        DROP_LATEST,
        /**
         * Drop the oldest queued item.
         */
        DROP_OLDEST,
        /**
         * Replace pending work that has the same coalescing key.
         */
        COALESCE,
        /**
         * Send the item to another node.
         */
        REDIRECT
    }
}
