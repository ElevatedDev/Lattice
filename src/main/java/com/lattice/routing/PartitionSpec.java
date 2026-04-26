package com.lattice.routing;

import java.util.Objects;
import java.util.function.Function;

/**
 * Configures keyed partitioning across a fixed number of lanes.
 * <p>
 * Messages with equal extracted keys are assigned to the same lane. Hot-key
 * thresholds are advisory metrics signals for skew detection.
 *
 * @param <T> message type
 * @param <K> key type
 */
public final class PartitionSpec<T, K> {

    private final Function<? super T, ? extends K> keyExtractor;
    private final int lanes;
    private final long hotKeyThreshold;

    private PartitionSpec(
        final Function<? super T, ? extends K> keyExtractor,
        final int lanes,
        final long hotKeyThreshold
    ) {
        if (lanes <= 0) {
            throw new IllegalArgumentException("lanes must be positive");
        }
        this.keyExtractor = Objects.requireNonNull(keyExtractor, "keyExtractor");
        this.lanes = lanes;
        this.hotKeyThreshold = hotKeyThreshold;
    }

    /**
     * Creates a keyed partition spec.
     *
     * @param keyExtractor extracts the partition key
     * @param lanes number of lanes; must be positive
     * @param <T> message type
     * @param <K> key type
     * @return partition spec
     */
    public static <T, K> PartitionSpec<T, K> byKey(
        final Function<? super T, ? extends K> keyExtractor,
        final int lanes
    ) {
        return new PartitionSpec<>(keyExtractor, lanes, 0L);
    }

    /**
     * Returns a copy that reports a hot-key signal after the threshold is
     * reached. A threshold of {@code 0} disables hot-key signaling.
     */
    public PartitionSpec<T, K> hotKeyThreshold(final long threshold) {
        if (threshold < 0L) {
            throw new IllegalArgumentException("threshold must not be negative");
        }
        return new PartitionSpec<>(keyExtractor, lanes, threshold);
    }

    /**
     * Returns the partition key extractor.
     */
    public Function<? super T, ? extends K> keyExtractor() {
        return keyExtractor;
    }

    /**
     * Returns the number of partition lanes.
     */
    public int lanes() {
        return lanes;
    }

    /**
     * Returns the hot-key threshold, or {@code 0} when disabled.
     */
    public long hotKeyThreshold() {
        return hotKeyThreshold;
    }
}
