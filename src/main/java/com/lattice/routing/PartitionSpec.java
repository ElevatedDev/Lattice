package com.lattice.routing;

import java.util.Objects;
import java.util.function.Function;

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

    public static <T, K> PartitionSpec<T, K> byKey(
        final Function<? super T, ? extends K> keyExtractor,
        final int lanes
    ) {
        return new PartitionSpec<>(keyExtractor, lanes, 0L);
    }

    public PartitionSpec<T, K> hotKeyThreshold(final long threshold) {
        if (threshold < 0L) {
            throw new IllegalArgumentException("threshold must not be negative");
        }
        return new PartitionSpec<>(keyExtractor, lanes, threshold);
    }

    public Function<? super T, ? extends K> keyExtractor() {
        return keyExtractor;
    }

    public int lanes() {
        return lanes;
    }

    public long hotKeyThreshold() {
        return hotKeyThreshold;
    }
}
