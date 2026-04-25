package com.lattice.routing;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

public final class DispatchSpec<T> {

    private final DispatchKind kind;
    private final Function<? super T, ?> keyExtractor;
    private final int[] weights;

    private DispatchSpec(
        final DispatchKind kind,
        final Function<? super T, ?> keyExtractor,
        final int[] weights
    ) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.keyExtractor = keyExtractor;
        this.weights = weights == null ? new int[0] : weights.clone();
    }

    public static <T> DispatchSpec<T> roundRobin() {
        return new DispatchSpec<>(DispatchKind.ROUND_ROBIN, null, null);
    }

    public static <T> DispatchSpec<T> keyed(final Function<? super T, ?> keyExtractor) {
        return new DispatchSpec<>(DispatchKind.KEYED, Objects.requireNonNull(keyExtractor, "keyExtractor"), null);
    }

    public static <T> DispatchSpec<T> weighted(final int... weights) {
        final int[] values = Objects.requireNonNull(weights, "weights").clone();
        if (values.length == 0) {
            throw new IllegalArgumentException("weights must not be empty");
        }
        for (final int weight : values) {
            if (weight <= 0) {
                throw new IllegalArgumentException("weights must be positive");
            }
        }
        return new DispatchSpec<>(DispatchKind.WEIGHTED, null, values);
    }

    public DispatchKind kind() {
        return kind;
    }

    public Function<? super T, ?> keyExtractor() {
        return keyExtractor;
    }

    public int[] weights() {
        return weights.clone();
    }

    public int weightSum() {
        return Arrays.stream(weights).sum();
    }

    public enum DispatchKind {
        ROUND_ROBIN,
        KEYED,
        WEIGHTED
    }
}
