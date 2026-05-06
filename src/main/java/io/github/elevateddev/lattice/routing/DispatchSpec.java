package io.github.elevateddev.lattice.routing;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/**
 * Configures a dispatch routing stage.
 * <p>
 * Dispatch selects one outgoing branch for each message. The selected branch is
 * deterministic for keyed dispatch and weighted by configured lane weights for
 * weighted dispatch.
 *
 * @param <T> message type
 */
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

    /**
     * Selects outgoing branches in round-robin order.
     */
    public static <T> DispatchSpec<T> roundRobin() {
        return new DispatchSpec<>(DispatchKind.ROUND_ROBIN, null, null);
    }

    /**
     * Selects an outgoing branch from the extracted key.
     */
    public static <T> DispatchSpec<T> keyed(final Function<? super T, ?> keyExtractor) {
        return new DispatchSpec<>(DispatchKind.KEYED, Objects.requireNonNull(keyExtractor, "keyExtractor"), null);
    }

    /**
     * Selects branches in proportion to positive integer weights.
     */
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

    /**
     * Returns the dispatch mode.
     */
    public DispatchKind kind() {
        return kind;
    }

    /**
     * Returns the key extractor for keyed dispatch, or {@code null} otherwise.
     */
    public Function<? super T, ?> keyExtractor() {
        return keyExtractor;
    }

    /**
     * Returns a defensive copy of configured branch weights.
     */
    public int[] weights() {
        return weights.clone();
    }

    /**
     * Returns the sum of configured branch weights.
     */
    public int weightSum() {
        return Arrays.stream(weights).sum();
    }

    /**
     * Dispatch selection modes.
     */
    public enum DispatchKind {
        /**
         * Select branches cyclically.
         */
        ROUND_ROBIN,
        /**
         * Select by extracted key.
         */
        KEYED,
        /**
         * Select by configured positive weights.
         */
        WEIGHTED
    }
}
