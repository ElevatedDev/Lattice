package com.lattice.routing;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * Configures a join stage.
 * <p>
 * By default, joins use long stamps extracted from {@link Stamped} inputs. Use
 * {@link #stampLong(ToLongFunction)} for allocation-free primitive stamp
 * extraction from custom message types, or {@link #stamp(Function)} when a join
 * key is not naturally represented as a long.
 *
 * @param <O> joined output type
 */
public final class JoinSpec<O> {

    private static final int DEFAULT_CAPACITY = 1024;

    private final JoinKind kind;
    private final Function<JoinGroup, ? extends O> combiner;
    private final Function<Object, ?> stampExtractor;
    private final ToLongFunction<Object> longStampExtractor;
    private final boolean longStamp;
    private final int capacity;
    private final Duration timeout;
    private final MissingBranchPolicy missingBranchPolicy;
    private final DuplicatePolicy duplicatePolicy;

    private JoinSpec(
        final JoinKind kind,
        final Function<JoinGroup, ? extends O> combiner,
        final Function<Object, ?> stampExtractor,
        final ToLongFunction<Object> longStampExtractor,
        final boolean longStamp,
        final int capacity,
        final Duration timeout,
        final MissingBranchPolicy missingBranchPolicy,
        final DuplicatePolicy duplicatePolicy
    ) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        final Duration timeoutValue = Objects.requireNonNull(timeout, "timeout");
        if (timeoutValue.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        this.kind = Objects.requireNonNull(kind, "kind");
        this.combiner = Objects.requireNonNull(combiner, "combiner");
        this.stampExtractor = stampExtractor;
        this.longStampExtractor = longStampExtractor;
        this.longStamp = longStamp;
        if (longStamp) {
            Objects.requireNonNull(longStampExtractor, "longStampExtractor");
        } else {
            Objects.requireNonNull(stampExtractor, "stampExtractor");
        }
        this.capacity = capacity;
        this.timeout = timeoutValue;
        this.missingBranchPolicy = Objects.requireNonNull(missingBranchPolicy, "missingBranchPolicy");
        this.duplicatePolicy = Objects.requireNonNull(duplicatePolicy, "duplicatePolicy");
    }

    /**
     * Emits when every input branch has provided the same stamp.
     * <p>
     * The default stamp extractor expects {@link Stamped} inputs and uses their
     * primitive long stamp.
     */
    public static <O> JoinSpec<O> allOf(final Function<JoinGroup, ? extends O> combiner) {
        return new JoinSpec<>(
            JoinKind.ALL_OF,
            combiner,
            JoinSpec::defaultStamp,
            JoinSpec::defaultLongStamp,
            true,
            DEFAULT_CAPACITY,
            Duration.ZERO,
            MissingBranchPolicy.DISCARD,
            DuplicatePolicy.COUNT
        );
    }

    /**
     * Emits on the first input branch for each stamp.
     * <p>
     * Later duplicate branches for that stamp are handled by the duplicate
     * policy. The default stamp extractor expects {@link Stamped} inputs and
     * uses their primitive long stamp.
     */
    public static <O> JoinSpec<O> anyOf(final Function<JoinGroup, ? extends O> combiner) {
        return new JoinSpec<>(
            JoinKind.ANY_OF,
            combiner,
            JoinSpec::defaultStamp,
            JoinSpec::defaultLongStamp,
            true,
            DEFAULT_CAPACITY,
            Duration.ZERO,
            MissingBranchPolicy.DISCARD,
            DuplicatePolicy.COUNT
        );
    }

    /**
     * Uses an object-valued join stamp.
     * <p>
     * Prefer {@link #stampLong(ToLongFunction)} for long-compatible stamps to
     * keep the runtime join table on its allocation-free path.
     */
    public JoinSpec<O> stamp(final Function<Object, ?> stampExtractor) {
        return new JoinSpec<>(
            kind,
            combiner,
            Objects.requireNonNull(stampExtractor, "stampExtractor"),
            null,
            false,
            capacity,
            timeout,
            missingBranchPolicy,
            duplicatePolicy
        );
    }

    /**
     * Uses a primitive long join stamp.
     * <p>
     * This is the fastest stamp mode and avoids boxing in runtime join state.
     */
    public JoinSpec<O> stampLong(final ToLongFunction<Object> stampExtractor) {
        return new JoinSpec<>(
            kind,
            combiner,
            null,
            Objects.requireNonNull(stampExtractor, "stampExtractor"),
            true,
            capacity,
            timeout,
            missingBranchPolicy,
            duplicatePolicy
        );
    }

    public JoinSpec<O> capacity(final int capacity) {
        return new JoinSpec<>(
            kind,
            combiner,
            stampExtractor,
            longStampExtractor,
            longStamp,
            capacity,
            timeout,
            missingBranchPolicy,
            duplicatePolicy
        );
    }

    public JoinSpec<O> timeout(final Duration timeout) {
        return new JoinSpec<>(
            kind,
            combiner,
            stampExtractor,
            longStampExtractor,
            longStamp,
            capacity,
            timeout,
            missingBranchPolicy,
            duplicatePolicy
        );
    }

    public JoinSpec<O> missingBranches(final MissingBranchPolicy policy) {
        return new JoinSpec<>(
            kind,
            combiner,
            stampExtractor,
            longStampExtractor,
            longStamp,
            capacity,
            timeout,
            policy,
            duplicatePolicy
        );
    }

    public JoinSpec<O> duplicates(final DuplicatePolicy policy) {
        return new JoinSpec<>(
            kind,
            combiner,
            stampExtractor,
            longStampExtractor,
            longStamp,
            capacity,
            timeout,
            missingBranchPolicy,
            policy
        );
    }

    public JoinKind kind() {
        return kind;
    }

    public Function<JoinGroup, ? extends O> combiner() {
        return combiner;
    }

    public Function<Object, ?> stampExtractor() {
        return longStamp ? item -> longStampExtractor.applyAsLong(item) : stampExtractor;
    }

    /**
     * Returns {@code true} when this spec uses primitive long stamps.
     * <p>
     * Long stamps avoid boxing in the runtime join table. The default
     * {@link #allOf(Function)} and {@link #anyOf(Function)} specs use this mode
     * and expect {@link Stamped} inputs unless {@link #stampLong(ToLongFunction)}
     * is configured.
     */
    public boolean longStamp() {
        return longStamp;
    }

    public Object extractStamp(final Object item) {
        if (longStamp) {
            return longStampExtractor.applyAsLong(item);
        }
        return stampExtractor.apply(item);
    }

    public long extractLongStamp(final Object item) {
        if (!longStamp) {
            throw new IllegalStateException("join spec does not use long stamps");
        }
        return longStampExtractor.applyAsLong(item);
    }

    public int capacity() {
        return capacity;
    }

    public Duration timeout() {
        return timeout;
    }

    public MissingBranchPolicy missingBranchPolicy() {
        return missingBranchPolicy;
    }

    public DuplicatePolicy duplicatePolicy() {
        return duplicatePolicy;
    }

    private static Object defaultStamp(final Object item) {
        return defaultLongStamp(item);
    }

    private static long defaultLongStamp(final Object item) {
        if (item instanceof Stamped<?> stamped) {
            return stamped.stamp();
        }
        throw new IllegalArgumentException("join input is not Stamped and no stamp extractor was configured");
    }

    public enum JoinKind {
        ALL_OF,
        ANY_OF
    }

    public enum MissingBranchPolicy {
        DISCARD,
        EMIT_PARTIAL
    }

    public enum DuplicatePolicy {
        IGNORE,
        COUNT,
        FAIL
    }
}
