package com.lattice.routing;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

public final class JoinSpec<O> {

    private static final int DEFAULT_CAPACITY = 1024;

    private final JoinKind kind;
    private final Function<JoinGroup, ? extends O> combiner;
    private final Function<Object, ?> stampExtractor;
    private final int capacity;
    private final Duration timeout;
    private final MissingBranchPolicy missingBranchPolicy;
    private final DuplicatePolicy duplicatePolicy;

    private JoinSpec(
        final JoinKind kind,
        final Function<JoinGroup, ? extends O> combiner,
        final Function<Object, ?> stampExtractor,
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
        this.stampExtractor = Objects.requireNonNull(stampExtractor, "stampExtractor");
        this.capacity = capacity;
        this.timeout = timeoutValue;
        this.missingBranchPolicy = Objects.requireNonNull(missingBranchPolicy, "missingBranchPolicy");
        this.duplicatePolicy = Objects.requireNonNull(duplicatePolicy, "duplicatePolicy");
    }

    public static <O> JoinSpec<O> allOf(final Function<JoinGroup, ? extends O> combiner) {
        return new JoinSpec<>(
            JoinKind.ALL_OF,
            combiner,
            JoinSpec::defaultStamp,
            DEFAULT_CAPACITY,
            Duration.ZERO,
            MissingBranchPolicy.DISCARD,
            DuplicatePolicy.COUNT
        );
    }

    public static <O> JoinSpec<O> anyOf(final Function<JoinGroup, ? extends O> combiner) {
        return new JoinSpec<>(
            JoinKind.ANY_OF,
            combiner,
            JoinSpec::defaultStamp,
            DEFAULT_CAPACITY,
            Duration.ZERO,
            MissingBranchPolicy.DISCARD,
            DuplicatePolicy.COUNT
        );
    }

    public JoinSpec<O> stamp(final Function<Object, ?> stampExtractor) {
        return new JoinSpec<>(kind, combiner, stampExtractor, capacity, timeout, missingBranchPolicy, duplicatePolicy);
    }

    public JoinSpec<O> capacity(final int capacity) {
        return new JoinSpec<>(kind, combiner, stampExtractor, capacity, timeout, missingBranchPolicy, duplicatePolicy);
    }

    public JoinSpec<O> timeout(final Duration timeout) {
        return new JoinSpec<>(kind, combiner, stampExtractor, capacity, timeout, missingBranchPolicy, duplicatePolicy);
    }

    public JoinSpec<O> missingBranches(final MissingBranchPolicy policy) {
        return new JoinSpec<>(kind, combiner, stampExtractor, capacity, timeout, policy, duplicatePolicy);
    }

    public JoinSpec<O> duplicates(final DuplicatePolicy policy) {
        return new JoinSpec<>(kind, combiner, stampExtractor, capacity, timeout, missingBranchPolicy, policy);
    }

    public JoinKind kind() {
        return kind;
    }

    public Function<JoinGroup, ? extends O> combiner() {
        return combiner;
    }

    public Function<Object, ?> stampExtractor() {
        return stampExtractor;
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
