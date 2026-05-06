package io.github.elevateddev.lattice.routing;

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
     * Later arrivals for that stamp are handled by the duplicate policy while
     * the stamp remains open. Once every branch has been seen, the runtime
     * releases the retained first-arrival value and forgets the stamp.
     * The default stamp extractor expects {@link Stamped} inputs and uses their
     * primitive long stamp.
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

    /**
     * Returns a copy with a different maximum number of open join groups.
     */
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

    /**
     * Returns a copy with a different timeout for incomplete groups.
     */
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

    /**
     * Returns a copy with the policy used when an input branch closes or times
     * out before a group is complete.
     */
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

    /**
     * Returns a copy with the policy used when a branch supplies the same stamp
     * more than once.
     */
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

    /**
     * Returns whether this join waits for all branches or emits on any branch.
     */
    public JoinKind kind() {
        return kind;
    }

    /**
     * Returns the combiner invoked for completed or policy-emitted groups.
     */
    public Function<JoinGroup, ? extends O> combiner() {
        return combiner;
    }

    /**
     * Returns an object stamp extractor.
     * <p>
     * For long-stamped specs this adapter boxes the primitive long stamp; prefer
     * {@link #extractLongStamp(Object)} when {@link #longStamp()} is true.
     */
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

    /**
     * Extracts this item's stamp using the configured extractor.
     */
    public Object extractStamp(final Object item) {
        if (longStamp) {
            return longStampExtractor.applyAsLong(item);
        }
        return stampExtractor.apply(item);
    }

    /**
     * Extracts this item's primitive long stamp.
     *
     * @throws IllegalStateException if this spec uses object-valued stamps
     */
    public long extractLongStamp(final Object item) {
        if (!longStamp) {
            throw new IllegalStateException("join spec does not use long stamps");
        }
        return longStampExtractor.applyAsLong(item);
    }

    /**
     * Returns the maximum number of open groups retained by this join.
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Returns the join timeout, or {@link Duration#ZERO} when disabled.
     */
    public Duration timeout() {
        return timeout;
    }

    /**
     * Returns how incomplete groups are handled.
     */
    public MissingBranchPolicy missingBranchPolicy() {
        return missingBranchPolicy;
    }

    /**
     * Returns how duplicate branch values are handled.
     */
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
        /**
         * Emit when every branch has supplied the same stamp.
         */
        ALL_OF,
        /**
         * Emit on the first branch seen for each stamp.
         */
        ANY_OF
    }

    /**
     * Handling for groups that cannot receive every expected branch.
     */
    public enum MissingBranchPolicy {
        /**
         * Drop incomplete groups and record the missing branch.
         */
        DISCARD,
        /**
         * Emit incomplete groups to the combiner.
         */
        EMIT_PARTIAL
    }

    /**
     * Handling for duplicate values from the same branch and stamp.
     */
    public enum DuplicatePolicy {
        /**
         * Ignore duplicate values.
         */
        IGNORE,
        /**
         * Count duplicate values in metrics and keep processing.
         */
        COUNT,
        /**
         * Fail when a duplicate value is observed.
         */
        FAIL
    }
}
