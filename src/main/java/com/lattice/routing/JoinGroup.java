package com.lattice.routing;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Values delivered to a join combiner for one stamp.
 * <p>
 * Runtime join groups are reused after the combiner returns. Read values inside
 * the combiner and copy anything that must outlive that call with
 * {@link #snapshotValuesBySource()}.
 */
public final class JoinGroup {

    private Object stamp;
    private long longStamp;
    private boolean usesLongStamp;
    private final Map<String, Object> valuesBySource;
    private boolean complete;
    private boolean timedOut;
    private String triggeringSource;

    public JoinGroup(
        final Object stamp,
        final Map<String, Object> valuesBySource,
        final boolean complete,
        final boolean timedOut,
        final String triggeringSource
    ) {
        this(
            Objects.requireNonNull(stamp, "stamp"),
            0L,
            false,
            Collections.unmodifiableMap(new LinkedHashMap<>(valuesBySource)),
            complete,
            timedOut,
            triggeringSource
        );
    }

    private JoinGroup(
        final Object stamp,
        final long longStamp,
        final boolean usesLongStamp,
        final Map<String, Object> valuesBySource,
        final boolean complete,
        final boolean timedOut,
        final String triggeringSource
    ) {
        this.stamp = stamp;
        this.longStamp = longStamp;
        this.usesLongStamp = usesLongStamp;
        this.valuesBySource = Objects.requireNonNull(valuesBySource, "valuesBySource");
        this.complete = complete;
        this.timedOut = timedOut;
        this.triggeringSource = triggeringSource == null ? "" : triggeringSource;
    }

    /**
     * Runtime support factory for allocation-free join delivery.
     * <p>
     * User code normally receives join groups from {@link JoinSpec} combiners
     * and should construct snapshots with {@link #snapshotValuesBySource()} when
     * values must be retained.
     */
    public static JoinGroup reusableRuntimeGroup(final Map<String, Object> valuesBySource) {
        return new JoinGroup(null, 0L, false, valuesBySource, false, false, "");
    }

    /**
     * Runtime support hook for reusing a join group instance.
     * <p>
     * User code should not call this method from a combiner.
     */
    public void resetRuntime(
        final Object stamp,
        final boolean complete,
        final boolean timedOut,
        final String triggeringSource
    ) {
        this.stamp = Objects.requireNonNull(stamp, "stamp");
        this.longStamp = 0L;
        this.usesLongStamp = false;
        this.complete = complete;
        this.timedOut = timedOut;
        this.triggeringSource = triggeringSource == null ? "" : triggeringSource;
    }

    /**
     * Runtime support hook for reusing a join group instance with a long stamp.
     * <p>
     * User code should not call this method from a combiner.
     */
    public void resetRuntime(
        final long stamp,
        final boolean complete,
        final boolean timedOut,
        final String triggeringSource
    ) {
        this.stamp = null;
        this.longStamp = stamp;
        this.usesLongStamp = true;
        this.complete = complete;
        this.timedOut = timedOut;
        this.triggeringSource = triggeringSource == null ? "" : triggeringSource;
    }

    public Object stamp() {
        return usesLongStamp ? longStamp : stamp;
    }

    /**
     * Returns whether this group was stamped through primitive long extraction.
     */
    public boolean usesLongStamp() {
        return usesLongStamp;
    }

    public long longStamp() {
        if (usesLongStamp) {
            return longStamp;
        }
        if (stamp instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalStateException("join stamp is not a long-compatible value");
    }

    public Map<String, Object> valuesBySource() {
        return valuesBySource;
    }

    /**
     * Returns a stable copy of the currently present values.
     * <p>
     * This is the safe form to retain beyond the combiner call because runtime
     * join groups and their map views are reused.
     */
    public Map<String, Object> snapshotValuesBySource() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(valuesBySource));
    }

    public boolean complete() {
        return complete;
    }

    public boolean timedOut() {
        return timedOut;
    }

    public String triggeringSource() {
        return triggeringSource;
    }

    public <T> Optional<T> value(final String sourceName, final Class<T> type) {
        final Object value = valuesBySource.get(sourceName);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(type.cast(value));
    }
}
