package com.lattice.routing;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class JoinGroup {

    private final Object stamp;
    private final Map<String, Object> valuesBySource;
    private final boolean complete;
    private final boolean timedOut;
    private final String triggeringSource;

    public JoinGroup(
        final Object stamp,
        final Map<String, Object> valuesBySource,
        final boolean complete,
        final boolean timedOut,
        final String triggeringSource
    ) {
        this.stamp = Objects.requireNonNull(stamp, "stamp");
        this.valuesBySource = Collections.unmodifiableMap(new LinkedHashMap<>(valuesBySource));
        this.complete = complete;
        this.timedOut = timedOut;
        this.triggeringSource = triggeringSource == null ? "" : triggeringSource;
    }

    public Object stamp() {
        return stamp;
    }

    public Map<String, Object> valuesBySource() {
        return valuesBySource;
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
