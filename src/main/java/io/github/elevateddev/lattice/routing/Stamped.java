package io.github.elevateddev.lattice.routing;

import java.util.Objects;

/**
 * Payload wrapper with a primitive long stamp.
 * <p>
 * Stamped values are the default input shape for {@link JoinSpec} and are also
 * produced by stamped sources.
 *
 * @param stamp correlation stamp
 * @param value payload value
 * @param <T> payload type
 */
public record Stamped<T>(long stamp, T value) {

    public Stamped {
        Objects.requireNonNull(value, "value");
    }

    /**
     * Creates a stamped payload.
     */
    public static <T> Stamped<T> of(final long stamp, final T value) {
        return new Stamped<>(stamp, value);
    }
}
