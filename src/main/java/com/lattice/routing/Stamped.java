package com.lattice.routing;

import java.util.Objects;

public record Stamped<T>(long stamp, T value) {

    public Stamped {
        Objects.requireNonNull(value, "value");
    }

    public static <T> Stamped<T> of(final long stamp, final T value) {
        return new Stamped<>(stamp, value);
    }
}
