package com.lattice.stage;

import java.time.Duration;

public interface Emitter<T> extends AutoCloseable {

    String name();

    void emit(T item);

    boolean emit(T item, Duration timeout);

    boolean tryEmit(T item);

    boolean isClosed();

    @Override
    void close();
}
