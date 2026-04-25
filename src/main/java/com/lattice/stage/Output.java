package com.lattice.stage;

import java.time.Duration;

public interface Output<T> {

    void push(T item);

    boolean push(T item, Duration timeout);

    boolean tryPush(T item);
}
