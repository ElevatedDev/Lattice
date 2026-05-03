package com.lattice.stage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

final class RecordingOutput<T> implements Output<T> {
    private final List<T> items = new ArrayList<>();

    @Override
    public void push(final T item) {
        items.add(item);
    }

    @Override
    public boolean push(final T item, final Duration timeout) {
        items.add(item);
        return true;
    }

    @Override
    public boolean tryPush(final T item) {
        items.add(item);
        return true;
    }

    List<T> items() {
        return List.copyOf(items);
    }
}
