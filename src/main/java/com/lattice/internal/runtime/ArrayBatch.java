package com.lattice.internal.runtime;

import com.lattice.stage.Batch;
import java.util.Arrays;

final class ArrayBatch implements Batch<Object> {
    private final Object[] items;
    private int size;

    ArrayBatch(final int capacity) {
        this.items = new Object[capacity];
    }

    Object[] items() {
        return items;
    }

    void size(final int size) {
        this.size = size;
    }

    Object itemAt(final int index) {
        return items[index];
    }

    void clear() {
        final int currentSize = size;
        if (currentSize > 0) {
            Arrays.fill(items, 0, currentSize, null);
            size = 0;
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Object get(final int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException(index);
        }
        return items[index];
    }
}
