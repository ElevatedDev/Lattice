package com.lattice.stage;

public interface Batch<T> {

    int size();

    T get(int index);

    default boolean isEmpty() {
        return size() == 0;
    }
}
