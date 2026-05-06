package io.github.elevateddev.lattice.stage;

/**
 * Read-only view of items delivered to a batch stage callback.
 *
 * @param <T> item type
 */
public interface Batch<T> {

    /**
     * Returns the number of items in this batch.
     */
    int size();

    /**
     * Returns the item at {@code index}.
     */
    T get(int index);

    /**
     * Returns whether this batch contains no items.
     */
    default boolean isEmpty() {
        return size() == 0;
    }
}
