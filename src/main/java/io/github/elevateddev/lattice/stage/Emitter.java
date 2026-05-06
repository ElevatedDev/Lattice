package io.github.elevateddev.lattice.stage;

import java.time.Duration;

/**
 * Publishes items into a normal graph source.
 * <p>
 * Emitters are obtained from {@link io.github.elevateddev.lattice.graph.StaticGraph#emitter} and
 * should be closed when no more items will be produced for that source.
 *
 * @param <T> source item type
 */
public interface Emitter<T> extends AutoCloseable {

    /**
     * Returns the source name.
     */
    String name();

    /**
     * Emits an item, waiting according to the source edge overflow policy.
     */
    void emit(T item);

    /**
     * Attempts to emit an item before the timeout expires.
     *
     * @return {@code true} when the item was accepted
     */
    boolean emit(T item, Duration timeout);

    /**
     * Attempts to emit an item without waiting.
     *
     * @return {@code true} when the item was accepted
     */
    boolean tryEmit(T item);

    /**
     * Returns whether this emitter has been closed.
     */
    boolean isClosed();

    /**
     * Closes this source emitter.
     */
    @Override
    void close();
}
