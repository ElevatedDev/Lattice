package com.lattice.stage;

import java.time.Duration;

/**
 * Publishes stage outputs to the stage's outgoing edge.
 *
 * @param <T> output item type
 */
public interface Output<T> {

    /**
     * Pushes an output item, waiting according to the edge overflow policy.
     */
    void push(T item);

    /**
     * Attempts to push an output item before the timeout expires.
     *
     * @return {@code true} when the item was accepted
     */
    boolean push(T item, Duration timeout);

    /**
     * Attempts to push an output item without waiting.
     *
     * @return {@code true} when the item was accepted
     */
    boolean tryPush(T item);
}
