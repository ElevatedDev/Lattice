package io.github.elevateddev.lattice.stage;

import java.time.Duration;

/**
 * Emits objects claimed from a preallocated source pool.
 * <p>
 * A preallocated emitter has at most one outstanding claim at a time. Mutate the
 * claimed object, then call one of the emit methods. If a timed or non-blocking
 * emit returns {@code false}, the claim is still outstanding; retry the emit or
 * call {@link #discard(Object)} before claiming another object.
 *
 * @param <T> pooled item type
 */
public interface PreallocatedEmitter<T> extends AutoCloseable {
    /**
     * Returns the source name this emitter publishes through.
     */
    String name();

    /**
     * Claims the next reusable item from the source pool.
     *
     * @return item owned by the caller until emitted or discarded
     * @throws io.github.elevateddev.lattice.graph.GraphRuntimeException if an earlier claim is
     * still outstanding
     */
    T claim();

    /**
     * Emits a claimed item, blocking according to the source edge policy.
     * <p>
     * The claim is cleared when this method returns or throws.
     */
    void emit(T item);

    /**
     * Attempts to emit a claimed item before the timeout expires.
     *
     * @return {@code true} if emitted; {@code false} if the item is still
     * claimed and must be retried or discarded
     */
    boolean emit(T item, Duration timeout);

    /**
     * Attempts to emit a claimed item without waiting.
     *
     * @return {@code true} if emitted; {@code false} if the item is still
     * claimed and must be retried or discarded
     */
    boolean tryEmit(T item);

    /**
     * Releases an outstanding claim without publishing the item.
     */
    void discard(T item);

    /**
     * Returns the configured source pool size.
     */
    int poolSize();

    /**
     * Returns the minimum number of in-flight items the runtime determined for
     * this source topology. The pool size is always greater than this value.
     */
    int reuseBound();

    /**
     * Returns whether this emitter has been closed.
     */
    boolean isClosed();

    /**
     * Closes the source and prevents further claims or emits.
     */
    @Override
    void close();
}
