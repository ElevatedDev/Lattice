package io.github.elevateddev.lattice.graph;

/**
 * Declares how a source emitter is used by application threads.
 * <p>
 * The mode lets the compiler choose the cheapest safe ingress edge when the
 * source is connected directly to a worker. Use {@link #SINGLE_PRODUCER} only
 * when one application thread will emit through the source at a time and source
 * close/graph stop is externally serialized with active emits; otherwise keep
 * the default {@link #MULTI_PRODUCER}.
 */
public enum SourceMode {
    /**
     * Allows concurrent calls from multiple producer threads and foreign-thread
     * lifecycle close/stop requests.
     */
    MULTI_PRODUCER,

    /**
     * Allows a single producer thread and may specialize source ingress to
     * single-producer edges. This is the fastest mode: the producer owns the
     * source edge tail cursor, so application code must not race active
     * {@code emit(...)} calls with {@code close()} or graph stop/abort.
     */
    SINGLE_PRODUCER
}
