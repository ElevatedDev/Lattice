package com.lattice.graph;

/**
 * Declares how a source emitter is used by application threads.
 * <p>
 * The mode lets the compiler choose the cheapest safe ingress edge when the
 * source is connected directly to a worker. Use {@link #SINGLE_PRODUCER} only
 * when one application thread will emit through the source at a time; otherwise
 * keep the default {@link #MULTI_PRODUCER}.
 */
public enum SourceMode {
    /**
     * Allows concurrent calls from multiple producer threads.
     */
    MULTI_PRODUCER,

    /**
     * Allows a single producer thread and may specialize source ingress to
     * single-producer edges.
     */
    SINGLE_PRODUCER
}
