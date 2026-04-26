package com.lattice.graph;

/**
 * Lifecycle states exposed by {@link StaticGraph#state()}.
 */
public enum GraphState {
    /**
     * Graph was built but has not started.
     */
    NEW,
    /**
     * Worker startup is in progress.
     */
    STARTING,
    /**
     * Graph is accepting source input and workers are running.
     */
    RUNNING,
    /**
     * Graph is rejecting new work while existing work drains.
     */
    QUIESCING,
    /**
     * Workers are draining already accepted work.
     */
    DRAINING,
    /**
     * Graph shutdown is in progress.
     */
    STOPPING,
    /**
     * Graph stopped normally.
     */
    STOPPED,
    /**
     * Graph stopped because of a failure.
     */
    FAILED
}
