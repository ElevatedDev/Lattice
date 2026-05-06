package io.github.elevateddev.lattice.metrics;

/**
 * Observable lifecycle state for a stage worker.
 */
public enum WorkerState {
    /**
     * Worker has been created but not started.
     */
    NEW,
    /**
     * Worker startup is in progress.
     */
    STARTING,
    /**
     * Worker is actively processing or polling.
     */
    RUNNING,
    /**
     * Worker is alive but currently has no work.
     */
    IDLE,
    /**
     * Worker is blocked on downstream pressure.
     */
    BLOCKED,
    /**
     * Worker is parked by its wait strategy.
     */
    PARKED,
    /**
     * Worker has been poisoned after a stage failure.
     */
    POISONED,
    /**
     * Worker shutdown is in progress.
     */
    STOPPING,
    /**
     * Worker has stopped.
     */
    STOPPED,
    /**
     * Worker stopped because of a failure.
     */
    FAILED
}
