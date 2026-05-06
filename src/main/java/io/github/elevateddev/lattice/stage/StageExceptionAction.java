package io.github.elevateddev.lattice.stage;

/**
 * Runtime response chosen by a {@link StageExceptionHandler}.
 */
public enum StageExceptionAction {
    /**
     * Stop the whole graph as failed.
     */
    FAIL_GRAPH,
    /**
     * Stop the failing stage while letting the graph wind down.
     */
    POISON_STAGE
}
