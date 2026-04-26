package com.lattice.stage;

import com.lattice.graph.GraphState;
import com.lattice.metrics.StageMetrics;

/**
 * Runtime context passed to stage callbacks.
 * <p>
 * The context exposes immutable identity information, current graph lifecycle
 * state, and the metrics object owned by the stage.
 */
public interface StageContext {

    /**
     * Returns the graph name.
     */
    String graphName();

    /**
     * Returns the current stage name.
     */
    String stageName();

    /**
     * Returns the current graph state observed by the stage.
     */
    GraphState graphState();

    /**
     * Returns metrics for the current stage.
     */
    StageMetrics metrics();

    /**
     * Returns whether the graph is stopping or stopped.
     */
    boolean isStopping();
}
