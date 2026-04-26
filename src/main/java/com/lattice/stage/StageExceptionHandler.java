package com.lattice.stage;

/**
 * Handles exceptions thrown by stage callbacks.
 */
@FunctionalInterface
public interface StageExceptionHandler {

    /**
     * Chooses how the runtime should respond to a stage failure.
     */
    StageExceptionAction onException(String graphName, String stageName, Throwable failure, StageContext context);

    /**
     * Returns a handler that fails the graph for every stage exception.
     */
    static StageExceptionHandler failGraph() {
        return (graphName, stageName, failure, context) -> StageExceptionAction.FAIL_GRAPH;
    }
}
