package com.lattice.stage;

@FunctionalInterface
public interface StageExceptionHandler {

    StageExceptionAction onException(String graphName, String stageName, Throwable failure, StageContext context);

    static StageExceptionHandler failGraph() {
        return (graphName, stageName, failure, context) -> StageExceptionAction.FAIL_GRAPH;
    }
}
