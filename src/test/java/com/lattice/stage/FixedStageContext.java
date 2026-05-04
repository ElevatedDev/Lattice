package com.lattice.stage;

import com.lattice.graph.GraphState;
import com.lattice.metrics.StageMetrics;

record FixedStageContext(String graphName, String stageName, GraphState graphState, StageMetrics metrics)
    implements StageContext {

    FixedStageContext(final String graphName, final String stageName) {
        this(graphName, stageName, GraphState.RUNNING, new StageMetrics(stageName));
    }

    @Override
    public boolean isStopping() {
        return graphState == GraphState.DRAINING
            || graphState == GraphState.STOPPING
            || graphState == GraphState.STOPPED
            || graphState == GraphState.FAILED;
    }
}
