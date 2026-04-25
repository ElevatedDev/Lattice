package com.lattice.stage;

import com.lattice.graph.GraphState;
import com.lattice.metrics.StageMetrics;

public interface StageContext {

    String graphName();

    String stageName();

    GraphState graphState();

    StageMetrics metrics();

    boolean isStopping();
}
