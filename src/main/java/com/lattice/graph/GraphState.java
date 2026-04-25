package com.lattice.graph;

public enum GraphState {
    NEW,
    STARTING,
    RUNNING,
    QUIESCING,
    DRAINING,
    STOPPING,
    STOPPED,
    FAILED
}
