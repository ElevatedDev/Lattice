package com.lattice.metrics;

public enum WorkerState {
    NEW,
    STARTING,
    RUNNING,
    IDLE,
    BLOCKED,
    PARKED,
    POISONED,
    STOPPING,
    STOPPED,
    FAILED
}
