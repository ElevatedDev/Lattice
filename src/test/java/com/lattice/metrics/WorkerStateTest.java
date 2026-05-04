package com.lattice.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class WorkerStateTest {

    @Test
    void exposesDocumentedWorkerLifecycleStates() {
        assertArrayEquals(
            new WorkerState[] {
                WorkerState.NEW,
                WorkerState.STARTING,
                WorkerState.RUNNING,
                WorkerState.IDLE,
                WorkerState.BLOCKED,
                WorkerState.PARKED,
                WorkerState.POISONED,
                WorkerState.STOPPING,
                WorkerState.STOPPED,
                WorkerState.FAILED
            },
            WorkerState.values()
        );
    }

    @Test
    void resolvesWorkerStatesByStableName() {
        for (final WorkerState state : WorkerState.values()) {
            assertSame(state, WorkerState.valueOf(state.name()));
        }
    }
}
