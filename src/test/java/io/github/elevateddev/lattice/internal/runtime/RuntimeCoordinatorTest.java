package io.github.elevateddev.lattice.internal.runtime;

import io.github.elevateddev.lattice.graph.GraphRuntimeException;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeCoordinatorTest {

    @Test
    void requestStopMarksAbortAndStopState() {
        final GraphMetrics metrics = RuntimeTestSupport.graphMetrics("coordinator", "source", "sink");
        final RuntimeCoordinator coordinator = RuntimeTestSupport.coordinator("coordinator", GraphState.RUNNING, metrics);

        coordinator.requestStop();

        assertTrue(coordinator.isAbortRequested());
        assertTrue(coordinator.isStopping());
        assertEquals(GraphState.STOPPING, coordinator.state());
    }

    @Test
    void failWrapsStageFailureAndRecordsGraphFailure() {
        final GraphMetrics metrics = RuntimeTestSupport.graphMetrics("coordinator-fail", "source", "sink");
        final RuntimeCoordinator coordinator = RuntimeTestSupport.coordinator(
            "coordinator-fail",
            GraphState.RUNNING,
            metrics
        );

        coordinator.fail("stage", new IllegalStateException("boom"));

        assertEquals(GraphState.FAILED, coordinator.state());
        assertTrue(coordinator.isAbortRequested());
        assertEquals(1, metrics.stageExceptions());
    }

    @Test
    void flagsReflectConfiguredRuntimeOptions() {
        final RuntimeCoordinator coordinator = new RuntimeCoordinator(
            "flags",
            new java.util.concurrent.atomic.AtomicReference<>(GraphState.RUNNING),
            new java.util.concurrent.atomic.AtomicReference<Throwable>(),
            RuntimeTestSupport.graphMetrics("flags", "source", "sink"),
            0,
            true,
            true,
            true,
            true,
            true,
            7L
        );

        assertEquals("flags", coordinator.graphName());
        assertTrue(coordinator.jfrEnabled());
        assertTrue(coordinator.fusedLogicalEdgeCountersEnabled());
        assertTrue(coordinator.validateFusedTypes());
        assertTrue(coordinator.strictPlacement());
        assertTrue(coordinator.firstTouchPlacement());
        assertEquals(7L, coordinator.placementBootstrapDelayMillis());
        assertFalse(coordinator.hasInFlightWork());
    }
}
