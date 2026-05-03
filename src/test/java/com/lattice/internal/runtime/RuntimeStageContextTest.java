package com.lattice.internal.runtime;

import com.lattice.graph.GraphState;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeStageContextTest {

    @Test
    void exposesGraphStageMetricsAndStoppingState() {
        final GraphMetrics graphMetrics = RuntimeTestSupport.graphMetrics("context", "source", "sink");
        final RuntimeCoordinator running = RuntimeTestSupport.coordinator("context", GraphState.RUNNING, graphMetrics);
        final StageMetrics stageMetrics = new StageMetrics("stage", RuntimeTestSupport.METRICS);
        final RuntimeStageContext context = new RuntimeStageContext(running, "stage", stageMetrics);

        assertEquals("context", context.graphName());
        assertEquals("stage", context.stageName());
        assertEquals(GraphState.RUNNING, context.graphState());
        assertSame(stageMetrics, context.metrics());
        assertFalse(context.isStopping());

        final RuntimeStageContext stopping = new RuntimeStageContext(
            RuntimeTestSupport.coordinator("context", GraphState.STOPPING, graphMetrics),
            "stage",
            stageMetrics
        );
        assertTrue(stopping.isStopping());
    }
}
