package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphCompilationReport;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import com.lattice.stage.StageSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultStaticGraphTest {

    @Test
    void builderReturnsDefaultStaticGraphWithImmutablePlanAndMetrics() {
        final StaticGraph graph = StaticGraph.builder("default-runtime")
            .source("source", Integer.class)
            .sink("sink", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("source", "sink", EdgeSpec.mpscRing(8))
            .build();

        assertInstanceOf(DefaultStaticGraph.class, graph);
        assertEquals("default-runtime", graph.plan().name());
        final GraphCompilationReport report = graph.compilationReport();
        assertEquals("default-runtime", report.graphName());
        assertEquals(GraphCompilationReport.EdgeUseKind.NORMAL,
            report.edge("source", "sink").orElseThrow().use());
        assertEquals(GraphCompilationReport.WorkerDecisionKind.RUNNABLE,
            report.worker("sink").orElseThrow().decision());
        assertEquals("default-runtime", graph.metrics().graphName());
        assertEquals(GraphState.NEW, graph.state());

        graph.close();

        assertEquals(GraphState.STOPPED, graph.state());
        assertTrue(graph.failure().isEmpty());
    }
}
