package io.github.elevateddev.lattice.internal.graph;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.GraphBuildException;
import io.github.elevateddev.lattice.graph.GraphPlan;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.stage.StageExceptionHandler;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphCompilerTest {

    @Test
    void compileBuildsInspectablePlanAndTopologyIndexes() {
        final CompiledGraph compiled = compiler(
            List.of(source("source"), sink("sink")),
            List.of(edge("source", "sink", EdgeSpec.mpscRing(8)))
        ).compile();

        assertEquals("compiled", compiled.plan().name());
        assertEquals(List.of("sink"), compiled.workerOrder());
        assertEquals(GraphPlan.NodeKind.SOURCE, compiled.nodes().get("source").kind());
        assertEquals("source->sink", compiled.edges().get(0).key());
        assertEquals(0, compiled.edges().get(0).branchIndex());
        assertTrue(compiled.edges().get(0).sourceIngress());
        assertEquals(List.of(compiled.edges().get(0)), compiled.incomingByTarget().get("sink"));
        assertEquals(List.of(compiled.edges().get(0)), compiled.normalOutgoingBySource().get("source"));
        assertTrue(compiled.redirectBySourceAndTarget().isEmpty());
    }

    @Test
    void compileRejectsInvalidDocumentedGraphShapes() {
        assertThrows(GraphBuildException.class, () -> compiler(List.of(), List.of()).compile());
        assertThrows(GraphBuildException.class,
            () -> compiler(List.of(source("source"), source("source"), sink("sink")),
                List.of(edge("source", "sink", EdgeSpec.mpscRing(8)))).compile());
        assertThrows(GraphBuildException.class,
            () -> compiler(List.of(source("source"), sink("sink")),
                List.of(edge("source", "missing", EdgeSpec.mpscRing(8)))).compile());
        assertThrows(GraphBuildException.class,
            () -> compiler(List.of(source("source"), sink("sink")),
                List.of(edge("source", "sink", EdgeSpec.spscRing(8)))).compile());
    }

    private static GraphCompiler compiler(
        final List<NodeDefinition> nodes,
        final List<StaticGraphBuilder.PendingEdge> edges
    ) {
        return new GraphCompiler(
            "compiled",
            nodes,
            edges,
            StageExceptionHandler.failGraph(),
            false,
            GraphRuntimeConfig.defaults()
        );
    }

    private static NodeDefinition source(final String name) {
        return new NodeDefinition(
            name,
            GraphPlan.NodeKind.SOURCE,
            null,
            String.class,
            null,
            null,
            null,
            null,
            0,
            SourceMode.MULTI_PRODUCER,
            false,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    private static NodeDefinition sink(final String name) {
        return new NodeDefinition(
            name,
            GraphPlan.NodeKind.SINK,
            String.class,
            null,
            null,
            null,
            ignored -> { },
            StageSpec.singleThreaded(),
            1,
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    private static StaticGraphBuilder.PendingEdge edge(
        final String from,
        final String to,
        final EdgeSpec spec
    ) {
        return new StaticGraphBuilder.PendingEdge(from, to, spec, 0);
    }
}
