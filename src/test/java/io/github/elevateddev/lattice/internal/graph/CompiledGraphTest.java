package io.github.elevateddev.lattice.internal.graph;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.GraphPlan;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.stage.StageExceptionHandler;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class CompiledGraphTest {

    @Test
    void recordExposesCompiledTopologyAndRuntimeConfiguration() {
        final GraphPlan plan = new GraphPlan(
            "compiled",
            List.of(sourceNode(), sinkNode()),
            List.of(new GraphPlan.Edge("source", "sink", String.class, EdgeSpec.mpscRing(8))),
            List.of("sink")
        );
        final NodeDefinition source = sourceDefinition();
        final NodeDefinition sink = sinkDefinition();
        final EdgeDefinition edge = new EdgeDefinition(
            "source",
            "sink",
            String.class,
            EdgeSpec.mpscRing(8),
            0,
            0,
            false,
            true
        );
        final StageExceptionHandler handler = StageExceptionHandler.failGraph();
        final GraphRuntimeConfig runtimeConfig = GraphRuntimeConfig.defaults();
        final CompiledGraph compiled = new CompiledGraph(
            plan,
            Map.of("source", source, "sink", sink),
            List.of(edge),
            Map.of("sink", List.of(edge)),
            Map.of("source", List.of(edge)),
            Map.of("source", List.of(edge)),
            Map.of(),
            List.of("sink"),
            handler,
            false,
            runtimeConfig
        );

        assertSame(plan, compiled.plan());
        assertSame(source, compiled.nodes().get("source"));
        assertEquals(List.of(edge), compiled.edges());
        assertEquals(List.of(edge), compiled.incomingByTarget().get("sink"));
        assertEquals(List.of("sink"), compiled.workerOrder());
        assertSame(handler, compiled.exceptionHandler());
        assertSame(runtimeConfig, compiled.runtimeConfig());
    }

    static GraphPlan.Node sourceNode() {
        return new GraphPlan.Node("source", GraphPlan.NodeKind.SOURCE, null, String.class, null);
    }

    static GraphPlan.Node sinkNode() {
        return new GraphPlan.Node("sink", GraphPlan.NodeKind.SINK, String.class, null, StageSpec.singleThreaded());
    }

    static NodeDefinition sourceDefinition() {
        return new NodeDefinition(
            "source",
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

    static NodeDefinition sinkDefinition() {
        return new NodeDefinition(
            "sink",
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
}
