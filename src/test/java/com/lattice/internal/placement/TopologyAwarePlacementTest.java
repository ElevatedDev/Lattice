package com.lattice.internal.placement;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphPlan;
import com.lattice.graph.GraphPlacementSpec;
import com.lattice.graph.SourceMode;
import com.lattice.internal.graph.CompiledGraph;
import com.lattice.internal.graph.EdgeDefinition;
import com.lattice.internal.graph.GraphRuntimeConfig;
import com.lattice.internal.graph.NodeDefinition;
import com.lattice.stage.StageExceptionHandler;
import com.lattice.stage.StageSpec;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TopologyAwarePlacementTest {

    @Test
    void disabledTopologyAwarePlacementDoesNotAssignPins() {
        final CompiledGraph compiled = compiled(GraphRuntimeConfig.defaults());

        assertTrue(TopologyAwarePlacement.plan(compiled, List.of("sink")).isEmpty());
    }

    @Test
    void unavailableNativeTopologyLeavesWorkersUnpinnedEvenWhenRequested() {
        final GraphRuntimeConfig config = new GraphRuntimeConfig(
            com.lattice.graph.FusionSpec.defaults(),
            com.lattice.graph.MetricsSpec.off(),
            GraphPlacementSpec.off().topologyAware(true),
            com.lattice.graph.DiagnosticsSpec.off()
        );

        TopologyAwarePlacement.plan(compiled(config), List.of("sink"));
    }

    private static CompiledGraph compiled(final GraphRuntimeConfig config) {
        final GraphPlan plan = new GraphPlan(
            "placement",
            List.of(
                new GraphPlan.Node("source", GraphPlan.NodeKind.SOURCE, null, String.class, null),
                new GraphPlan.Node("sink", GraphPlan.NodeKind.SINK, String.class, null, StageSpec.singleThreaded())
            ),
            List.of(new GraphPlan.Edge("source", "sink", String.class, EdgeSpec.mpscRing(8))),
            List.of("sink")
        );
        final NodeDefinition source = new NodeDefinition(
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
        final NodeDefinition sink = new NodeDefinition(
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
        return new CompiledGraph(
            plan,
            Map.of("source", source, "sink", sink),
            List.of(edge),
            Map.of("sink", List.of(edge)),
            Map.of("source", List.of(edge)),
            Map.of("source", List.of(edge)),
            Map.of(),
            List.of("sink"),
            StageExceptionHandler.failGraph(),
            false,
            config
        );
    }
}
