package io.github.elevateddev.lattice.internal.graph;

import io.github.elevateddev.lattice.graph.GraphPlan;
import io.github.elevateddev.lattice.stage.StageExceptionHandler;
import java.util.List;
import java.util.Map;

public record CompiledGraph(
    GraphPlan plan,
    Map<String, NodeDefinition> nodes,
    List<EdgeDefinition> edges,
    Map<String, List<EdgeDefinition>> incomingByTarget,
    Map<String, List<EdgeDefinition>> outgoingBySource,
    Map<String, List<EdgeDefinition>> normalOutgoingBySource,
    Map<String, Map<String, EdgeDefinition>> redirectBySourceAndTarget,
    List<String> workerOrder,
    StageExceptionHandler exceptionHandler,
    boolean customExceptionHandler,
    GraphRuntimeConfig runtimeConfig
) {
}
