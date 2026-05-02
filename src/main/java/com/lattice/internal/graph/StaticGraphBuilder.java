package com.lattice.internal.graph;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.DiagnosticsSpec;
import com.lattice.graph.FusionSpec;
import com.lattice.graph.GraphPlan;
import com.lattice.graph.GraphPlacementSpec;
import com.lattice.graph.MetricsSpec;
import com.lattice.graph.PreallocationSpec;
import com.lattice.graph.SourceMode;
import com.lattice.graph.StaticGraph;
import com.lattice.internal.runtime.DefaultStaticGraph;
import com.lattice.routing.BroadcastSpec;
import com.lattice.routing.DispatchSpec;
import com.lattice.routing.JoinSpec;
import com.lattice.routing.PartitionSpec;
import com.lattice.routing.Stamped;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.BatchStageLogic;
import com.lattice.stage.StageExceptionHandler;
import com.lattice.stage.StageLogic;
import com.lattice.stage.StageSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class StaticGraphBuilder implements StaticGraph.Builder {

    private final String name;
    private final List<NodeDefinition> nodes = new ArrayList<>();
    private final List<PendingEdge> edges = new ArrayList<>();
    private StageExceptionHandler exceptionHandler = StageExceptionHandler.failGraph();
    private boolean customExceptionHandler;
    private FusionSpec fusionSpec = FusionSpec.defaults();
    private MetricsSpec metricsSpec = MetricsSpec.off();
    private GraphPlacementSpec placementSpec = GraphPlacementSpec.off();
    private DiagnosticsSpec diagnosticsSpec = DiagnosticsSpec.off();

    public StaticGraphBuilder(final String name) {
        this.name = requireName(name, "graph");
    }

    @Override
    public StaticGraph.Builder fusion(final FusionSpec spec) {
        this.fusionSpec = Objects.requireNonNull(spec, "spec");
        return this;
    }

    @Override
    public StaticGraph.Builder metrics(final MetricsSpec spec) {
        this.metricsSpec = Objects.requireNonNull(spec, "spec");
        return this;
    }

    @Override
    public StaticGraph.Builder placement(final GraphPlacementSpec spec) {
        this.placementSpec = Objects.requireNonNull(spec, "spec");
        return this;
    }

    @Override
    public StaticGraph.Builder diagnostics(final DiagnosticsSpec spec) {
        this.diagnosticsSpec = Objects.requireNonNull(spec, "spec");
        return this;
    }

    @Override
    public <T> StaticGraph.Builder source(final String name, final Class<T> type, final SourceMode mode) {
        nodes.add(new NodeDefinition(
            requireName(name, "source"),
            GraphPlan.NodeKind.SOURCE,
            null,
            Objects.requireNonNull(type, "type"),
            null,
            null,
            null,
            null,
            nodes.size(),
            Objects.requireNonNull(mode, "mode"),
            false,
            null,
            null,
            null,
            null,
            null,
            null
        ));

        return this;
    }

    @Override
    public <T> StaticGraph.Builder preallocatedSource(
        final String name,
        final Class<T> type,
        final PreallocationSpec<T> spec
    ) {
        nodes.add(new NodeDefinition(
            requireName(name, "source"),
            GraphPlan.NodeKind.SOURCE,
            null,
            Objects.requireNonNull(type, "type"),
            null,
            null,
            null,
            null,
            nodes.size(),
            SourceMode.SINGLE_PRODUCER,
            false,
            null,
            Objects.requireNonNull(spec, "spec"),
            null,
            null,
            null,
            null
        ));

        return this;
    }

    @Override
    public <T> StaticGraph.Builder stampedSource(
        final String name,
        final Class<T> payloadType,
        final SourceMode mode
    ) {
        nodes.add(new NodeDefinition(
            requireName(name, "source"),
            GraphPlan.NodeKind.SOURCE,
            null,
            Stamped.class,
            null,
            null,
            null,
            null,
            nodes.size(),
            Objects.requireNonNull(mode, "mode"),
            true,
            Objects.requireNonNull(payloadType, "payloadType"),
            null,
            null,
            null,
            null,
            null
        ));

        return this;
    }

    @Override
    public <I, O> StaticGraph.Builder stage(
        final String name,
        final Class<I> inputType,
        final Class<O> outputType,
        final StageLogic<I, O> logic,
        final StageSpec spec
    ) {
        nodes.add(new NodeDefinition(
            requireName(name, "stage"),
            GraphPlan.NodeKind.STAGE,
            Objects.requireNonNull(inputType, "inputType"),
            Objects.requireNonNull(outputType, "outputType"),
            Objects.requireNonNull(logic, "logic"),
            null,
            null,
            Objects.requireNonNull(spec, "spec"),
            nodes.size(),
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            null
        ));
        return this;
    }

    @Override
    public <I, O> StaticGraph.Builder batchStage(
        final String name,
        final Class<I> inputType,
        final Class<O> outputType,
        final BatchStageLogic<I, O> logic,
        final StageSpec spec
    ) {
        final StageSpec stageSpec = Objects.requireNonNull(spec, "spec");
        if (stageSpec.batchPolicy().kind() == BatchPolicy.BatchKind.DISABLED) {
            throw new IllegalArgumentException("batch stages require StageSpec.batch(BatchPolicy.maxItems(...) or linger(...))");
        }
        nodes.add(new NodeDefinition(
            requireName(name, "stage"),
            GraphPlan.NodeKind.STAGE,
            Objects.requireNonNull(inputType, "inputType"),
            Objects.requireNonNull(outputType, "outputType"),
            null,
            Objects.requireNonNull(logic, "logic"),
            null,
            stageSpec,
            nodes.size(),
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            null
        ));
        return this;
    }

    @Override
    public <I> StaticGraph.Builder sink(
        final String name,
        final Class<I> inputType,
        final Consumer<? super I> consumer,
        final StageSpec spec
    ) {
        nodes.add(new NodeDefinition(
            requireName(name, "sink"),
            GraphPlan.NodeKind.SINK,
            Objects.requireNonNull(inputType, "inputType"),
            null,
            null,
            null,
            Objects.requireNonNull(consumer, "consumer"),
            Objects.requireNonNull(spec, "spec"),
            nodes.size(),
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            null
        ));
        return this;
    }

    @Override
    public <T> StaticGraph.Builder dispatch(
        final String name,
        final Class<T> type,
        final DispatchSpec<? super T> spec,
        final StageSpec stageSpec
    ) {
        nodes.add(new NodeDefinition(
            requireName(name, "dispatch"),
            GraphPlan.NodeKind.DISPATCH,
            Objects.requireNonNull(type, "type"),
            type,
            null,
            null,
            null,
            Objects.requireNonNull(stageSpec, "stageSpec"),
            nodes.size(),
            null,
            false,
            null,
            null,
            Objects.requireNonNull(spec, "spec"),
            null,
            null,
            null
        ));
        return this;
    }

    @Override
    public <T> StaticGraph.Builder broadcast(
        final String name,
        final Class<T> type,
        final BroadcastSpec<? super T> spec,
        final StageSpec stageSpec
    ) {
        nodes.add(new NodeDefinition(
            requireName(name, "broadcast"),
            GraphPlan.NodeKind.BROADCAST,
            Objects.requireNonNull(type, "type"),
            type,
            null,
            null,
            null,
            Objects.requireNonNull(stageSpec, "stageSpec"),
            nodes.size(),
            null,
            false,
            null,
            null,
            null,
            Objects.requireNonNull(spec, "spec"),
            null,
            null
        ));
        return this;
    }

    @Override
    public <T, K> StaticGraph.Builder partition(
        final String name,
        final Class<T> type,
        final PartitionSpec<? super T, K> spec,
        final StageSpec stageSpec
    ) {
        nodes.add(new NodeDefinition(
            requireName(name, "partition"),
            GraphPlan.NodeKind.PARTITION,
            Objects.requireNonNull(type, "type"),
            type,
            null,
            null,
            null,
            Objects.requireNonNull(stageSpec, "stageSpec"),
            nodes.size(),
            null,
            false,
            null,
            null,
            null,
            null,
            Objects.requireNonNull(spec, "spec"),
            null
        ));
        return this;
    }

    @Override
    public <O> StaticGraph.Builder join(
        final String name,
        final Class<O> outputType,
        final JoinSpec<? extends O> spec,
        final StageSpec stageSpec
    ) {
        nodes.add(new NodeDefinition(
            requireName(name, "join"),
            GraphPlan.NodeKind.JOIN,
            Object.class,
            Objects.requireNonNull(outputType, "outputType"),
            null,
            null,
            null,
            Objects.requireNonNull(stageSpec, "stageSpec"),
            nodes.size(),
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            Objects.requireNonNull(spec, "spec")
        ));
        return this;
    }

    @Override
    public StaticGraph.Builder edge(final String from, final String to, final EdgeSpec spec) {
        edges.add(new PendingEdge(requireName(from, "edge source"), requireName(to, "edge target"), Objects.requireNonNull(spec, "spec"), edges.size()));
        return this;
    }

    @Override
    public StaticGraph.Builder exceptionHandler(final StageExceptionHandler exceptionHandler) {
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler, "exceptionHandler");
        this.customExceptionHandler = true;
        return this;
    }

    @Override
    public StaticGraph build() {
        final CompiledGraph compiled = new GraphCompiler(
            name,
            nodes,
            edges,
            exceptionHandler,
            customExceptionHandler,
            new GraphRuntimeConfig(fusionSpec, metricsSpec, placementSpec, diagnosticsSpec)
        ).compile();
        return new DefaultStaticGraph(compiled);
    }

    private static String requireName(final String value, final String label) {
        final String trimmed = Objects.requireNonNull(value, label + " name").trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(label + " name must not be blank");
        }
        return trimmed;
    }

    record PendingEdge(String from, String to, EdgeSpec spec, int declarationOrder) {
    }
}
