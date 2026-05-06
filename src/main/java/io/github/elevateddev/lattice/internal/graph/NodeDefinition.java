package io.github.elevateddev.lattice.internal.graph;

import io.github.elevateddev.lattice.graph.GraphPlan;
import io.github.elevateddev.lattice.graph.PreallocationSpec;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.routing.BroadcastSpec;
import io.github.elevateddev.lattice.routing.DispatchSpec;
import io.github.elevateddev.lattice.routing.JoinSpec;
import io.github.elevateddev.lattice.routing.PartitionSpec;
import io.github.elevateddev.lattice.stage.BatchStageLogic;
import io.github.elevateddev.lattice.stage.StageLogic;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.util.function.Consumer;

public record NodeDefinition(
    String name,
    GraphPlan.NodeKind kind,
    Class<?> inputType,
    Class<?> outputType,
    StageLogic<?, ?> logic,
    BatchStageLogic<?, ?> batchLogic,
    Consumer<?> sink,
    StageSpec spec,
    int declarationOrder,
    SourceMode sourceMode,
    boolean stampedSource,
    Class<?> sourcePayloadType,
    PreallocationSpec<?> preallocationSpec,
    DispatchSpec<?> dispatchSpec,
    BroadcastSpec<?> broadcastSpec,
    PartitionSpec<?, ?> partitionSpec,
    JoinSpec<?> joinSpec
) {
}
