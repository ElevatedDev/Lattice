package com.lattice.internal.graph;

import com.lattice.graph.GraphPlan;
import com.lattice.graph.SourceMode;
import com.lattice.routing.BroadcastSpec;
import com.lattice.routing.DispatchSpec;
import com.lattice.routing.JoinSpec;
import com.lattice.routing.PartitionSpec;
import com.lattice.stage.BatchStageLogic;
import com.lattice.stage.StageLogic;
import com.lattice.stage.StageSpec;
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
    DispatchSpec<?> dispatchSpec,
    BroadcastSpec<?> broadcastSpec,
    PartitionSpec<?, ?> partitionSpec,
    JoinSpec<?> joinSpec
) {
}
