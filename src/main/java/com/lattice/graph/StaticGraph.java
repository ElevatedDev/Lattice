package com.lattice.graph;

import com.lattice.edge.EdgeSpec;
import com.lattice.metrics.GraphMetrics;
import com.lattice.routing.BroadcastSpec;
import com.lattice.routing.DispatchSpec;
import com.lattice.routing.JoinSpec;
import com.lattice.routing.PartitionSpec;
import com.lattice.stage.BatchStageLogic;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageExceptionHandler;
import com.lattice.stage.StageLogic;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

public interface StaticGraph extends AutoCloseable {
    static Builder builder(final String name) {
        return new com.lattice.internal.graph.StaticGraphBuilder(name);
    }

    GraphPlan plan();

    GraphMetrics metrics();

    <T> Emitter<T> emitter(String sourceName, Class<T> type);

    void start();

    void stop();

    boolean stop(Duration timeout);

    boolean quiesce(Duration timeout);

    void resume();

    void abort();

    void awaitTermination() throws InterruptedException;

    boolean awaitTermination(Duration timeout) throws InterruptedException;

    GraphState state();

    Optional<Throwable> failure();

    @Override
    default void close() {
        stop();
    }

    interface Builder {

        <T> Builder source(String name, Class<T> type);

        <T> Builder stampedSource(String name, Class<T> payloadType);

        <I, O> Builder stage(
            String name,
            Class<I> inputType,
            Class<O> outputType,
            StageLogic<I, O> logic,
            StageSpec spec
        );

        <I, O> Builder batchStage(
            String name,
            Class<I> inputType,
            Class<O> outputType,
            BatchStageLogic<I, O> logic,
            StageSpec spec
        );

        <I> Builder sink(String name, Class<I> inputType, Consumer<? super I> consumer, StageSpec spec);

        <T> Builder dispatch(String name, Class<T> type, DispatchSpec<? super T> spec, StageSpec stageSpec);

        <T> Builder broadcast(String name, Class<T> type, BroadcastSpec<? super T> spec, StageSpec stageSpec);

        <T, K> Builder partition(String name, Class<T> type, PartitionSpec<? super T, K> spec, StageSpec stageSpec);

        <O> Builder join(String name, Class<O> outputType, JoinSpec<? extends O> spec, StageSpec stageSpec);

        Builder edge(String from, String to, EdgeSpec spec);

        Builder exceptionHandler(StageExceptionHandler exceptionHandler);

        StaticGraph build();
    }
}
