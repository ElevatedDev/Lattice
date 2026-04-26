package com.lattice.graph;

import com.lattice.edge.EdgeSpec;
import com.lattice.metrics.GraphMetrics;
import com.lattice.routing.BroadcastSpec;
import com.lattice.routing.DispatchSpec;
import com.lattice.routing.JoinSpec;
import com.lattice.routing.PartitionSpec;
import com.lattice.stage.BatchStageLogic;
import com.lattice.stage.Emitter;
import com.lattice.stage.PreallocatedEmitter;
import com.lattice.stage.StageExceptionHandler;
import com.lattice.stage.StageLogic;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.IntFunction;

public interface StaticGraph extends AutoCloseable {
    static Builder builder(final String name) {
        return new com.lattice.internal.graph.StaticGraphBuilder(name);
    }

    GraphPlan plan();

    GraphMetrics metrics();

    <T> Emitter<T> emitter(String sourceName, Class<T> type);

    /**
     * Returns the claim/emit API for a preallocated source.
     * <p>
     * Sources declared with {@link Builder#preallocatedSource(String, Class, PreallocationSpec)}
     * must be emitted through this method. Calling {@link #emitter(String, Class)}
     * for a preallocated source is rejected so callers do not accidentally bypass
     * the source pool.
     */
    <T> PreallocatedEmitter<T> preallocatedEmitter(String sourceName, Class<T> type);

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

        default <T> Builder source(final String name, final Class<T> type) {
            return source(name, type, SourceMode.MULTI_PRODUCER);
        }

        <T> Builder source(String name, Class<T> type, SourceMode mode);

        <T> Builder preallocatedSource(String name, Class<T> type, PreallocationSpec<T> spec);

        default <T> Builder preallocatedSource(
            final String name,
            final Class<T> type,
            final IntFunction<? extends T> factory
        ) {
            return preallocatedSource(name, type, PreallocationSpec.pool(factory));
        }

        default <T> Builder preallocatedSource(
            final String name,
            final Class<T> type,
            final IntFunction<? extends T> factory,
            final int poolSize
        ) {
            return preallocatedSource(name, type, PreallocationSpec.pool(factory, poolSize));
        }

        default <T> Builder stampedSource(final String name, final Class<T> payloadType) {
            return stampedSource(name, payloadType, SourceMode.MULTI_PRODUCER);
        }

        <T> Builder stampedSource(String name, Class<T> payloadType, SourceMode mode);

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
