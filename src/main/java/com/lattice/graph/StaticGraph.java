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

/**
 * A compiled static topology graph.
 * <p>
 * Graph structure is fixed after {@link Builder#build()}. Application code
 * publishes through source emitters, observes immutable plan metadata through
 * {@link #plan()}, and reads counters through {@link #metrics()}.
 */
public interface StaticGraph extends AutoCloseable {
    /**
     * Creates a builder for a named graph.
     */
    static Builder builder(final String name) {
        return new com.lattice.internal.graph.StaticGraphBuilder(name);
    }

    /**
     * Returns the immutable plan compiled for this graph.
     */
    GraphPlan plan();

    /**
     * Returns live graph, stage, and edge metrics.
     */
    GraphMetrics metrics();

    /**
     * Returns an emitter for a normal source.
     */
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

    /**
     * Starts graph worker threads.
     */
    void start();

    /**
     * Requests graph shutdown and waits according to runtime defaults.
     */
    void stop();

    /**
     * Requests graph shutdown and waits up to the timeout.
     *
     * @return {@code true} if the graph stopped before the timeout
     */
    boolean stop(Duration timeout);

    /**
     * Pauses source acceptance and waits for in-flight work to drain.
     */
    boolean quiesce(Duration timeout);

    /**
     * Resumes a quiesced graph.
     */
    void resume();

    /**
     * Stops the graph as failed and wakes waiting workers.
     */
    void abort();

    /**
     * Waits until the graph reaches a terminal state.
     */
    void awaitTermination() throws InterruptedException;

    /**
     * Waits until the graph reaches a terminal state or timeout expires.
     *
     * @return {@code true} if the graph terminated before the timeout
     */
    boolean awaitTermination(Duration timeout) throws InterruptedException;

    /**
     * Returns the current graph lifecycle state.
     */
    GraphState state();

    /**
     * Returns the failure that stopped the graph, when any.
     */
    Optional<Throwable> failure();

    @Override
    default void close() {
        stop();
    }

    interface Builder {

        /**
         * Adds a multi-producer source.
         */
        default <T> Builder source(final String name, final Class<T> type) {
            return source(name, type, SourceMode.MULTI_PRODUCER);
        }

        /**
         * Adds a source with an explicit producer mode.
         */
        <T> Builder source(String name, Class<T> type, SourceMode mode);

        /**
         * Adds a source whose items are claimed from a reusable pool.
         */
        <T> Builder preallocatedSource(String name, Class<T> type, PreallocationSpec<T> spec);

        /**
         * Adds a factory-backed preallocated source.
         */
        default <T> Builder preallocatedSource(
            final String name,
            final Class<T> type,
            final IntFunction<? extends T> factory
        ) {
            return preallocatedSource(name, type, PreallocationSpec.pool(factory));
        }

        /**
         * Adds a factory-backed preallocated source with an explicit pool size.
         */
        default <T> Builder preallocatedSource(
            final String name,
            final Class<T> type,
            final IntFunction<? extends T> factory,
            final int poolSize
        ) {
            return preallocatedSource(name, type, PreallocationSpec.pool(factory, poolSize));
        }

        /**
         * Adds a fixed-pool preallocated source.
         */
        default <T> Builder preallocatedSource(
            final String name,
            final Class<T> type,
            final T[] pool
        ) {
            return preallocatedSource(name, type, PreallocationSpec.fixedPool(pool));
        }

        /**
         * Adds a multi-producer source that emits {@link com.lattice.routing.Stamped}
         * values.
         */
        default <T> Builder stampedSource(final String name, final Class<T> payloadType) {
            return stampedSource(name, payloadType, SourceMode.MULTI_PRODUCER);
        }

        /**
         * Adds a source that stamps emitted payloads for join correlation.
         */
        <T> Builder stampedSource(String name, Class<T> payloadType, SourceMode mode);

        /**
         * Adds a one-message-at-a-time transformation stage.
         */
        <I, O> Builder stage(
            String name,
            Class<I> inputType,
            Class<O> outputType,
            StageLogic<I, O> logic,
            StageSpec spec
        );

        /**
         * Adds a batch transformation stage.
         */
        <I, O> Builder batchStage(
            String name,
            Class<I> inputType,
            Class<O> outputType,
            BatchStageLogic<I, O> logic,
            StageSpec spec
        );

        /**
         * Adds a terminal consumer stage.
         */
        <I> Builder sink(String name, Class<I> inputType, Consumer<? super I> consumer, StageSpec spec);

        /**
         * Adds a routing stage that chooses one downstream branch.
         */
        <T> Builder dispatch(String name, Class<T> type, DispatchSpec<? super T> spec, StageSpec stageSpec);

        /**
         * Adds a routing stage that publishes to every downstream branch.
         */
        <T> Builder broadcast(String name, Class<T> type, BroadcastSpec<? super T> spec, StageSpec stageSpec);

        /**
         * Adds a keyed partitioning stage with a fixed lane count.
         */
        <T, K> Builder partition(String name, Class<T> type, PartitionSpec<? super T, K> spec, StageSpec stageSpec);

        /**
         * Adds a join stage that correlates inputs by stamp.
         */
        <O> Builder join(String name, Class<O> outputType, JoinSpec<? extends O> spec, StageSpec stageSpec);

        /**
         * Adds a directed edge between two nodes.
         */
        Builder edge(String from, String to, EdgeSpec spec);

        /**
         * Sets the graph-level stage exception handler.
         */
        Builder exceptionHandler(StageExceptionHandler exceptionHandler);

        /**
         * Compiles and validates the graph.
         */
        StaticGraph build();
    }
}
