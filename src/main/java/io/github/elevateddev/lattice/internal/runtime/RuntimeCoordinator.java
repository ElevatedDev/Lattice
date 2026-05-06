package io.github.elevateddev.lattice.internal.runtime;

import io.github.elevateddev.lattice.graph.GraphRuntimeException;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.internal.edge.MessageEdge;
import io.github.elevateddev.lattice.internal.jfr.JfrEvents;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class RuntimeCoordinator {

    private static final MessageEdge[] NO_EDGES = new MessageEdge[0];
    private static final StageWorker[] NO_WORKERS = new StageWorker[0];
    private static final SourceEmitter<?>[] NO_SOURCES = new SourceEmitter<?>[0];

    private final String graphName;
    private final AtomicReference<GraphState> state;
    private final AtomicReference<Throwable> failure;
    private final GraphMetrics metrics;
    private final boolean jfrEnabled;
    private final boolean fusedLogicalEdgeCountersEnabled;
    private final boolean validateFusedTypes;
    private final boolean strictPlacement;
    private final boolean firstTouchPlacement;
    private final long placementBootstrapDelayMillis;
    private final AtomicInteger remainingWorkers;
    private final CountDownLatch bootstrapLatch;
    private final CountDownLatch runLatch = new CountDownLatch(1);
    private volatile MessageEdge[] edges = NO_EDGES;
    private volatile StageWorker[] workers = NO_WORKERS;
    private volatile SourceEmitter<?>[] sources = NO_SOURCES;
    private volatile boolean abortRequested;

    RuntimeCoordinator(
        final String graphName,
        final AtomicReference<GraphState> state,
        final AtomicReference<Throwable> failure,
        final GraphMetrics metrics,
        final int workerCount,
        final boolean jfrEnabled,
        final boolean fusedLogicalEdgeCountersEnabled,
        final boolean validateFusedTypes,
        final boolean strictPlacement,
        final boolean firstTouchPlacement,
        final long placementBootstrapDelayMillis
    ) {
        this.graphName = graphName;
        this.state = state;
        this.failure = failure;
        this.metrics = metrics;
        this.jfrEnabled = jfrEnabled;
        this.fusedLogicalEdgeCountersEnabled = fusedLogicalEdgeCountersEnabled;
        this.validateFusedTypes = validateFusedTypes;
        this.strictPlacement = strictPlacement;
        this.firstTouchPlacement = firstTouchPlacement;
        this.placementBootstrapDelayMillis = placementBootstrapDelayMillis;
        this.remainingWorkers = new AtomicInteger(workerCount);
        this.bootstrapLatch = new CountDownLatch(workerCount);
    }

    void attach(final MessageEdge[] edges, final StageWorker[] workers, final SourceEmitter<?>[] sources) {
        this.edges = edges.clone();
        this.workers = workers.clone();
        this.sources = sources.clone();
    }

    String graphName() {
        return graphName;
    }

    GraphState state() {
        return state.get();
    }

    GraphMetrics metrics() {
        return metrics;
    }

    boolean jfrEnabled() {
        return jfrEnabled;
    }

    boolean fusedLogicalEdgeCountersEnabled() {
        return fusedLogicalEdgeCountersEnabled;
    }

    boolean validateFusedTypes() {
        return validateFusedTypes;
    }

    boolean strictPlacement() {
        return strictPlacement;
    }

    boolean firstTouchPlacement() {
        return firstTouchPlacement;
    }

    long placementBootstrapDelayMillis() {
        return placementBootstrapDelayMillis;
    }

    boolean isAbortRequested() {
        return abortRequested;
    }

    boolean isStopping() {
        final GraphState current = state.get();
        return current == GraphState.DRAINING
            || current == GraphState.STOPPING
            || current == GraphState.STOPPED
            || current == GraphState.FAILED;
    }

    boolean hasInFlightWork() {
        if (hasInFlightInlineWork()) {
            return true;
        }
        final StageWorker[] currentWorkers = workers;
        for (int i = 0; i < currentWorkers.length; i++) {
            if (currentWorkers[i].active()) {
                return true;
            }
        }
        return false;
    }

    boolean hasInFlightInlineWork() {
        final SourceEmitter<?>[] currentSources = sources;
        for (int i = 0; i < currentSources.length; i++) {
            if (currentSources[i].pendingInline() > 0L) {
                return true;
            }
        }
        return false;
    }

    void workerBootstrapped() {
        bootstrapLatch.countDown();
    }

    void awaitWorkerBootstrap() throws InterruptedException {
        bootstrapLatch.await();
    }

    void releaseWorkers() {
        runLatch.countDown();
    }

    void awaitRunRelease() throws InterruptedException {
        runLatch.await();
    }

    void fail(final String stageName, final Throwable cause) {
        final GraphRuntimeException wrapped = cause instanceof GraphRuntimeException runtime
            ? runtime
            : new GraphRuntimeException("stage failed: " + stageName, cause);

        if (failure.compareAndSet(null, wrapped)) {
            abortRequested = true;
            metrics.recordStageException();
            state.set(GraphState.FAILED);
            releaseWorkers();

            if (jfrEnabled) {
                JfrEvents.stageException(graphName, stageName, cause);
            }

            final SourceEmitter<?>[] currentSources = sources;
            for (int i = 0; i < currentSources.length; i++) {
                currentSources[i].close();
            }

            final MessageEdge[] currentEdges = edges;
            for (int i = 0; i < currentEdges.length; i++) {
                currentEdges[i].close();
            }

            final StageWorker[] currentWorkers = workers;
            for (int i = 0; i < currentWorkers.length; i++) {
                currentWorkers[i].interrupt();
            }
        }
    }

    void requestStop() {
        abortRequested = true;
        while (true) {
            final GraphState current = state.get();
            if (current == GraphState.STOPPING || current == GraphState.STOPPED || current == GraphState.FAILED) {
                releaseWorkers();
                return;
            }
            if (state.compareAndSet(current, GraphState.STOPPING)) {
                releaseWorkers();
                return;
            }
        }
    }

    void poison(final String stageName, final Throwable cause, final MessageEdge input, final MessageEdge output) {
        poison(stageName, cause, input == null ? NO_EDGES : new MessageEdge[] { input },
            output == null ? NO_EDGES : new MessageEdge[] { output });
    }

    void poison(
        final String stageName,
        final Throwable cause,
        final MessageEdge[] inputs,
        final MessageEdge[] outputs
    ) {
        metrics.recordStageException();
        if (jfrEnabled) {
            JfrEvents.stageException(graphName, stageName, cause);
        }
        if (inputs != null) {
            for (int i = 0; i < inputs.length; i++) {
                inputs[i].close();
            }
        }
        if (outputs != null) {
            for (int i = 0; i < outputs.length; i++) {
                outputs[i].close();
            }
        }
    }

    void workerStopped() {
        if (remainingWorkers.decrementAndGet() == 0) {
            final MessageEdge[] currentEdges = edges;
            for (int i = 0; i < currentEdges.length; i++) {
                currentEdges[i].releaseRemainingAfterQuiescence();
            }
            metrics.markStopped();
            state.compareAndSet(GraphState.RUNNING, GraphState.STOPPED);
            state.compareAndSet(GraphState.QUIESCING, GraphState.STOPPED);
            state.compareAndSet(GraphState.DRAINING, GraphState.STOPPED);
            state.compareAndSet(GraphState.STOPPING, GraphState.STOPPED);
            if (jfrEnabled) {
                JfrEvents.graphStopped(graphName, state.get().name());
            }
        }
    }
}
