package com.lattice.internal.runtime;

import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.GraphState;
import com.lattice.graph.SourceMode;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.metrics.WorkerState;
import com.lattice.routing.Stamped;
import com.lattice.stage.Emitter;
import com.lattice.stage.Output;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

final class SourceEmitter<T> implements Emitter<T> {

    private static final VarHandle INLINE_ACTIVE;

    private static final boolean INLINE_DEPTH_TRACKING_ENABLED = Boolean.parseBoolean(
        System.getProperty("lattice.runtime.inlineDepthTracking", "true")
    );
    private static final boolean INLINE_LOGICAL_EDGE_METRICS = EdgeMetrics.hotCountersEnabled()
        && Boolean.parseBoolean(System.getProperty("lattice.runtime.fusedLogicalEdgeMetrics", "true"));

    static {
        try {
            INLINE_ACTIVE = MethodHandles.lookup().findVarHandle(
                SourceEmitter.class, "inlineActive", boolean.class);
        } catch (final ReflectiveOperationException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private final String name;
    private final EdgeSender sender;
    private final StageMetrics metrics;
    private final AtomicReference<GraphState> graphState;
    private final boolean stampItems;
    private final boolean singleProducer;
    private final RuntimeCoordinator coordinator;
    private final GraphMetrics graphMetrics;
    private volatile boolean closed;
    private long nextStamp;

    private Output<Object> inlineOutput;
    private InlineLifecycleParticipant inlineLifecycle;
    private EdgeMetrics inlineIngressMetrics;

    /**
     * In-flight inline emits admitted on the producer thread. Published before the state
     * re-check and cleared after the fused chain returns, so quiesce and termination cannot
     * miss producer-thread stage work. Inline source fusion is single-producer only, so the
     * nesting depth itself can stay producer-thread local while the active flag is published.
     */
    @SuppressWarnings("unused") // accessed via VarHandle
    private boolean inlineActive;
    private int inlineDepth;

    SourceEmitter(
        final String name,
        final EdgeSender sender,
        final StageMetrics metrics,
        final AtomicReference<GraphState> graphState,
        final boolean stampItems,
        final SourceMode sourceMode,
        final RuntimeCoordinator coordinator
    ) {
        this.name = name;
        this.sender = sender;
        this.metrics = metrics;
        this.graphState = graphState;
        this.stampItems = stampItems;
        this.singleProducer = sourceMode == SourceMode.SINGLE_PRODUCER;
        this.coordinator = coordinator;
        this.graphMetrics = coordinator.metrics();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void emit(final T item) {
        final Output<Object> inline = inlineOutput;
        if (inline != null) {
            pushInline(inline, item);
            return;
        }
        ensureOpenAndRunning();
        if (stampItems) {
            if (singleProducer) {
                emitStampedSingleProducer(item);
            } else {
                emitStamped(item);
            }
        } else {
            physicalSender().emitTrustedFromSource(item);
        }
    }

    void attachInlineOutput(final Output<Object> output) {
        if (this.inlineOutput != null) {
            throw new IllegalStateException("source " + name + " already has an inline-fused target");
        }
        this.inlineOutput = output;
    }

    void attachInlineOutput(final Output<Object> output, final EdgeMetrics ingressMetrics) {
        attachInlineOutput(output);
        this.inlineIngressMetrics = ingressMetrics;
    }

    void attachInlineOutput(
        final Output<Object> output,
        final InlineLifecycleParticipant lifecycle,
        final EdgeMetrics ingressMetrics
    ) {
        attachInlineOutput(output, ingressMetrics);
        this.inlineLifecycle = lifecycle;
    }

    @Override
    public boolean emit(final T item, final Duration timeout) {
        final Output<Object> inline = inlineOutput;
        if (inline != null) {
            pushInline(inline, item);
            return true;
        }
        ensureOpenAndRunning();

        if (stampItems) {
            return singleProducer
                ? emitStampedSingleProducer(item, timeout.toNanos())
                : emitStamped(item, timeout.toNanos());
        }
        return physicalSender().emit(item, timeout.toNanos());
    }

    @Override
    public boolean tryEmit(final T item) {
        final Output<Object> inline = inlineOutput;
        if (inline != null) {
            return tryPushInline(inline, item);
        }
        if (closed || graphState.get() != GraphState.RUNNING) {
            return false;
        }
        if (stampItems) {
            return singleProducer ? tryEmitStampedSingleProducer(item) : tryEmitStamped(item);
        }
        return physicalSender().tryEmitFromSource(item);
    }

    private void pushInline(final Output<Object> inline, final T item) {
        final long depth;
        final boolean trackDepth = trackInlineDepth();
        if (trackDepth) {
            depth = enterInline();
        } else {
            ensureOpenAndRunning();
            depth = 0L;
        }
        recordInlineEmit();
        try {
            inline.push(item);
        } catch (final Throwable ex) {
            throw pushInlineFailure(ex);
        } finally {
            if (trackDepth) {
                exitInline(depth);
            }
            notifyInlineExit();
        }
    }

    private boolean tryPushInline(final Output<Object> inline, final T item) {
        final long depth;
        final boolean trackDepth = trackInlineDepth();
        if (trackDepth) {
            depth = inlineDepth;
            inlineDepth = (int) depth + 1;
            if (depth == 0L) {
                INLINE_ACTIVE.setOpaque(this, true);
            }
            if (closed || graphState.get() != GraphState.RUNNING) {
                exitInline(depth);
                return false;
            }
        } else {
            if (closed || graphState.get() != GraphState.RUNNING) {
                return false;
            }
            depth = 0L;
        }
        recordInlineEmit();
        try {
            inline.push(item);
        } catch (final Throwable ex) {
            throw pushInlineFailure(ex);
        } finally {
            if (trackDepth) {
                exitInline(depth);
            }
            notifyInlineExit();
        }
        return true;
    }

    private void recordInlineEmit() {
        if (StageMetrics.hotCountersEnabled()) {
            metrics.recordEmit();
        }
        if (!INLINE_LOGICAL_EDGE_METRICS) {
            return;
        }
        final EdgeMetrics ingressMetrics = inlineIngressMetrics;
        if (ingressMetrics == null) {
            return;
        }
        ingressMetrics.recordEmit();
        graphMetrics.recordEmit();
        ingressMetrics.recordConsume();
        graphMetrics.recordConsume();
    }

    private long enterInline() {
        final int depth = inlineDepth;
        inlineDepth = depth + 1;
        if (depth == 0) {
            INLINE_ACTIVE.setOpaque(this, true);
        }
        try {
            ensureOpenAndRunning();
            return depth;
        } catch (final RuntimeException | Error ex) {
            exitInline(depth);
            throw ex;
        }
    }

    private void exitInline(final long depth) {
        inlineDepth = (int) depth;
        if (depth == 0L) {
            INLINE_ACTIVE.setOpaque(this, false);
        }
    }

    private boolean trackInlineDepth() {
        return INLINE_DEPTH_TRACKING_ENABLED || inlineLifecycle != null;
    }

    private void notifyInlineExit() {
        final InlineLifecycleParticipant lifecycle = inlineLifecycle;
        if (lifecycle != null) {
            lifecycle.afterInlineExit();
        }
    }

    private RuntimeException pushInlineFailure(final Throwable ex) {
        if (ex instanceof StageWorker.FusedStageException fused) {
            final Throwable cause = fused.getCause() != null ? fused.getCause() : fused;
            coordinator.fail(fused.stageName(), cause);
            return publicEmitFailure(fused.stageName(), cause);
        }
        if (ex instanceof GraphRuntimeException gex) {
            coordinator.fail(name, gex);
            return gex;
        }
        if (ex instanceof Error error) {
            coordinator.fail(name, error);
            sneakyThrow(error);
            throw new AssertionError("unreachable");
        }
        coordinator.fail(name, ex);
        return publicEmitFailure(name, ex);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(final Throwable ex) throws T {
        throw (T) ex;
    }

    /**
     * Counts producer-thread inline emits that have been admitted but have not completed.
     * The release/acquire pair with {@link RuntimeCoordinator#hasInFlightWork()} keeps
     * quiesce and graph termination from observing the inline path as idle too early.
     */
    long pendingInline() {
        if (!trackInlineDepth()) {
            return 0L;
        }
        return (boolean) INLINE_ACTIVE.getOpaque(this) ? 1L : 0L;
    }

    private static GraphRuntimeException publicEmitFailure(final String stageName, final Throwable cause) {
        return new GraphRuntimeException("stage failed: " + stageName, cause);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    void emitPlain(final T item) {
        ensureOpenAndRunning();
        physicalSender().emitFromSource(item);
    }

    void emitPreallocatedTrusted(final T item) {
        ensureOpenAndRunning();
        physicalSender().emitTrustedFromSource(item);
    }

    boolean emitPlain(final T item, final Duration timeout) {
        ensureOpenAndRunning();
        return physicalSender().emit(item, timeout.toNanos());
    }

    boolean tryEmitPlain(final T item) {
        if (closed || graphState.get() != GraphState.RUNNING) {
            return false;
        }
        return physicalSender().tryEmitFromSource(item);
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        final EdgeSender physicalSender = sender;
        if (physicalSender != null) {
            physicalSender.close();
        }
        final InlineLifecycleParticipant lifecycle = inlineLifecycle;
        if (lifecycle != null) {
            lifecycle.sourceClosed();
        }
        metrics.workerState(WorkerState.STOPPED);
        metrics.markStopped();
    }

    void markStarted() {
        metrics.markStarted();
        metrics.workerState(WorkerState.RUNNING);
    }

    private void ensureOpenAndRunning() {
        if (closed) {
            throw new GraphRuntimeException("source is closed: " + name);
        }
        if (graphState.get() != GraphState.RUNNING) {
            throw new GraphRuntimeException("graph is not running; source " + name + " cannot emit");
        }
    }

    private synchronized void emitStamped(final T item) {
        emitStampedSingleProducer(item);
    }

    private void emitStampedSingleProducer(final T item) {
        physicalSender().emit(Stamped.of(nextStamp, item));
        nextStamp++;
    }

    private synchronized boolean emitStamped(final T item, final long timeoutNanos) {
        return emitStampedSingleProducer(item, timeoutNanos);
    }

    private boolean emitStampedSingleProducer(final T item, final long timeoutNanos) {
        if (!physicalSender().emit(Stamped.of(nextStamp, item), timeoutNanos)) {
            return false;
        }
        nextStamp++;
        return true;
    }

    private synchronized boolean tryEmitStamped(final T item) {
        return tryEmitStampedSingleProducer(item);
    }

    private boolean tryEmitStampedSingleProducer(final T item) {
        if (!physicalSender().tryEmit(Stamped.of(nextStamp, item))) {
            return false;
        }
        nextStamp++;
        return true;
    }

    private EdgeSender physicalSender() {
        final EdgeSender physicalSender = sender;
        if (physicalSender == null) {
            throw new GraphRuntimeException("source " + name + " has no physical sender");
        }
        return physicalSender;
    }
}
