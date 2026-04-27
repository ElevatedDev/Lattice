package com.lattice.internal.runtime;

import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.GraphState;
import com.lattice.graph.SourceMode;
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

    private static final VarHandle INLINE_DEPTH;

    static {
        try {
            INLINE_DEPTH = MethodHandles.lookup().findVarHandle(
                SourceEmitter.class, "inlineDepth", long.class);
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
    private volatile boolean closed;
    private long nextStamp;

    private Output<Object> inlineOutput;
    /**
     * Per-source counter of currently-running inline emits. Writes happen only on the
     * source's own producer thread (inline fusion is restricted to {@code SINGLE_PRODUCER}
     * sources at the {@link DefaultStaticGraph} eligibility check), so the producer can use
     * relaxed plain reads + {@code setRelease} writes to publish progress visibility without
     * a {@code LOCK}-prefixed atomic on x86. {@link RuntimeCoordinator#hasInFlightWork()}
     * sums these counters via {@code getAcquire} from the quiesce thread, which is a plain
     * load on x86 and provides happens-before with the producer's release writes. The
     * tradeoff vs a single graph-wide {@link java.util.concurrent.atomic.AtomicInteger}
     * (~6-10ns LOCK XADD pair per emit on x86) is that quiesce now does an O(sources) scan
     * instead of one read; sources count is bounded and small, so the trade is overwhelmingly
     * positive on the steady-state hot path.
     */
    @SuppressWarnings("unused") // accessed via VarHandle
    private long inlineDepth;

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
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void emit(final T item) {
        ensureOpenAndRunning();
        final Output<Object> inline = inlineOutput;
        if (inline != null) {
            pushInline(inline, item);
            return;
        }
        if (stampItems) {
            if (singleProducer) {
                emitStampedSingleProducer(item);
            } else {
                emitStamped(item);
            }
        } else {
            sender.emitTrustedFromSource(item);
        }
    }

    void attachInlineOutput(final Output<Object> output) {
        if (this.inlineOutput != null) {
            throw new IllegalStateException("source " + name + " already has an inline-fused target");
        }
        this.inlineOutput = output;
    }

    @Override
    public boolean emit(final T item, final Duration timeout) {
        ensureOpenAndRunning();
        final Output<Object> inline = inlineOutput;
        if (inline != null) {
            // The inline-fused chain is synchronous on the producer thread; there is no ring
            // to fill, so the timeout is effectively irrelevant: the chain either runs to
            // completion or throws. Treat it identically to the unbounded emit path.
            pushInline(inline, item);
            return true;
        }

        if (stampItems) {
            return singleProducer
                ? emitStampedSingleProducer(item, timeout.toNanos())
                : emitStamped(item, timeout.toNanos());
        }
        return sender.emit(item, timeout.toNanos());
    }

    @Override
    public boolean tryEmit(final T item) {
        if (closed || graphState.get() != GraphState.RUNNING) {
            return false;
        }
        final Output<Object> inline = inlineOutput;
        if (inline != null) {
            // Same reasoning as emit(T, Duration): the inline chain has no backpressure to
            // refuse on, so tryEmit always proceeds and runs the chain synchronously. Any
            // user-stage exception fails the graph just like the bounded path.
            pushInline(inline, item);
            return true;
        }
        if (stampItems) {
            return singleProducer ? tryEmitStampedSingleProducer(item) : tryEmitStamped(item);
        }
        return sender.tryEmitFromSource(item);
    }

    /**
     * Runs the inline-fused chain synchronously on the producer thread. Any user-code failure
     * is unwrapped to (a) the failed stage name (the actual stage that threw, recovered from
     * the internal {@link StageWorker.FusedStageException} attribution), and (b) the user's
     * original cause. The graph is then transitioned to {@code FAILED} via
     * {@link RuntimeCoordinator#fail(String, Throwable)} (the same fail-stop semantics as
     * {@link com.lattice.stage.StageExceptionHandler#failGraph()} on the worker path), and
     * the cause is rethrown to the caller as a public {@link GraphRuntimeException}. The
     * private {@code FusedStageException} type is never leaked through {@link Emitter#emit}.
     *
     * <p>Inline-fused stages do not currently invoke a user-supplied
     * {@link com.lattice.stage.StageExceptionHandler}; the producer-thread context makes
     * stage-poisoning ill-defined (the producer thread cannot continue running after the
     * stage it just executed has been poisoned). The effective policy is fail-stop, which
     * matches the runtime's default exception handler.
     */
    private void pushInline(final Output<Object> inline, final T item) {
        // Bracket the inline chain with a per-source progress counter so that
        // {@code RuntimeCoordinator.hasInFlightWork()} (and therefore {@code quiesce})
        // observes the producer thread as busy while it executes user stage logic. Single
        // producer means no atomic RMW is needed: a relaxed plain read + setRelease write
        // is sufficient (well under one nanosecond on x86 for both, vs ~6-10ns for a
        // LOCK XADD pair). The release write happens-before any acquire reader on the
        // quiesce path. {@link #pendingInline()} reads the counter via {@code getAcquire}.
        final long depth = (long) INLINE_DEPTH.get(this);
        INLINE_DEPTH.setRelease(this, depth + 1L);
        try {
            inline.push(item);
        } catch (final StageWorker.FusedStageException fused) {
            // Attribute to the actual failing stage; never expose the internal type.
            final Throwable cause = fused.getCause() != null ? fused.getCause() : fused;
            coordinator.fail(fused.stageName(), cause);
            throw publicEmitFailure(fused.stageName(), cause);
        } catch (final GraphRuntimeException ex) {
            // Already a public exception (e.g. ensureOpenAndRunning, edge-closed). Pass through.
            coordinator.fail(name, ex);
            throw ex;
        } catch (final RuntimeException ex) {
            // Untranslated user exception (e.g. owner stage threw a RuntimeException not yet
            // wrapped). Attribute to this source's name -- best available -- and surface as
            // a public {@link GraphRuntimeException} so callers do not have to depend on the
            // user-thrown type's stability.
            coordinator.fail(name, ex);
            throw publicEmitFailure(name, ex);
        } catch (final Error ex) {
            coordinator.fail(name, ex);
            throw ex;
        } catch (final Throwable ex) {
            coordinator.fail(name, ex);
            throw publicEmitFailure(name, ex);
        } finally {
            // Decrement back to the pre-emit depth even on exception. Single-writer source,
            // so we can re-read plainly and write release.
            INLINE_DEPTH.setRelease(this, depth);
        }
        if (StageMetrics.hotCountersEnabled()) {
            metrics.recordEmit();
        }
    }

    /**
     * Returns the number of inline emits currently in progress on this source's producer
     * thread, observed with acquire semantics from any thread. Used by
     * {@link RuntimeCoordinator#hasInFlightWork()} to keep {@code quiesce} honest while a
     * producer is mid-stage on the inline-fused path.
     */
    long pendingInline() {
        return (long) INLINE_DEPTH.getAcquire(this);
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
        sender.emitFromSource(item);
    }

    void emitPreallocatedTrusted(final T item) {
        ensureOpenAndRunning();
        sender.emitTrustedFromSource(item);
    }

    boolean emitPlain(final T item, final Duration timeout) {
        ensureOpenAndRunning();
        return sender.emit(item, timeout.toNanos());
    }

    boolean tryEmitPlain(final T item) {
        if (closed || graphState.get() != GraphState.RUNNING) {
            return false;
        }
        return sender.tryEmitFromSource(item);
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        sender.close();
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
        sender.emit(Stamped.of(nextStamp, item));
        nextStamp++;
    }

    private synchronized boolean emitStamped(final T item, final long timeoutNanos) {
        return emitStampedSingleProducer(item, timeoutNanos);
    }

    private boolean emitStampedSingleProducer(final T item, final long timeoutNanos) {
        if (!sender.emit(Stamped.of(nextStamp, item), timeoutNanos)) {
            return false;
        }
        nextStamp++;
        return true;
    }

    private synchronized boolean tryEmitStamped(final T item) {
        return tryEmitStampedSingleProducer(item);
    }

    private boolean tryEmitStampedSingleProducer(final T item) {
        if (!sender.tryEmit(Stamped.of(nextStamp, item))) {
            return false;
        }
        nextStamp++;
        return true;
    }
}
