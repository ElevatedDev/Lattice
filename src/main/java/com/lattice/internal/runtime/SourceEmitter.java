package com.lattice.internal.runtime;

import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.GraphState;
import com.lattice.graph.SourceMode;
import com.lattice.metrics.StageMetrics;
import com.lattice.metrics.WorkerState;
import com.lattice.routing.Stamped;
import com.lattice.stage.Emitter;
import com.lattice.stage.Output;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

final class SourceEmitter<T> implements Emitter<T> {

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
     * is routed through {@link RuntimeCoordinator#fail(String, Throwable)} so the graph
     * transitions to FAILED with the same semantics as a worker-thread failure, then the
     * exception is rethrown to the caller as a {@link GraphRuntimeException} so the
     * application thread does not silently continue past a failed stage.
     */
    private void pushInline(final Output<Object> inline, final T item) {
        try {
            inline.push(item);
        } catch (final Throwable ex) {
            coordinator.fail(name, ex);

            if (ex instanceof RuntimeException re) {
                throw re;
            }
            if (ex instanceof Error err) {
                throw err;
            }
            throw new GraphRuntimeException("inline-fused source emit failed: " + name, ex);
        }
        if (StageMetrics.hotCountersEnabled()) {
            metrics.recordEmit();
        }
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
