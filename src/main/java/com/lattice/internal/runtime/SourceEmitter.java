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
    private volatile boolean closed;
    private long nextStamp;

    private Output<Object> inlineOutput;

    SourceEmitter(
        final String name,
        final EdgeSender sender,
        final StageMetrics metrics,
        final AtomicReference<GraphState> graphState,
        final boolean stampItems,
        final SourceMode sourceMode
    ) {
        this.name = name;
        this.sender = sender;
        this.metrics = metrics;
        this.graphState = graphState;
        this.stampItems = stampItems;
        this.singleProducer = sourceMode == SourceMode.SINGLE_PRODUCER;
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
            // Inline source-side fusion: execute the entire downstream fused chain on the
            // producer thread, bypassing the SPSC ring. Backpressure is impossible because
            // the chain is fully synchronous; eligibility (single producer, no preallocation,
            // no stamping, no handle-bearing types) is enforced at graph build time.
            inline.push(item);
            if (StageMetrics.hotCountersEnabled()) {
                metrics.recordEmit();
            }
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
        if (stampItems) {
            return singleProducer ? tryEmitStampedSingleProducer(item) : tryEmitStamped(item);
        }
        return sender.tryEmitFromSource(item);
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
