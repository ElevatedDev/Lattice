package com.lattice.internal.runtime;

import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.GraphState;
import com.lattice.metrics.StageMetrics;
import com.lattice.metrics.WorkerState;
import com.lattice.routing.Stamped;
import com.lattice.stage.Emitter;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

final class SourceEmitter<T> implements Emitter<T> {

    private final String name;
    private final EdgeSender sender;
    private final StageMetrics metrics;
    private final AtomicReference<GraphState> graphState;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final boolean stampItems;
    private long nextStamp;

    SourceEmitter(
        final String name,
        final EdgeSender sender,
        final StageMetrics metrics,
        final AtomicReference<GraphState> graphState,
        final boolean stampItems
    ) {
        this.name = name;
        this.sender = sender;
        this.metrics = metrics;
        this.graphState = graphState;
        this.stampItems = stampItems;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void emit(final T item) {
        ensureOpenAndRunning();
        if (stampItems) {
            emitStamped(item);
        } else {
            sender.emit(item);
        }
    }

    @Override
    public boolean emit(final T item, final Duration timeout) {
        ensureOpenAndRunning();
        if (stampItems) {
            return emitStamped(item, timeout.toNanos());
        }
        return sender.emit(item, timeout.toNanos());
    }

    @Override
    public boolean tryEmit(final T item) {
        if (closed.get() || graphState.get() != GraphState.RUNNING) {
            return false;
        }
        if (stampItems) {
            return tryEmitStamped(item);
        }
        return sender.tryEmit(item);
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            sender.close();
            metrics.workerState(WorkerState.STOPPED);
            metrics.markStopped();
        }
    }

    void markStarted() {
        metrics.markStarted();
        metrics.workerState(WorkerState.RUNNING);
    }

    private void ensureOpenAndRunning() {
        if (closed.get()) {
            throw new GraphRuntimeException("source is closed: " + name);
        }
        if (graphState.get() != GraphState.RUNNING) {
            throw new GraphRuntimeException("graph is not running; source " + name + " cannot emit");
        }
    }

    private synchronized void emitStamped(final T item) {
        sender.emit(Stamped.of(nextStamp, item));
        nextStamp++;
    }

    private synchronized boolean emitStamped(final T item, final long timeoutNanos) {
        if (!sender.emit(Stamped.of(nextStamp, item), timeoutNanos)) {
            return false;
        }
        nextStamp++;
        return true;
    }

    private synchronized boolean tryEmitStamped(final T item) {
        if (!sender.tryEmit(Stamped.of(nextStamp, item))) {
            return false;
        }
        nextStamp++;
        return true;
    }
}
