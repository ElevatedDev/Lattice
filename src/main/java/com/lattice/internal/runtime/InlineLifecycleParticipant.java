package com.lattice.internal.runtime;

import java.util.concurrent.atomic.AtomicBoolean;

final class InlineLifecycleParticipant {

    private final StageWorker worker;
    private final SourceEmitter<?> source;
    private final RuntimeCoordinator coordinator;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    InlineLifecycleParticipant(
            final StageWorker worker,
            final SourceEmitter<?> source,
            final RuntimeCoordinator coordinator
    ) {
        this.worker = worker;
        this.source = source;
        this.coordinator = coordinator;
    }

    void markBootstrapped() {
        coordinator.workerBootstrapped();
    }

    void markRunning() {
        if (stopped.get()) {
            return;
        }
        if (running.compareAndSet(false, true)) {
            if (stopped.get()) {
                return;
            }
            worker.startInlineLifecycle();
            tryStop();
        }
    }

    void sourceClosed() {
        closed.set(true);
        tryStop();
    }

    void afterInlineExit() {
        tryStop();
    }

    private void tryStop() {
        if (!closed.get() || source.pendingInline() > 0L) {
            return;
        }
        if (stopped.compareAndSet(false, true)) {
            if (running.get()) {
                worker.stopInlineLifecycle();
            } else {
                coordinator.workerStopped();
            }
        }
    }
}
