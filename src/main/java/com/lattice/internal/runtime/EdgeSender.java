package com.lattice.internal.runtime;

import com.lattice.edge.BackpressureException;
import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.GraphRuntimeException;
import com.lattice.internal.jfr.JfrEvents;
import com.lattice.internal.edge.MessageEdge;
import com.lattice.internal.wait.WaitStrategies;
import com.lattice.internal.wait.WaitStrategy;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.metrics.WaitMetrics;
import com.lattice.routing.Stamped;
import com.lattice.slab.SlabHandle;

final class EdgeSender {

    private final String ownerName;
    private final Class<?> messageType;
    private final String messageTypeName;
    private final String nullItemMessage;
    private final boolean acceptsAnyType;
    private final MessageEdge edge;
    private final EdgeMetrics edgeMetrics;
    private final GraphMetrics graphMetrics;
    private final StageMetrics ownerMetrics;
    private final RuntimeCoordinator coordinator;
    private final WaitStrategy waitStrategy;
    private final WaitMetrics waitMetrics;
    private final OverflowPolicy overflowPolicy;
    private final OverflowPolicy.OverflowKind overflowKind;
    private final long policyTimeoutNanos;
    private EdgeSender redirectSender;
    private final String graphName;
    private final String edgeFrom;
    private final String edgeTo;
    private final String edgeName;
    private final boolean jfrEnabled;

    EdgeSender(
        final String ownerName,
        final Class<?> messageType,
        final MessageEdge edge,
        final EdgeSpec spec,
        final StageMetrics ownerMetrics,
        final RuntimeCoordinator coordinator
    ) {
        this.ownerName = ownerName;
        this.messageType = messageType;
        this.messageTypeName = messageType.getName();
        this.nullItemMessage = ownerName + " cannot emit null";
        this.acceptsAnyType = messageType == Object.class;
        this.edge = edge;
        this.edgeMetrics = edge.metrics();
        this.graphMetrics = coordinator.metrics();
        this.ownerMetrics = ownerMetrics;
        this.coordinator = coordinator;
        this.waitStrategy = WaitStrategies.from(spec.waitSpec());
        this.waitMetrics = new CombinedWaitMetrics(ownerMetrics, edgeMetrics);
        this.overflowPolicy = spec.overflowPolicy();
        this.overflowKind = overflowPolicy.kind();
        this.policyTimeoutNanos = overflowKind == OverflowPolicy.OverflowKind.BLOCK_FOR
            ? spec.overflowPolicy().timeout().toNanos()
            : 0L;

        this.graphName = coordinator.graphName();
        this.edgeFrom = edge.from();
        this.edgeTo = edge.to();
        this.edgeName = edgeFrom + "->" + edgeTo;
        this.jfrEnabled = JfrEvents.enabled();
    }

    void redirectSender(final EdgeSender redirectSender) {
        this.redirectSender = redirectSender;
    }

    void emit(final Object item) {
        final Object outbound = HandleOwnership.prepareForEnqueue(item);
        validateItem(outbound);
        boolean handled = false;
        try {
            emitPrepared(outbound);
            handled = true;
        } finally {
            if (!handled && outbound != item) {
                releaseIfHandle(outbound);
            }
        }
    }

    private void emitPrepared(final Object item) {
        if (overflowKind == OverflowPolicy.OverflowKind.FAIL_FAST) {
            if (!tryEmitValidated(item)) {
                throw new BackpressureException("edge is full: " + edgeName);
            }
            return;
        }
        if (emitLossy(item)) {
            return;
        }
        if (!emitValidated(item, policyTimeoutNanos, policyTimeoutNanos > 0L)) {
            throw new BackpressureException("timed out offering to edge: " + edgeName);
        }
    }

    boolean emit(final Object item, final long timeoutNanos) {
        final Object outbound = HandleOwnership.prepareForEnqueue(item);
        validateItem(outbound);
        boolean handled = false;
        try {
            handled = emitPrepared(outbound, timeoutNanos);
            return handled;
        } finally {
            if (!handled && outbound != item) {
                releaseIfHandle(outbound);
            }
        }
    }

    private boolean emitPrepared(final Object item, final long timeoutNanos) {
        if (overflowKind == OverflowPolicy.OverflowKind.FAIL_FAST) {
            if (!tryEmitValidated(item)) {
                throw new BackpressureException("edge is full: " + edgeName);
            }
            return true;
        }
        if (emitLossy(item)) {
            return true;
        }
        return emitValidated(item, Math.max(0L, timeoutNanos), true);
    }

    private boolean emitValidated(final Object item, final long timeoutNanos, final boolean timed) {
        int idle = 0;
        boolean blockedRecorded = false;
        boolean blockedDurationRecorded = false;
        long blockedStart = 0L;
        final long deadline = timed ? System.nanoTime() + timeoutNanos : Long.MAX_VALUE;

        try {
            while (true) {
                if (coordinator.isAbortRequested()) {
                    throw new GraphRuntimeException("graph is stopping or failed: " + graphName);
                }
                if (edge.offer(item)) {
                    if (blockedRecorded) {
                        recordBackpressureDuration(System.nanoTime() - blockedStart);
                        blockedDurationRecorded = true;
                    }
                    graphMetrics.clearOverload();
                    ownerMetrics.recordEmit();
                    return true;
                }
                if (edge.isClosed()) {
                    throw new GraphRuntimeException("edge is closed: " + edgeName);
                }
                edgeMetrics.recordFailedOffer();
                graphMetrics.recordFailedOffer();
                ownerMetrics.recordFailedOutput();
                if (!blockedRecorded) {
                    blockedStart = System.nanoTime();
                    edgeMetrics.recordBlockedOffer();
                    graphMetrics.recordBlockedOffer();
                    graphMetrics.activateOverload();
                    ownerMetrics.recordBlockedOutput();

                    if (jfrEnabled) {
                        JfrEvents.edgeBackpressure(graphName, edgeFrom, edgeTo);
                    }

                    blockedRecorded = true;
                }
                if (timed) {
                    final long now = System.nanoTime();
                    if (now >= deadline) {
                        recordBackpressureDuration(now - blockedStart);
                        blockedDurationRecorded = true;
                        return false;
                    }
                }
                idle = waitStrategy.idle(idle, waitMetrics);
            }
        } finally {
            if (blockedRecorded && !blockedDurationRecorded) {
                recordBackpressureDuration(System.nanoTime() - blockedStart);
            }
        }
    }

    boolean tryEmit(final Object item) {
        final Object outbound = HandleOwnership.prepareForEnqueue(item);
        validateItem(outbound);
        boolean handled = false;
        try {
            if (emitLossy(outbound)) {
                handled = true;
                return true;
            }
            handled = tryEmitValidated(outbound);
            return handled;
        } finally {
            if (!handled && outbound != item) {
                releaseIfHandle(outbound);
            }
        }
    }

    private boolean tryEmitValidated(final Object item) {
        if (coordinator.isAbortRequested() || edge.isClosed()) {
            return false;
        }
        final boolean offered = edge.offer(item);
        if (offered) {
            graphMetrics.clearOverload();
            ownerMetrics.recordEmit();
        } else if (!edge.isClosed()) {
            edgeMetrics.recordFailedOffer();
            graphMetrics.recordFailedOffer();
            ownerMetrics.recordFailedOutput();
        }
        return offered;
    }

    void close() {
        edge.close();
        final EdgeSender redirect = redirectSender;
        if (redirect != null) {
            redirect.edge.close();
        }
    }

    EdgeMetrics edgeMetrics() {
        return edgeMetrics;
    }

    private boolean emitLossy(final Object item) {
        return switch (overflowKind) {
            case DROP_LATEST -> {
                if (tryEmitValidated(item)) {
                    yield true;
                }
                edgeMetrics.recordDroppedLatest();
                graphMetrics.recordDroppedMessage();
                releaseIfHandle(item);
                yield true;
            }
            case DROP_OLDEST -> {
                if (tryEmitValidated(item)) {
                    yield true;
                }
                final Object dropped = edge.dropOldest();
                if (dropped != null) {
                    edgeMetrics.recordDroppedOldest();
                    graphMetrics.recordDroppedMessage();
                    releaseIfHandle(dropped);
                }
                if (tryEmitValidated(item)) {
                    yield true;
                }
                edgeMetrics.recordDroppedLatest();
                graphMetrics.recordDroppedMessage();
                releaseIfHandle(item);
                yield true;
            }
            case COALESCE -> {
                if (tryEmitValidated(item)) {
                    yield true;
                }
                if (edge.tryCoalesce(item, overflowPolicy.coalescingKey())) {
                    edgeMetrics.recordCoalescedOffer();
                    graphMetrics.recordCoalescedMessage();
                    yield true;
                }
                yield false;
            }
            case REDIRECT -> {
                if (tryEmitValidated(item)) {
                    yield true;
                }
                final EdgeSender redirect = redirectSender;
                if (redirect == null) {
                    yield false;
                }
                edgeMetrics.recordRedirectedOffer();
                graphMetrics.recordRedirectedMessage();
                redirect.emit(item);
                yield true;
            }
            default -> false;
        };
    }

    private static void releaseIfHandle(final Object item) {
        if (item instanceof SlabHandle<?> handle) {
            handle.release();
        } else if (item instanceof Stamped<?> stamped) {
            releaseIfHandle(stamped.value());
        }
    }

    private void validateItem(final Object item) {
        if (item == null) {
            throw new NullPointerException(nullItemMessage);
        }
        if (!acceptsAnyType && !messageType.isInstance(item)) {
            throw new ClassCastException(ownerName + " emitted " + item.getClass().getName()
                + ", expected " + messageTypeName);
        }
    }

    private void recordBackpressureDuration(final long nanos) {
        edgeMetrics.recordBackpressureNanos(nanos);
        graphMetrics.recordBackpressureNanos(nanos);
        ownerMetrics.recordBlockedNanos(nanos);
        if (jfrEnabled) {
            JfrEvents.edgeStall(graphName, edgeFrom, edgeTo, nanos);
        }
    }

    private record CombinedWaitMetrics(StageMetrics owner, EdgeMetrics edge) implements WaitMetrics {

        @Override
        public void recordSpin() {
            owner.recordSpin();
            edge.recordSpin();
        }

        @Override
        public void recordYield() {
            owner.recordYield();
            edge.recordYield();
        }

        @Override
        public void recordPark() {
            owner.recordPark();
            edge.recordPark();
        }
    }
}
