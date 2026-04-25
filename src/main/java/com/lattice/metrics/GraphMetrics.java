package com.lattice.metrics;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

public final class GraphMetrics {
    private final String graphName;
    private final LongAdder emittedCount = new LongAdder();
    private final LongAdder consumedCount = new LongAdder();
    private final LongAdder failedOffers = new LongAdder();
    private final LongAdder blockedOffers = new LongAdder();
    private final LongAdder backpressureNanos = new LongAdder();
    private final LongAdder stageExceptions = new LongAdder();
    private final LongAdder overloadActivations = new LongAdder();
    private final LongAdder droppedMessages = new LongAdder();
    private final LongAdder redirectedMessages = new LongAdder();
    private final LongAdder coalescedMessages = new LongAdder();
    private final AtomicBoolean overloaded = new AtomicBoolean();
    private final AtomicReference<Instant> startTime = new AtomicReference<>();
    private final AtomicReference<Instant> stopTime = new AtomicReference<>();
    private final Map<String, StageMetrics> stageMetrics;
    private final Map<String, EdgeMetrics> edgeMetrics;

    public GraphMetrics(
        final String graphName,
        final Map<String, StageMetrics> stageMetrics,
        final Map<String, EdgeMetrics> edgeMetrics
    ) {
        this.graphName = graphName;
        this.stageMetrics = Collections.unmodifiableMap(new LinkedHashMap<>(stageMetrics));
        this.edgeMetrics = Collections.unmodifiableMap(new LinkedHashMap<>(edgeMetrics));
    }

    public String graphName() {
        return graphName;
    }

    public long emittedCount() {
        return emittedCount.sum();
    }

    public long consumedCount() {
        return consumedCount.sum();
    }

    public long failedOffers() {
        return failedOffers.sum();
    }

    public long blockedOffers() {
        return blockedOffers.sum();
    }

    public long backpressureNanos() {
        return backpressureNanos.sum();
    }

    public long stageExceptions() {
        return stageExceptions.sum();
    }

    public boolean overloaded() {
        return overloaded.get();
    }

    public long overloadActivations() {
        return overloadActivations.sum();
    }

    public long droppedMessages() {
        return droppedMessages.sum();
    }

    public long redirectedMessages() {
        return redirectedMessages.sum();
    }

    public long coalescedMessages() {
        return coalescedMessages.sum();
    }

    public Instant startTime() {
        return startTime.get();
    }

    public Instant stopTime() {
        return stopTime.get();
    }

    public Map<String, StageMetrics> stages() {
        return stageMetrics;
    }

    public Map<String, EdgeMetrics> edges() {
        return edgeMetrics;
    }

    public StageMetrics stage(final String name) {
        return stageMetrics.get(name);
    }

    public EdgeMetrics edge(final String from, final String to) {
        return edgeMetrics.get(from + "->" + to);
    }

    public List<StagePlacement> placementReport() {
        return stageMetrics.values().stream()
            .map(stage -> new StagePlacement(
                stage.name(),
                stage.placementStatus(),
                stage.placementMessage(),
                stage.expectedCpu(),
                stage.observedCpu(),
                stage.expectedNumaNode(),
                stage.observedNumaNode(),
                stage.affinityViolations(),
                stage.numaViolations()
            ))
            .toList();
    }

    public void recordEmit() {
        emittedCount.increment();
    }

    public void recordConsume() {
        consumedCount.increment();
    }

    public void recordConsume(final int count) {
        if (count > 0) {
            consumedCount.add(count);
        }
    }

    public void recordFailedOffer() {
        failedOffers.increment();
    }

    public void recordBlockedOffer() {
        blockedOffers.increment();
    }

    public void recordBackpressureNanos(final long nanos) {
        if (nanos > 0L) {
            backpressureNanos.add(nanos);
        }
    }

    public void activateOverload() {
        if (overloaded.compareAndSet(false, true)) {
            overloadActivations.increment();
        }
    }

    public void clearOverload() {
        overloaded.set(false);
    }

    public void recordStageException() {
        stageExceptions.increment();
    }

    public void recordDroppedMessage() {
        droppedMessages.increment();
    }

    public void recordRedirectedMessage() {
        redirectedMessages.increment();
    }

    public void recordCoalescedMessage() {
        coalescedMessages.increment();
    }

    public void markStarted() {
        startTime.compareAndSet(null, Instant.now());
    }

    public void markStopped() {
        stopTime.compareAndSet(null, Instant.now());
    }

    public record StagePlacement(
        String stageName,
        PlacementStatus status,
        String message,
        int expectedCpu,
        int observedCpu,
        int expectedNumaNode,
        int observedNumaNode,
        long affinityViolations,
        long numaViolations
    ) {
    }
}
