package io.github.elevateddev.lattice.metrics;

import io.github.elevateddev.lattice.graph.MetricsSpec;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Live metrics for one graph instance.
 * <p>
 * Counters are monotonic unless the method name describes state
 * ({@link #overloaded()}, {@link #startTime()}, {@link #stopTime()}). The stage
 * and edge maps are immutable views of the graph topology; the metric objects
 * inside those maps continue to update while the graph runs.
 * Runtime update methods such as {@code recordEmit()} are exposed for the
 * runtime implementation and should not be called by applications.
 */
public final class GraphMetrics {
    private final String graphName;
    private final boolean hotCountersEnabled;
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

    /**
     * Creates graph metrics from already-created stage and edge metrics.
     */
    public GraphMetrics(
        final String graphName,
        final Map<String, StageMetrics> stageMetrics,
        final Map<String, EdgeMetrics> edgeMetrics
    ) {
        this(graphName, stageMetrics, edgeMetrics, MetricsSpec.off());
    }

    /**
     * Creates graph metrics from already-created stage and edge metrics.
     */
    public GraphMetrics(
        final String graphName,
        final Map<String, StageMetrics> stageMetrics,
        final Map<String, EdgeMetrics> edgeMetrics,
        final MetricsSpec metricsSpec
    ) {
        this.graphName = graphName;
        this.hotCountersEnabled = metricsSpec.hotCounters();
        this.stageMetrics = Collections.unmodifiableMap(new LinkedHashMap<>(stageMetrics));
        this.edgeMetrics = Collections.unmodifiableMap(new LinkedHashMap<>(edgeMetrics));
    }

    /**
     * Returns the graph name.
     */
    public String graphName() {
        return graphName;
    }

    /**
     * Returns total accepted source emissions when hot counters are enabled.
     */
    public long emittedCount() {
        return emittedCount.sum();
    }

    /**
     * Returns total consumed graph messages when hot counters are enabled.
     */
    public long consumedCount() {
        return consumedCount.sum();
    }

    /**
     * Returns failed edge offers.
     */
    public long failedOffers() {
        return failedOffers.sum();
    }

    /**
     * Returns how many offers had to wait for capacity.
     */
    public long blockedOffers() {
        return blockedOffers.sum();
    }

    /**
     * Returns total nanoseconds spent under producer backpressure.
     */
    public long backpressureNanos() {
        return backpressureNanos.sum();
    }

    /**
     * Returns total stage exceptions observed by the graph.
     */
    public long stageExceptions() {
        return stageExceptions.sum();
    }

    /**
     * Returns whether any overload policy currently marks the graph overloaded.
     */
    public boolean overloaded() {
        return overloaded.get();
    }

    /**
     * Returns how often graph overload was activated.
     */
    public long overloadActivations() {
        return overloadActivations.sum();
    }

    /**
     * Returns messages dropped by drop-latest/drop-newest or drop-oldest
     * policies.
     */
    public long droppedMessages() {
        return droppedMessages.sum();
    }

    /**
     * Returns messages redirected by overflow policy.
     */
    public long redirectedMessages() {
        return redirectedMessages.sum();
    }

    /**
     * Returns offers coalesced by coalescing overflow policy.
     */
    public long coalescedMessages() {
        return coalescedMessages.sum();
    }

    /**
     * Returns graph start time, or {@code null} before start.
     */
    public Instant startTime() {
        return startTime.get();
    }

    /**
     * Returns graph stop time, or {@code null} until stopped.
     */
    public Instant stopTime() {
        return stopTime.get();
    }

    /**
     * Returns the immutable stage metrics map keyed by stage name.
     */
    public Map<String, StageMetrics> stages() {
        return stageMetrics;
    }

    /**
     * Returns whether hot-path counters are enabled for this graph.
     */
    public boolean hotCounters() {
        return hotCountersEnabled;
    }

    /**
     * Returns the immutable edge metrics map keyed as {@code from->to}.
     */
    public Map<String, EdgeMetrics> edges() {
        return edgeMetrics;
    }

    /**
     * Returns metrics for a named stage, or {@code null} if absent.
     */
    public StageMetrics stage(final String name) {
        return stageMetrics.get(name);
    }

    /**
     * Returns metrics for an edge, or {@code null} if absent.
     */
    public EdgeMetrics edge(final String from, final String to) {
        return edgeMetrics.get(from + "->" + to);
    }

    /**
     * Returns a snapshot-style placement report for all stages.
     */
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
        if (!hotCountersEnabled) {
            return;
        }
        emittedCount.increment();
    }

    public void recordConsume() {
        if (!hotCountersEnabled) {
            return;
        }
        consumedCount.increment();
    }

    public void recordConsume(final int count) {
        if (!hotCountersEnabled) {
            return;
        }
        if (count > 0) {
            consumedCount.add(count);
        }
    }

    public void recordFailedOffer() {
        if (!hotCountersEnabled) {
            return;
        }
        failedOffers.increment();
    }

    public void recordFailedOffers(final long count) {
        if (!hotCountersEnabled) {
            return;
        }
        if (count > 0L) {
            failedOffers.add(count);
        }
    }

    public void recordBlockedOffer() {
        if (!hotCountersEnabled) {
            return;
        }
        blockedOffers.increment();
    }

    public void recordBackpressureNanos(final long nanos) {
        if (!hotCountersEnabled) {
            return;
        }
        if (nanos > 0L) {
            backpressureNanos.add(nanos);
        }
    }

    public void activateOverload() {
        if (overloaded.compareAndSet(false, true)) {
            if (hotCountersEnabled) {
                overloadActivations.increment();
            }
        }
    }

    public void clearOverload() {
        if (overloaded.get()) {
            overloaded.set(false);
        }
    }

    public void recordStageException() {
        if (!hotCountersEnabled) {
            return;
        }
        stageExceptions.increment();
    }

    public void recordDroppedMessage() {
        if (!hotCountersEnabled) {
            return;
        }
        droppedMessages.increment();
    }

    public void recordRedirectedMessage() {
        if (!hotCountersEnabled) {
            return;
        }
        redirectedMessages.increment();
    }

    public void recordCoalescedMessage() {
        if (!hotCountersEnabled) {
            return;
        }
        coalescedMessages.increment();
    }

    public void markStarted() {
        startTime.compareAndSet(null, Instant.now());
    }

    public void markStopped() {
        stopTime.compareAndSet(null, Instant.now());
    }

    /**
     * Stage placement row used by {@link #placementReport()}.
     */
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
