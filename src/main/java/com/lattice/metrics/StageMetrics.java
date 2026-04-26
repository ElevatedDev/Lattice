package com.lattice.metrics;

import org.HdrHistogram.Histogram;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Live metrics for one stage worker.
 * <p>
 * Hot-path counters can be disabled with {@code lattice.metrics.hotCounters}.
 * Batch and service-time histograms are opt-in through
 * {@code lattice.metrics.stageHistograms}; histogram accessors return defensive
 * copies.
 */
public final class StageMetrics implements WaitMetrics {

    private static final boolean HISTOGRAMS_ENABLED = Boolean.getBoolean("lattice.metrics.stageHistograms");
    private static final boolean HOT_COUNTERS_ENABLED = Boolean.parseBoolean(
        System.getProperty("lattice.metrics.hotCounters", "true")
    );

    private final String name;
    private final LongAdder emittedCount = new LongAdder();
    private final LongAdder consumedCount = new LongAdder();
    private final LongAdder stageExceptions = new LongAdder();
    private final LongAdder failedOutputs = new LongAdder();
    private final LongAdder blockedOutputs = new LongAdder();
    private final LongAdder blockedNanos = new LongAdder();
    private final LongAdder spinCount = new LongAdder();
    private final LongAdder yieldCount = new LongAdder();
    private final LongAdder parkCount = new LongAdder();
    private final LongAdder batchesProcessed = new LongAdder();
    private final LongAdder processedMessages = new LongAdder();
    private final LongAdder affinityViolations = new LongAdder();
    private final LongAdder numaViolations = new LongAdder();
    private final LongAdder routingDecisions = new LongAdder();
    private final LongAdder branchIsolationActions = new LongAdder();
    private final LongAdder openJoinGroups = new LongAdder();
    private final LongAdder completedJoinGroups = new LongAdder();
    private final LongAdder timedOutJoinGroups = new LongAdder();
    private final LongAdder duplicateJoinStamps = new LongAdder();
    private final LongAdder missingJoinBranches = new LongAdder();
    private final LongAdder retainedHandles = new LongAdder();
    private final LongAdder releasedHandles = new LongAdder();
    private final Histogram batchSizeHistogram = HISTOGRAMS_ENABLED ? new Histogram(1, 1_000_000, 3) : null;
    private final Histogram serviceTimeNanosHistogram = HISTOGRAMS_ENABLED
        ? new Histogram(1, 60_000_000_000L, 3)
        : null;
    private volatile WorkerState workerState = WorkerState.NEW;
    private volatile PlacementStatus placementStatus = PlacementStatus.NOT_REQUESTED;
    private volatile String placementMessage = "";
    private volatile int expectedCpu = -1;
    private volatile int observedCpu = -1;
    private volatile int expectedNumaNode = -1;
    private volatile int observedNumaNode = -1;
    private volatile String allocationOwner = "";
    private final AtomicReference<Instant> startTime = new AtomicReference<>();
    private final AtomicReference<Instant> stopTime = new AtomicReference<>();
    private final AtomicLong startNanos = new AtomicLong();
    private final AtomicLong stopNanos = new AtomicLong();

    /**
     * Creates metrics for a named stage.
     */
    public StageMetrics(final String name) {
        this.name = name;
    }

    /**
     * Returns whether hot-path counters are enabled for this JVM.
     */
    public static boolean hotCountersEnabled() {
        return HOT_COUNTERS_ENABLED;
    }

    /**
     * Returns the stage name.
     */
    public String name() {
        return name;
    }

    public long emittedCount() {
        return emittedCount.sum();
    }

    public long consumedCount() {
        return consumedCount.sum();
    }

    public long stageExceptions() {
        return stageExceptions.sum();
    }

    public long failedOutputs() {
        return failedOutputs.sum();
    }

    public long blockedOutputs() {
        return blockedOutputs.sum();
    }

    public long blockedNanos() {
        return blockedNanos.sum();
    }

    public long spinCount() {
        return spinCount.sum();
    }

    public long yieldCount() {
        return yieldCount.sum();
    }

    public long parkCount() {
        return parkCount.sum();
    }

    public long batchesProcessed() {
        return batchesProcessed.sum();
    }

    public long processedMessages() {
        return processedMessages.sum();
    }

    public long affinityViolations() {
        return affinityViolations.sum();
    }

    public long numaViolations() {
        return numaViolations.sum();
    }

    public long routingDecisions() {
        return routingDecisions.sum();
    }

    public long branchIsolationActions() {
        return branchIsolationActions.sum();
    }

    public long openJoinGroups() {
        return openJoinGroups.sum();
    }

    public long completedJoinGroups() {
        return completedJoinGroups.sum();
    }

    public long timedOutJoinGroups() {
        return timedOutJoinGroups.sum();
    }

    public long duplicateJoinStamps() {
        return duplicateJoinStamps.sum();
    }

    public long missingJoinBranches() {
        return missingJoinBranches.sum();
    }

    public long retainedHandles() {
        return retainedHandles.sum();
    }

    public long releasedHandles() {
        return releasedHandles.sum();
    }

    /**
     * Returns approximate processed-message throughput since start.
     */
    public double processRatePerSecond() {
        final Instant started = startTime.get();
        if (started == null) {
            return 0.0d;
        }
        final long stoppedNanos = stopNanos.get();
        final long end = stoppedNanos == 0L ? System.nanoTime() : stoppedNanos;
        final long start = startNanos.get();
        final long elapsed = Math.max(1L, end - start);
        return processedMessages.sum() * 1_000_000_000.0d / elapsed;
    }

    /**
     * Returns a copy of batch-size histogram data.
     */
    public Histogram batchSizeHistogram() {
        return batchSizeHistogram == null ? new Histogram(1, 1_000_000, 3) : batchSizeHistogram.copy();
    }

    /**
     * Returns a copy of service-time histogram data.
     */
    public Histogram serviceTimeNanosHistogram() {
        return serviceTimeNanosHistogram == null
            ? new Histogram(1, 60_000_000_000L, 3)
            : serviceTimeNanosHistogram.copy();
    }

    /**
     * Returns whether stage histograms are enabled for this JVM.
     */
    public static boolean histogramsEnabled() {
        return HISTOGRAMS_ENABLED;
    }

    public WorkerState workerState() {
        return workerState;
    }

    public PlacementStatus placementStatus() {
        return placementStatus;
    }

    public String placementMessage() {
        return placementMessage;
    }

    public int expectedCpu() {
        return expectedCpu;
    }

    public int observedCpu() {
        return observedCpu;
    }

    public int expectedNumaNode() {
        return expectedNumaNode;
    }

    public int observedNumaNode() {
        return observedNumaNode;
    }

    public String allocationOwner() {
        return allocationOwner;
    }

    public Instant startTime() {
        return startTime.get();
    }

    public Instant stopTime() {
        return stopTime.get();
    }

    public void recordEmit() {
        if (!HOT_COUNTERS_ENABLED) {
            return;
        }
        emittedCount.increment();
    }

    public void recordConsume() {
        if (!HOT_COUNTERS_ENABLED) {
            return;
        }
        consumedCount.increment();
    }

    public void recordConsume(final int count) {
        if (!HOT_COUNTERS_ENABLED) {
            return;
        }
        if (count > 0) {
            consumedCount.add(count);
        }
    }

    public void recordException() {
        stageExceptions.increment();
    }

    public void recordFailedOutput() {
        failedOutputs.increment();
    }

    public void recordBlockedOutput() {
        blockedOutputs.increment();
    }

    public void recordBlockedNanos(final long nanos) {
        if (nanos > 0L) {
            blockedNanos.add(nanos);
        }
    }

    @Override
    public void recordSpin() {
        if (!HOT_COUNTERS_ENABLED) {
            return;
        }
        spinCount.increment();
    }

    @Override
    public void recordYield() {
        if (!HOT_COUNTERS_ENABLED) {
            return;
        }
        yieldCount.increment();
    }

    @Override
    public void recordPark() {
        if (!HOT_COUNTERS_ENABLED) {
            return;
        }
        parkCount.increment();
    }

    public void recordBatch(final int size, final long serviceTimeNanos) {
        if (!HOT_COUNTERS_ENABLED) {
            return;
        }
        batchesProcessed.increment();
        processedMessages.add(size);
        if (batchSizeHistogram != null && size > 0) {
            batchSizeHistogram.recordValue(clampToHistogram(batchSizeHistogram, size));
        }
        if (serviceTimeNanosHistogram != null && serviceTimeNanos > 0L) {
            serviceTimeNanosHistogram.recordValue(clampToHistogram(serviceTimeNanosHistogram, serviceTimeNanos));
        }
    }

    public void recordRoutingDecision() {
        routingDecisions.increment();
    }

    public void recordBranchIsolationAction() {
        branchIsolationActions.increment();
    }

    public void recordOpenJoinGroup() {
        openJoinGroups.increment();
    }

    public void recordCompletedJoinGroup() {
        completedJoinGroups.increment();
    }

    public void recordTimedOutJoinGroup() {
        timedOutJoinGroups.increment();
    }

    public void recordDuplicateJoinStamp() {
        duplicateJoinStamps.increment();
    }

    public void recordMissingJoinBranch() {
        missingJoinBranches.increment();
    }

    public void recordRetainedHandle() {
        retainedHandles.increment();
    }

    public void recordReleasedHandle() {
        releasedHandles.increment();
    }

    public void workerState(final WorkerState state) {
        workerState = state;
    }

    public void recordPlacement(
        final PlacementStatus status,
        final String message,
        final int expectedCpu,
        final int observedCpu,
        final int expectedNumaNode,
        final int observedNumaNode,
        final String allocationOwner,
        final boolean affinityViolation,
        final boolean numaViolation
    ) {
        this.placementStatus = status;
        this.placementMessage = message == null ? "" : message;
        this.expectedCpu = expectedCpu;
        this.observedCpu = observedCpu;
        this.expectedNumaNode = expectedNumaNode;
        this.observedNumaNode = observedNumaNode;
        this.allocationOwner = allocationOwner == null ? "" : allocationOwner;
        if (affinityViolation) {
            affinityViolations.increment();
        }
        if (numaViolation) {
            numaViolations.increment();
        }
    }

    public void markStarted() {
        if (startTime.compareAndSet(null, Instant.now())) {
            startNanos.compareAndSet(0L, System.nanoTime());
        }
    }

    public void markStopped() {
        if (stopTime.compareAndSet(null, Instant.now())) {
            stopNanos.compareAndSet(0L, System.nanoTime());
        }
    }

    private static long clampToHistogram(final Histogram histogram, final long value) {
        return Math.min(histogram.getHighestTrackableValue(), Math.max(1L, value));
    }
}
