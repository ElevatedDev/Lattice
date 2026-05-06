package io.github.elevateddev.lattice.metrics;

import io.github.elevateddev.lattice.graph.MetricsSpec;
import org.HdrHistogram.Histogram;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Live metrics for one stage worker.
 * <p>
 * Hot-path counters and histograms are graph-level opt-ins; histogram accessors
 * return defensive copies.
 * Runtime update methods such as {@code recordConsume()} are exposed for the
 * runtime implementation and should not be called by applications.
 */
public final class StageMetrics implements WaitMetrics {

    private final String name;
    private final boolean hotCountersEnabled;
    private final boolean histogramsEnabled;
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
    private final Histogram batchSizeHistogram;
    private final Histogram serviceTimeNanosHistogram;
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
        this(name, MetricsSpec.off());
    }

    /**
     * Creates metrics for a named stage.
     */
    public StageMetrics(final String name, final MetricsSpec metricsSpec) {
        this.name = name;
        this.hotCountersEnabled = metricsSpec.hotCounters();
        this.histogramsEnabled = metricsSpec.stageHistograms();
        this.batchSizeHistogram = histogramsEnabled ? new Histogram(1, 1_000_000, 3) : null;
        this.serviceTimeNanosHistogram = histogramsEnabled
            ? new Histogram(1, 60_000_000_000L, 3)
            : null;
    }

    /**
     * Returns the source-compatible default hot-counter state.
     */
    public static boolean hotCountersEnabled() {
        return false;
    }

    /**
     * Returns whether hot-path counters are enabled for this stage.
     */
    public boolean hotCounters() {
        return hotCountersEnabled;
    }

    /**
     * Returns the stage name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns messages emitted by this stage when hot counters are enabled.
     */
    public long emittedCount() {
        return emittedCount.sum();
    }

    /**
     * Returns messages consumed by this stage when hot counters are enabled.
     */
    public long consumedCount() {
        return consumedCount.sum();
    }

    /**
     * Returns exceptions thrown by this stage's user logic.
     */
    public long stageExceptions() {
        return stageExceptions.sum();
    }

    /**
     * Returns failed downstream output attempts.
     */
    public long failedOutputs() {
        return failedOutputs.sum();
    }

    /**
     * Returns downstream output attempts that had to wait.
     */
    public long blockedOutputs() {
        return blockedOutputs.sum();
    }

    /**
     * Returns total nanoseconds spent blocked on downstream output.
     */
    public long blockedNanos() {
        return blockedNanos.sum();
    }

    /**
     * Returns wait-loop spin count when hot counters are enabled.
     */
    public long spinCount() {
        return spinCount.sum();
    }

    /**
     * Returns wait-loop yield count when hot counters are enabled.
     */
    public long yieldCount() {
        return yieldCount.sum();
    }

    /**
     * Returns wait-loop park count when hot counters are enabled.
     */
    public long parkCount() {
        return parkCount.sum();
    }

    /**
     * Returns processed batch count.
     */
    public long batchesProcessed() {
        return batchesProcessed.sum();
    }

    /**
     * Returns processed message count across single-message and batch stages.
     */
    public long processedMessages() {
        return processedMessages.sum();
    }

    /**
     * Returns observed CPU affinity violations.
     */
    public long affinityViolations() {
        return affinityViolations.sum();
    }

    /**
     * Returns observed NUMA placement violations.
     */
    public long numaViolations() {
        return numaViolations.sum();
    }

    /**
     * Returns routing decisions made by dispatch, partition, or broadcast nodes.
     */
    public long routingDecisions() {
        return routingDecisions.sum();
    }

    /**
     * Returns branch-isolation actions taken for slow broadcast branches.
     */
    public long branchIsolationActions() {
        return branchIsolationActions.sum();
    }

    /**
     * Returns currently opened join groups as a monotonic counter input.
     */
    public long openJoinGroups() {
        return openJoinGroups.sum();
    }

    /**
     * Returns completed join groups.
     */
    public long completedJoinGroups() {
        return completedJoinGroups.sum();
    }

    /**
     * Returns join groups emitted or discarded after timeout.
     */
    public long timedOutJoinGroups() {
        return timedOutJoinGroups.sum();
    }

    /**
     * Returns duplicate join stamps observed by duplicate policy.
     */
    public long duplicateJoinStamps() {
        return duplicateJoinStamps.sum();
    }

    /**
     * Returns missing join branch observations.
     */
    public long missingJoinBranches() {
        return missingJoinBranches.sum();
    }

    /**
     * Returns slab handles retained by this stage.
     */
    public long retainedHandles() {
        return retainedHandles.sum();
    }

    /**
     * Returns slab handles released by this stage.
     */
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
     * Returns whether stage histograms are enabled by default.
     */
    public static boolean histogramsEnabled() {
        return false;
    }

    /**
     * Returns whether stage histograms are enabled for this stage.
     */
    public boolean histograms() {
        return histogramsEnabled;
    }

    /**
     * Returns the worker lifecycle state.
     */
    public WorkerState workerState() {
        return workerState;
    }

    /**
     * Returns the latest placement status for this worker.
     */
    public PlacementStatus placementStatus() {
        return placementStatus;
    }

    /**
     * Returns placement diagnostics for this worker.
     */
    public String placementMessage() {
        return placementMessage;
    }

    /**
     * Returns requested CPU id, or {@code -1} when not requested.
     */
    public int expectedCpu() {
        return expectedCpu;
    }

    /**
     * Returns observed CPU id, or {@code -1} when unavailable.
     */
    public int observedCpu() {
        return observedCpu;
    }

    /**
     * Returns requested NUMA node, or {@code -1} when not requested.
     */
    public int expectedNumaNode() {
        return expectedNumaNode;
    }

    /**
     * Returns observed NUMA node, or {@code -1} when unavailable.
     */
    public int observedNumaNode() {
        return observedNumaNode;
    }

    /**
     * Returns allocation owner metadata associated with this worker.
     */
    public String allocationOwner() {
        return allocationOwner;
    }

    /**
     * Returns stage start time, or {@code null} before start.
     */
    public Instant startTime() {
        return startTime.get();
    }

    /**
     * Returns stage stop time, or {@code null} until stopped.
     */
    public Instant stopTime() {
        return stopTime.get();
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

    public void recordException() {
        if (!hotCountersEnabled) {
            return;
        }
        stageExceptions.increment();
    }

    public void recordFailedOutput() {
        if (!hotCountersEnabled) {
            return;
        }
        failedOutputs.increment();
    }

    public void recordFailedOutputs(final long count) {
        if (!hotCountersEnabled) {
            return;
        }
        if (count > 0L) {
            failedOutputs.add(count);
        }
    }

    public void recordBlockedOutput() {
        if (!hotCountersEnabled) {
            return;
        }
        blockedOutputs.increment();
    }

    public void recordBlockedNanos(final long nanos) {
        if (!hotCountersEnabled) {
            return;
        }
        if (nanos > 0L) {
            blockedNanos.add(nanos);
        }
    }

    @Override
    public void recordSpin() {
        if (!hotCountersEnabled) {
            return;
        }
        spinCount.increment();
    }

    @Override
    public void recordYield() {
        if (!hotCountersEnabled) {
            return;
        }
        yieldCount.increment();
    }

    @Override
    public void recordPark() {
        if (!hotCountersEnabled) {
            return;
        }
        parkCount.increment();
    }

    public void recordBatch(final int size, final long serviceTimeNanos) {
        if (hotCountersEnabled) {
            batchesProcessed.increment();
            processedMessages.add(size);
        }
        if (batchSizeHistogram != null && size > 0) {
            batchSizeHistogram.recordValue(clampToHistogram(batchSizeHistogram, size));
        }
        if (serviceTimeNanosHistogram != null && serviceTimeNanos > 0L) {
            serviceTimeNanosHistogram.recordValue(clampToHistogram(serviceTimeNanosHistogram, serviceTimeNanos));
        }
    }

    public void recordRoutingDecision() {
        if (!hotCountersEnabled) {
            return;
        }
        routingDecisions.increment();
    }

    public void recordBranchIsolationAction() {
        if (!hotCountersEnabled) {
            return;
        }
        branchIsolationActions.increment();
    }

    public void recordOpenJoinGroup() {
        if (!hotCountersEnabled) {
            return;
        }
        openJoinGroups.increment();
    }

    public void recordCompletedJoinGroup() {
        if (!hotCountersEnabled) {
            return;
        }
        completedJoinGroups.increment();
    }

    public void recordTimedOutJoinGroup() {
        if (!hotCountersEnabled) {
            return;
        }
        timedOutJoinGroups.increment();
    }

    public void recordDuplicateJoinStamp() {
        if (!hotCountersEnabled) {
            return;
        }
        duplicateJoinStamps.increment();
    }

    public void recordMissingJoinBranch() {
        if (!hotCountersEnabled) {
            return;
        }
        missingJoinBranches.increment();
    }

    public void recordRetainedHandle() {
        if (!hotCountersEnabled) {
            return;
        }
        retainedHandles.increment();
    }

    public void recordReleasedHandle() {
        if (!hotCountersEnabled) {
            return;
        }
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
        if (hotCountersEnabled && affinityViolation) {
            affinityViolations.increment();
        }
        if (hotCountersEnabled && numaViolation) {
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
