package com.lattice.metrics;

import org.HdrHistogram.Histogram;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class StageMetrics implements WaitMetrics {

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
    private final Histogram batchSizeHistogram = new Histogram(1, 1_000_000, 3);
    private final Histogram serviceTimeNanosHistogram = new Histogram(1, 60_000_000_000L, 3);
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

    public StageMetrics(final String name) {
        this.name = name;
    }

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

    public Histogram batchSizeHistogram() {
        return batchSizeHistogram.copy();
    }

    public Histogram serviceTimeNanosHistogram() {
        return serviceTimeNanosHistogram.copy();
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
        spinCount.increment();
    }

    @Override
    public void recordYield() {
        yieldCount.increment();
    }

    @Override
    public void recordPark() {
        parkCount.increment();
    }

    public void recordBatch(final int size, final long serviceTimeNanos) {
        batchesProcessed.increment();
        processedMessages.add(size);
        if (size > 0) {
            batchSizeHistogram.recordValue(size);
        }
        if (serviceTimeNanos > 0L) {
            serviceTimeNanosHistogram.recordValue(serviceTimeNanos);
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
}
