package io.github.elevateddev.lattice.metrics;

import io.github.elevateddev.lattice.graph.MetricsSpec;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageMetricsTest {

    @Test
    void defaultsExposeCreatedButUnstartedWorkerState() {
        final StageMetrics metrics = new StageMetrics("stage");

        assertEquals("stage", metrics.name());
        assertFalse(metrics.hotCounters());
        assertFalse(metrics.histograms());
        assertFalse(StageMetrics.hotCountersEnabled());
        assertFalse(StageMetrics.histogramsEnabled());
        assertSame(WorkerState.NEW, metrics.workerState());
        assertSame(PlacementStatus.NOT_REQUESTED, metrics.placementStatus());
        assertEquals("", metrics.placementMessage());
        assertEquals(-1, metrics.expectedCpu());
        assertEquals(-1, metrics.observedCpu());
        assertEquals(-1, metrics.expectedNumaNode());
        assertEquals(-1, metrics.observedNumaNode());
        assertEquals("", metrics.allocationOwner());
        assertNull(metrics.startTime());
        assertNull(metrics.stopTime());
        assertEquals(0.0d, metrics.processRatePerSecond());
        assertEquals(0, metrics.batchSizeHistogram().getTotalCount());
        assertEquals(0, metrics.serviceTimeNanosHistogram().getTotalCount());
    }

    @Test
    void hotCounterUpdatesAreIgnoredWhenMetricsAreOffButStateMetadataRecords() {
        final StageMetrics metrics = new StageMetrics("stage", MetricsSpec.off());

        metrics.recordEmit();
        metrics.recordConsume();
        metrics.recordConsume(3);
        metrics.recordException();
        metrics.recordFailedOutput();
        metrics.recordFailedOutputs(2);
        metrics.recordBlockedOutput();
        metrics.recordBlockedNanos(11);
        metrics.recordSpin();
        metrics.recordYield();
        metrics.recordPark();
        metrics.recordBatch(5, 99);
        metrics.recordRoutingDecision();
        metrics.recordBranchIsolationAction();
        metrics.recordOpenJoinGroup();
        metrics.recordCompletedJoinGroup();
        metrics.recordTimedOutJoinGroup();
        metrics.recordDuplicateJoinStamp();
        metrics.recordMissingJoinBranch();
        metrics.recordRetainedHandle();
        metrics.recordReleasedHandle();
        metrics.recordPlacement(PlacementStatus.APPLIED, null, 1, 2, 3, 4, null, true, true);
        metrics.workerState(WorkerState.RUNNING);
        metrics.markStarted();
        metrics.markStopped();

        assertEquals(0, metrics.emittedCount());
        assertEquals(0, metrics.consumedCount());
        assertEquals(0, metrics.stageExceptions());
        assertEquals(0, metrics.failedOutputs());
        assertEquals(0, metrics.blockedOutputs());
        assertEquals(0, metrics.blockedNanos());
        assertEquals(0, metrics.spinCount());
        assertEquals(0, metrics.yieldCount());
        assertEquals(0, metrics.parkCount());
        assertEquals(0, metrics.batchesProcessed());
        assertEquals(0, metrics.processedMessages());
        assertEquals(0, metrics.routingDecisions());
        assertEquals(0, metrics.branchIsolationActions());
        assertEquals(0, metrics.openJoinGroups());
        assertEquals(0, metrics.completedJoinGroups());
        assertEquals(0, metrics.timedOutJoinGroups());
        assertEquals(0, metrics.duplicateJoinStamps());
        assertEquals(0, metrics.missingJoinBranches());
        assertEquals(0, metrics.retainedHandles());
        assertEquals(0, metrics.releasedHandles());
        assertEquals(0, metrics.affinityViolations());
        assertEquals(0, metrics.numaViolations());
        assertSame(PlacementStatus.APPLIED, metrics.placementStatus());
        assertEquals("", metrics.placementMessage());
        assertEquals(1, metrics.expectedCpu());
        assertEquals(2, metrics.observedCpu());
        assertEquals(3, metrics.expectedNumaNode());
        assertEquals(4, metrics.observedNumaNode());
        assertEquals("", metrics.allocationOwner());
        assertSame(WorkerState.RUNNING, metrics.workerState());
        assertNotNull(metrics.startTime());
        assertNotNull(metrics.stopTime());
    }

    @Test
    void hotCountersRecordStageWaitRoutingJoinAndHandleMetrics() {
        final StageMetrics metrics = new StageMetrics("stage", MetricsSpec.off().hotCounters(true));

        metrics.recordEmit();
        metrics.recordConsume();
        metrics.recordConsume(2);
        metrics.recordConsume(0);
        metrics.recordException();
        metrics.recordFailedOutput();
        metrics.recordFailedOutputs(4);
        metrics.recordFailedOutputs(0);
        metrics.recordBlockedOutput();
        metrics.recordBlockedNanos(13);
        metrics.recordBlockedNanos(0);
        metrics.recordSpin();
        metrics.recordYield();
        metrics.recordPark();
        metrics.recordBatch(5, 99);
        metrics.recordRoutingDecision();
        metrics.recordBranchIsolationAction();
        metrics.recordOpenJoinGroup();
        metrics.recordCompletedJoinGroup();
        metrics.recordTimedOutJoinGroup();
        metrics.recordDuplicateJoinStamp();
        metrics.recordMissingJoinBranch();
        metrics.recordRetainedHandle();
        metrics.recordReleasedHandle();
        metrics.recordPlacement(PlacementStatus.DEGRADED, "fallback", 1, 2, 3, 4, "owner", true, true);
        metrics.workerState(WorkerState.BLOCKED);
        metrics.markStarted();

        assertTrue(metrics.hotCounters());
        assertEquals(1, metrics.emittedCount());
        assertEquals(3, metrics.consumedCount());
        assertEquals(1, metrics.stageExceptions());
        assertEquals(5, metrics.failedOutputs());
        assertEquals(1, metrics.blockedOutputs());
        assertEquals(13, metrics.blockedNanos());
        assertEquals(1, metrics.spinCount());
        assertEquals(1, metrics.yieldCount());
        assertEquals(1, metrics.parkCount());
        assertEquals(1, metrics.batchesProcessed());
        assertEquals(5, metrics.processedMessages());
        assertEquals(1, metrics.routingDecisions());
        assertEquals(1, metrics.branchIsolationActions());
        assertEquals(1, metrics.openJoinGroups());
        assertEquals(1, metrics.completedJoinGroups());
        assertEquals(1, metrics.timedOutJoinGroups());
        assertEquals(1, metrics.duplicateJoinStamps());
        assertEquals(1, metrics.missingJoinBranches());
        assertEquals(1, metrics.retainedHandles());
        assertEquals(1, metrics.releasedHandles());
        assertEquals(1, metrics.affinityViolations());
        assertEquals(1, metrics.numaViolations());
        assertSame(PlacementStatus.DEGRADED, metrics.placementStatus());
        assertEquals("fallback", metrics.placementMessage());
        assertEquals("owner", metrics.allocationOwner());
        assertSame(WorkerState.BLOCKED, metrics.workerState());
        assertTrue(metrics.processRatePerSecond() > 0.0d);
    }

    @Test
    void histogramsAreOptInAndReturnedAsDefensiveCopies() {
        final StageMetrics disabled = new StageMetrics("stage", MetricsSpec.off());
        disabled.recordBatch(4, 100);

        assertEquals(0, disabled.batchSizeHistogram().getTotalCount());
        assertEquals(0, disabled.serviceTimeNanosHistogram().getTotalCount());

        final StageMetrics enabled = new StageMetrics("stage", MetricsSpec.off().stageHistograms(true));
        enabled.recordBatch(0, 0);
        enabled.recordBatch(4, 100);
        enabled.recordBatch(1_000_001, Long.MAX_VALUE);

        assertTrue(enabled.histograms());
        assertEquals(2, enabled.batchSizeHistogram().getTotalCount());
        assertEquals(2, enabled.serviceTimeNanosHistogram().getTotalCount());
        assertEquals(0, enabled.batchesProcessed());
        assertEquals(0, enabled.processedMessages());

        final Histogram batchSnapshot = enabled.batchSizeHistogram();
        final Histogram serviceSnapshot = enabled.serviceTimeNanosHistogram();
        batchSnapshot.recordValue(1);
        serviceSnapshot.recordValue(1);

        assertEquals(2, enabled.batchSizeHistogram().getTotalCount());
        assertEquals(2, enabled.serviceTimeNanosHistogram().getTotalCount());
    }

    @Test
    void lifecycleTimestampsRecordFirstStartAndStopOnly() {
        final StageMetrics metrics = new StageMetrics("stage");

        metrics.markStarted();
        final Instant firstStart = metrics.startTime();
        metrics.markStarted();
        metrics.markStopped();
        final Instant firstStop = metrics.stopTime();
        metrics.markStopped();

        assertNotNull(firstStart);
        assertNotNull(firstStop);
        assertSame(firstStart, metrics.startTime());
        assertSame(firstStop, metrics.stopTime());
    }
}
