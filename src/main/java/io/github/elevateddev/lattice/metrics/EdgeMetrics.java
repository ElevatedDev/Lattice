package io.github.elevateddev.lattice.metrics;

import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.placement.MemoryMode;
import org.HdrHistogram.Histogram;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Live metrics for one edge.
 * <p>
 * Hot-path counters and residence-time histograms are graph-level opt-ins.
 * Histogram accessors return defensive copies.
 * Runtime update methods such as {@code recordEmit()} are exposed for the
 * runtime implementation and should not be called by applications.
 */
public final class EdgeMetrics implements WaitMetrics {

    private static final int DEPTH_SAMPLE_SHIFT = 10; // sample every 1024 emits
    private static final long DEPTH_SAMPLE_MASK = (1L << DEPTH_SAMPLE_SHIFT) - 1L;

    private final String from;
    private final String to;
    private final String allocationOwner;
    private final MemoryMode.MemoryKind memoryKind;
    private final boolean hotCountersEnabled;
    private final boolean residenceTimingEnabled;

    private final LongAdder emittedCount = new LongAdder();
    private final LongAdder consumedCount = new LongAdder();
    private final LongAdder failedOffers = new LongAdder();
    private final LongAdder blockedOffers = new LongAdder();
    private final LongAdder backpressureNanos = new LongAdder();
    private final LongAdder droppedLatest = new LongAdder();
    private final LongAdder droppedOldest = new LongAdder();
    private final LongAdder droppedOldestCount = new LongAdder();
    private final LongAdder coalescedOffers = new LongAdder();
    private final LongAdder redirectedOffers = new LongAdder();
    private final LongAdder branchIsolationActions = new LongAdder();
    private final LongAdder laneSelections = new LongAdder();
    private final LongAdder hotKeySignals = new LongAdder();
    private final LongAdder spinCount = new LongAdder();
    private final LongAdder yieldCount = new LongAdder();
    private final LongAdder parkCount = new LongAdder();
    private final AtomicLong highWaterMark = new AtomicLong();
    private final LongAdder residenceSamples = new LongAdder();
    private final LongAdder firstTouchCount = new LongAdder();
    private final LongAdder firstTouchNanos = new LongAdder();
    private final Histogram residenceTimeNanosHistogram;
    private final long createdNanos = System.nanoTime();

    /**
     * Creates on-heap edge metrics without an allocation owner.
     */
    public EdgeMetrics(final String from, final String to) {
        this(from, to, "", MemoryMode.MemoryKind.ON_HEAP_SLOTS, MetricsSpec.off());
    }

    /**
     * Returns the source-compatible default hot-counter state.
     */
    public static boolean hotCountersEnabled() {
        return false;
    }

    /**
     * Creates edge metrics with allocation metadata.
     */
    public EdgeMetrics(
        final String from,
        final String to,
        final String allocationOwner,
        final MemoryMode.MemoryKind memoryKind
    ) {
        this(from, to, allocationOwner, memoryKind, MetricsSpec.off());
    }

    /**
     * Creates edge metrics with allocation metadata.
     */
    public EdgeMetrics(
        final String from,
        final String to,
        final String allocationOwner,
        final MemoryMode.MemoryKind memoryKind,
        final MetricsSpec metricsSpec
    ) {
        this.from = from;
        this.to = to;
        this.allocationOwner = allocationOwner == null ? "" : allocationOwner;
        this.memoryKind = memoryKind == null ? MemoryMode.MemoryKind.ON_HEAP_SLOTS : memoryKind;
        this.hotCountersEnabled = metricsSpec.hotCounters();
        this.residenceTimingEnabled = metricsSpec.residenceTiming();
        this.residenceTimeNanosHistogram = residenceTimingEnabled
            ? new Histogram(1, 60_000_000_000L, 3)
            : null;
    }

    /**
     * Returns the source node name.
     */
    public String from() {
        return from;
    }

    /**
     * Returns the target node name.
     */
    public String to() {
        return to;
    }

    /**
     * Returns the stage or subsystem that owns this edge allocation, when known.
     */
    public String allocationOwner() {
        return allocationOwner;
    }

    /**
     * Returns the configured edge memory kind.
     */
    public MemoryMode.MemoryKind memoryKind() {
        return memoryKind;
    }

    /**
     * Returns whether hot-path counters are enabled for this edge.
     */
    public boolean hotCounters() {
        return hotCountersEnabled;
    }

    /**
     * Returns whether residence-time tracking is enabled for this edge.
     */
    public boolean residenceTiming() {
        return residenceTimingEnabled;
    }

    /**
     * Returns offers accepted by this edge when hot counters are enabled.
     */
    public long emittedCount() {
        return emittedCount.sum();
    }

    /**
     * Returns items consumed from this edge when hot counters are enabled.
     */
    public long consumedCount() {
        return consumedCount.sum();
    }

    /**
     * Returns failed offer attempts.
     */
    public long failedOffers() {
        return failedOffers.sum();
    }

    /**
     * Returns offer attempts that had to wait for capacity.
     */
    public long blockedOffers() {
        return blockedOffers.sum();
    }

    /**
     * Returns total nanoseconds spent under edge backpressure.
     */
    public long backpressureNanos() {
        return backpressureNanos.sum();
    }

    /**
     * Returns messages dropped by drop-latest/drop-newest policy.
     */
    public long droppedLatest() {
        return droppedLatest.sum();
    }

    /**
     * Returns messages dropped by drop-oldest policy.
     */
    public long droppedOldest() {
        return droppedOldest.sum();
    }

    /**
     * Returns offers merged by coalescing overflow policy.
     */
    public long coalescedOffers() {
        return coalescedOffers.sum();
    }

    /**
     * Returns offers redirected by overflow policy.
     */
    public long redirectedOffers() {
        return redirectedOffers.sum();
    }

    /**
     * Returns branch-isolation actions taken for this edge.
     */
    public long branchIsolationActions() {
        return branchIsolationActions.sum();
    }

    /**
     * Returns partition lane selections made for this edge.
     */
    public long laneSelections() {
        return laneSelections.sum();
    }

    /**
     * Returns hot-key signals observed for this edge.
     */
    public long hotKeySignals() {
        return hotKeySignals.sum();
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
     * Returns current logical depth, derived from emitted, consumed, and dropped
     * oldest counts.
     */
    public long depth() {
        return currentDepth();
    }

    /**
     * Returns sampled high-water logical depth.
     */
    public long highWaterMark() {
        return highWaterMark.get();
    }

    /**
     * Returns number of residence-time samples recorded.
     */
    public long residenceSamples() {
        return residenceSamples.sum();
    }

    /**
     * Returns first-touch operation count for this edge.
     */
    public long firstTouchCount() {
        return firstTouchCount.sum();
    }

    /**
     * Returns nanoseconds spent first-touching this edge's memory.
     */
    public long firstTouchNanos() {
        return firstTouchNanos.sum();
    }

    /**
     * Returns a copy of residence-time histogram data.
     */
    public Histogram residenceTimeNanosHistogram() {
        return residenceTimeNanosHistogram == null
            ? new Histogram(1, 60_000_000_000L, 3)
            : residenceTimeNanosHistogram.copy();
    }

    /**
     * Returns whether residence timing is enabled by default.
     */
    public static boolean residenceTimingEnabled() {
        return false;
    }

    /**
     * Returns approximate offer throughput since metric creation.
     */
    public double offerRatePerSecond() {
        return ratePerSecond(emittedCount.sum());
    }

    /**
     * Returns approximate poll throughput since metric creation.
     */
    public double pollRatePerSecond() {
        return ratePerSecond(consumedCount.sum());
    }

    private final ThreadLocal<long[]> emitTick = ThreadLocal.withInitial(() -> new long[1]);

    public void recordEmit() {
        if (!hotCountersEnabled) {
            return;
        }
        emittedCount.increment();
        final long[] tick = emitTick.get();
        final long next = ++tick[0];
        if (next <= DEPTH_SAMPLE_MASK || (next & DEPTH_SAMPLE_MASK) == 0L) {
            final long depth = emittedCount.sum() - consumedCount.sum() - droppedOldestCount.sum();
            recordDepth(depth);
        }
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

    public void recordDroppedLatest() {
        if (!hotCountersEnabled) {
            return;
        }
        droppedLatest.increment();
    }

    public void recordDroppedOldest() {
        if (!hotCountersEnabled) {
            return;
        }
        droppedOldest.increment();
        droppedOldestCount.increment();
    }

    public void recordCoalescedOffer() {
        if (!hotCountersEnabled) {
            return;
        }
        coalescedOffers.increment();
    }

    public void recordRedirectedOffer() {
        if (!hotCountersEnabled) {
            return;
        }
        redirectedOffers.increment();
    }

    public void recordBranchIsolationAction() {
        if (!hotCountersEnabled) {
            return;
        }
        branchIsolationActions.increment();
    }

    public void recordLaneSelection() {
        if (!hotCountersEnabled) {
            return;
        }
        laneSelections.increment();
    }

    public void recordHotKeySignal() {
        if (!hotCountersEnabled) {
            return;
        }
        hotKeySignals.increment();
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

    public void recordResidenceNanos(final long nanos) {
        if (residenceTimeNanosHistogram != null && nanos > 0L) {
            residenceSamples.increment();
            residenceTimeNanosHistogram.recordValue(clampToHistogram(residenceTimeNanosHistogram, nanos));
        }
    }

    public void recordFirstTouch(final long nanos) {
        if (!hotCountersEnabled) {
            return;
        }
        firstTouchCount.increment();
        if (nanos > 0L) {
            firstTouchNanos.add(nanos);
        }
    }

    public void recordDepth(final long depth) {
        if (!hotCountersEnabled) {
            return;
        }
        long current = highWaterMark.get();
        if (depth <= current) {
            return;
        }

        while (depth > current && !highWaterMark.compareAndSet(current, depth)) {
            current = highWaterMark.get();
        }
    }

    private double ratePerSecond(final long count) {
        final long elapsedNanos = Math.max(1L, System.nanoTime() - createdNanos);
        return count * 1_000_000_000.0d / elapsedNanos;
    }

    private long currentDepth() {
        return emittedCount.sum() - consumedCount.sum() - droppedOldestCount.sum();
    }

    private static long clampToHistogram(final Histogram histogram, final long value) {
        return Math.min(histogram.getHighestTrackableValue(), Math.max(1L, value));
    }
}
