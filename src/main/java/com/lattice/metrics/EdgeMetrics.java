package com.lattice.metrics;

import com.lattice.placement.MemoryMode;
import org.HdrHistogram.Histogram;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class EdgeMetrics implements WaitMetrics {

    private static final boolean RESIDENCE_TIMING_ENABLED = Boolean.getBoolean("lattice.metrics.residence");

    private final String from;
    private final String to;
    private final String allocationOwner;
    private final MemoryMode.MemoryKind memoryKind;
    private final AtomicLong emittedCount = new AtomicLong();
    private final AtomicLong consumedCount = new AtomicLong();
    private final LongAdder failedOffers = new LongAdder();
    private final LongAdder blockedOffers = new LongAdder();
    private final LongAdder backpressureNanos = new LongAdder();
    private final LongAdder droppedLatest = new LongAdder();
    private final LongAdder droppedOldest = new LongAdder();
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
    private final Histogram residenceTimeNanosHistogram = new Histogram(1, 60_000_000_000L, 3);
    private final long createdNanos = System.nanoTime();

    public EdgeMetrics(final String from, final String to) {
        this(from, to, "", MemoryMode.MemoryKind.ON_HEAP_SLOTS);
    }

    public EdgeMetrics(
        final String from,
        final String to,
        final String allocationOwner,
        final MemoryMode.MemoryKind memoryKind
    ) {
        this.from = from;
        this.to = to;
        this.allocationOwner = allocationOwner == null ? "" : allocationOwner;
        this.memoryKind = memoryKind == null ? MemoryMode.MemoryKind.ON_HEAP_SLOTS : memoryKind;
    }

    public String from() {
        return from;
    }

    public String to() {
        return to;
    }

    public String allocationOwner() {
        return allocationOwner;
    }

    public MemoryMode.MemoryKind memoryKind() {
        return memoryKind;
    }

    public long emittedCount() {
        return emittedCount.get();
    }

    public long consumedCount() {
        return consumedCount.get();
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

    public long droppedLatest() {
        return droppedLatest.sum();
    }

    public long droppedOldest() {
        return droppedOldest.sum();
    }

    public long coalescedOffers() {
        return coalescedOffers.sum();
    }

    public long redirectedOffers() {
        return redirectedOffers.sum();
    }

    public long branchIsolationActions() {
        return branchIsolationActions.sum();
    }

    public long laneSelections() {
        return laneSelections.sum();
    }

    public long hotKeySignals() {
        return hotKeySignals.sum();
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

    public long depth() {
        return currentDepth();
    }

    public long highWaterMark() {
        return highWaterMark.get();
    }

    public long residenceSamples() {
        return residenceSamples.sum();
    }

    public long firstTouchCount() {
        return firstTouchCount.sum();
    }

    public long firstTouchNanos() {
        return firstTouchNanos.sum();
    }

    public Histogram residenceTimeNanosHistogram() {
        return residenceTimeNanosHistogram.copy();
    }

    public static boolean residenceTimingEnabled() {
        return RESIDENCE_TIMING_ENABLED;
    }

    public double offerRatePerSecond() {
        return ratePerSecond(emittedCount.get());
    }

    public double pollRatePerSecond() {
        return ratePerSecond(consumedCount.get());
    }

    public void recordEmit() {
        final long emitted = emittedCount.incrementAndGet();
        final long depth = emitted - consumedCount.get() - droppedOldest.sum();
        recordDepth(depth);
    }

    public void recordConsume() {
        consumedCount.incrementAndGet();
    }

    public void recordConsume(final int count) {
        if (count > 0) {
            consumedCount.addAndGet(count);
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

    public void recordDroppedLatest() {
        droppedLatest.increment();
    }

    public void recordDroppedOldest() {
        droppedOldest.increment();
    }

    public void recordCoalescedOffer() {
        coalescedOffers.increment();
    }

    public void recordRedirectedOffer() {
        redirectedOffers.increment();
    }

    public void recordBranchIsolationAction() {
        branchIsolationActions.increment();
    }

    public void recordLaneSelection() {
        laneSelections.increment();
    }

    public void recordHotKeySignal() {
        hotKeySignals.increment();
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

    public void recordResidenceNanos(final long nanos) {
        if (nanos > 0L) {
            residenceSamples.increment();
            residenceTimeNanosHistogram.recordValue(clampToHistogram(residenceTimeNanosHistogram, nanos));
        }
    }

    public void recordFirstTouch(final long nanos) {
        firstTouchCount.increment();
        if (nanos > 0L) {
            firstTouchNanos.add(nanos);
        }
    }

    public void recordDepth(final long depth) {
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
        return emittedCount.get() - consumedCount.get() - droppedOldest.sum();
    }

    private static long clampToHistogram(final Histogram histogram, final long value) {
        return Math.min(histogram.getHighestTrackableValue(), Math.max(1L, value));
    }
}
