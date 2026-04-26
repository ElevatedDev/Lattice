package com.lattice.benchmark;

import java.util.concurrent.ConcurrentLinkedQueue;
import org.HdrHistogram.Histogram;

final class LatencyRecorder {

    private static final boolean ENABLED = Boolean.parseBoolean(
        System.getProperty("lattice.benchmark.latency", "true")
    );
    private static final long MAX_LATENCY_NANOS = 60_000_000_000L;
    private static final int SIGNIFICANT_DIGITS = 3;

    private final ConcurrentLinkedQueue<Histogram> histograms = new ConcurrentLinkedQueue<>();
    private final ThreadLocal<Histogram> threadHistogram = ThreadLocal.withInitial(() -> {
        final Histogram histogram = new Histogram(1, MAX_LATENCY_NANOS, SIGNIFICANT_DIGITS);
        histograms.add(histogram);
        return histogram;
    });

    void recordElapsedSince(final long startedAtNanos) {
        if (!ENABLED) {
            return;
        }
        recordDurationNanos(System.nanoTime() - startedAtNanos);
    }

    void recordDurationNanos(final long durationNanos) {
        if (!ENABLED) {
            return;
        }
        threadHistogram.get().recordValue(Math.max(1L, durationNanos));
    }

    void print(final String benchmarkName, final String latencyKind) {
        if (!ENABLED) {
            return;
        }
        final Histogram snapshot = new Histogram(1, MAX_LATENCY_NANOS, SIGNIFICANT_DIGITS);
        for (final Histogram histogram : histograms) {
            snapshot.add(histogram);
        }

        System.out.printf(
            "%s %s latency nanos p50=%d p99=%d p99.9=%d p99.99=%d max=%d samples=%d%n",
            benchmarkName,
            latencyKind,
            snapshot.getValueAtPercentile(50.0),
            snapshot.getValueAtPercentile(99.0),
            snapshot.getValueAtPercentile(99.9),
            snapshot.getValueAtPercentile(99.99),
            snapshot.getMaxValue(),
            snapshot.getTotalCount()
        );
    }
}
