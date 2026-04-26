package com.lattice.examples;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.PreallocationSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.stage.PreallocatedEmitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public final class BenchmarkStyleFastPathExample {

    private static final int RING_SIZE = 1024;
    private static final int POOL_SIZE = RING_SIZE << 1;
    private static final int MESSAGES = 100_000;

    private BenchmarkStyleFastPathExample() {
    }

    public static void main(final String[] args) throws Exception {
        requireBenchmarkFlags();

        final AtomicLong consumed = new AtomicLong();
        final StaticGraph graph = StaticGraph.builder("example-benchmark-style-fast-path")
            .preallocatedSource("ingress", Signal.class,
                PreallocationSpec.pool(ignored -> new Signal()).poolSize(POOL_SIZE))
            .stage("pass", Signal.class, Signal.class, (signal, out, ctx) -> out.push(signal),
                StageSpec.singleThreaded())
            .sink("egress", Signal.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
            .edge("ingress", "pass", EdgeSpec.spscRing(RING_SIZE))
            .edge("pass", "egress", EdgeSpec.spscRing(RING_SIZE))
            .build();

        graph.start();
        final PreallocatedEmitter<Signal> ingress = graph.preallocatedEmitter("ingress", Signal.class);

        final long started = System.nanoTime();
        for (int i = 0; i < MESSAGES; i++) {
            final Signal signal = ingress.claim();
            signal.sequence = i;
            ingress.emit(signal);
        }
        ingress.close();

        while (consumed.get() != MESSAGES && graph.failure().isEmpty()) {
            Thread.onSpinWait();
        }
        if (graph.failure().isPresent()) {
            throw new IllegalStateException("graph failed", graph.failure().orElseThrow());
        }
        awaitTermination(graph);

        final long elapsedNanos = System.nanoTime() - started;
        final double throughput = MESSAGES * 1_000_000_000.0d / Math.max(1L, elapsedNanos);
        System.out.printf("messages=%d throughput=%.2f ops/s hotCounters=%s histograms=%s residence=%s%n",
            MESSAGES,
            throughput,
            StageMetrics.hotCountersEnabled(),
            StageMetrics.histogramsEnabled(),
            EdgeMetrics.residenceTimingEnabled());
    }

    private static void requireBenchmarkFlags() {
        if (StageMetrics.hotCountersEnabled()) {
            throw new IllegalStateException("run with -Dlattice.metrics.hotCounters=false for throughput-only timing");
        }
        if (StageMetrics.histogramsEnabled()) {
            throw new IllegalStateException("omit -Dlattice.metrics.stageHistograms=true for throughput-only timing");
        }
        if (EdgeMetrics.residenceTimingEnabled()) {
            throw new IllegalStateException("omit -Dlattice.metrics.residence=true for throughput-only timing");
        }
        if (Boolean.getBoolean("lattice.jfr")) {
            throw new IllegalStateException("omit -Dlattice.jfr=true unless measuring JFR overhead");
        }
    }

    private static void awaitTermination(final StaticGraph graph) throws InterruptedException {
        if (!graph.awaitTermination(Duration.ofSeconds(10))) {
            graph.abort();
            throw new IllegalStateException("graph did not stop cleanly");
        }
    }

    public static final class Signal {
        long sequence;
    }
}
