package io.github.elevateddev.lattice.benchmark;

import io.github.elevateddev.lattice.internal.edge.MpscRingEdge;
import io.github.elevateddev.lattice.internal.edge.SpscRingEdge;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class EdgePairBenchmark {

    @Benchmark
    @Group("spscPair")
    @GroupThreads(1)
    public boolean spscOffer(final SpscPairState state) {
        final Object item = state.payloads[state.sequence++ & state.mask];
        while (!state.edge.offer(item)) {
            Thread.onSpinWait();
        }
        return true;
    }

    @Benchmark
    @Group("spscPair")
    @GroupThreads(1)
    public Object spscPoll(final SpscPairState state) {
        return state.edge.poll();
    }

    @Benchmark
    @Group("mpscPair")
    @GroupThreads(1)
    public boolean mpscOffer(final MpscPairState state) {
        final Object item = state.payloads[state.sequence++ & state.mask];
        while (!state.edge.offer(item)) {
            Thread.onSpinWait();
        }
        return true;
    }

    @Benchmark
    @Group("mpscPair")
    @GroupThreads(1)
    public Object mpscPoll(final MpscPairState state) {
        return state.edge.poll();
    }

    @State(Scope.Group)
    public static class SpscPairState {
        SpscRingEdge edge;
        Object[] payloads;
        int mask;
        int sequence;

        @Setup(Level.Trial)
        public void setup() {
            edge = new SpscRingEdge("producer", "consumer", 8192,
                BenchmarkMetrics.edgeMetrics("producer", "consumer"),
                BenchmarkMetrics.graphMetrics("spsc-pair", "producer", "consumer"));
            payloads = payloads(8192);
            mask = payloads.length - 1;
        }
    }

    @State(Scope.Group)
    public static class MpscPairState {
        MpscRingEdge edge;
        Object[] payloads;
        int mask;
        int sequence;

        @Setup(Level.Trial)
        public void setup() {
            edge = new MpscRingEdge("producer", "consumer", 8192,
                BenchmarkMetrics.edgeMetrics("producer", "consumer"),
                BenchmarkMetrics.graphMetrics("mpsc-pair", "producer", "consumer"));
            payloads = payloads(8192);
            mask = payloads.length - 1;
        }
    }

    private static Object[] payloads(final int size) {
        final Object[] payloads = new Object[size];
        for (int i = 0; i < size; i++) {
            payloads[i] = new Object();
        }
        return payloads;
    }
}
