package com.lattice.benchmark;

import com.lattice.internal.edge.MpscRingEdge;
import com.lattice.internal.edge.SpscRingEdge;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class EdgeMicroBenchmark {

    @Benchmark
    public Object spscSameThreadSanityPath(final SpscState state) {
        state.edge.offer(state.payloads[state.value++ & state.mask]);
        return state.edge.poll();
    }

    @Benchmark
    public Object mpscSingleProducerSanityPath(final MpscState state) {
        state.edge.offer(state.payloads[state.value++ & state.mask]);
        return state.edge.poll();
    }

    @State(Scope.Thread)
    public static class SpscState {

        SpscRingEdge edge;
        Object[] payloads;
        int mask;
        int value;

        @Setup(Level.Trial)
        public void setup() {
            edge = new SpscRingEdge("producer", "consumer", 1024,
                BenchmarkMetrics.edgeMetrics("producer", "consumer"),
                BenchmarkMetrics.graphMetrics("spsc", "producer", "consumer"));
            payloads = payloads(1024);
            mask = payloads.length - 1;
        }
    }

    @State(Scope.Thread)
    public static class MpscState {

        MpscRingEdge edge;
        Object[] payloads;
        int mask;
        int value;

        @Setup(Level.Trial)
        public void setup() {
            edge = new MpscRingEdge("producer", "consumer", 1024,
                BenchmarkMetrics.edgeMetrics("producer", "consumer"),
                BenchmarkMetrics.graphMetrics("mpsc", "producer", "consumer"));
            payloads = payloads(1024);
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
