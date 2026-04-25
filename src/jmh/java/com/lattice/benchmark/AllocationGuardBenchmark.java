package com.lattice.benchmark;

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
public class AllocationGuardBenchmark {

    @Benchmark
    public Object spscSteadyState(final SpscAllocationState state) {
        state.edge.offer(state.payload);
        return state.edge.poll();
    }

    @State(Scope.Thread)
    public static class SpscAllocationState {

        final Object payload = new Object();
        SpscRingEdge edge;

        @Setup(Level.Trial)
        public void setup() {
            edge = new SpscRingEdge("producer", "consumer", 1024,
                BenchmarkMetrics.edgeMetrics("producer", "consumer"),
                BenchmarkMetrics.graphMetrics("allocation", "producer", "consumer"));
        }
    }
}
