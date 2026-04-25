package com.lattice.benchmark;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.openjdk.jmh.annotations.TearDown;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class MpscIngressBenchmark {

    @Benchmark
    @Group("mpscIngress")
    @GroupThreads(4)
    public void producers(final MpscState state) {
        final long started = System.nanoTime();
        state.emitter.emit(started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @State(Scope.Benchmark)
    public static class MpscState {

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Long> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("mpsc-ingress")
                .source("ingress", Long.class)
                .sink("egress", Long.class, emittedAtNanos -> {
                    consumed.incrementAndGet();
                    endToEndLatency.recordElapsedSince(emittedAtNanos);
                }, StageSpec.singleThreaded())
                .edge("ingress", "egress", EdgeSpec.mpscRing(8192))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Long.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("mpscIngress", "enqueue");
            endToEndLatency.print("mpscIngress", "endToEnd");
        }
    }
}
