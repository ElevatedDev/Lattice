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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class TopologyBenchmark {

    @Benchmark
    public void oneSourceOneSink(final SourceSinkState state) {
        final long started = System.nanoTime();
        state.emitter.emit(started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void oneSourceValidateSink(final ValidateState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, true, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @State(Scope.Benchmark)
    public static class SourceSinkState {

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Long> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("source-sink")
                .source("ingress", Long.class)
                .sink("egress", Long.class, emittedAtNanos -> {
                    consumed.incrementAndGet();
                    endToEndLatency.recordElapsedSince(emittedAtNanos);
                }, StageSpec.singleThreaded())
                .edge("ingress", "egress", EdgeSpec.mpscRing(4096))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Long.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("oneSourceOneSink", "enqueue");
            endToEndLatency.print("oneSourceOneSink", "endToEnd");
        }
    }

    @State(Scope.Benchmark)
    public static class ValidateState {

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Order> emitter;
        long next;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("validate-sink")
                .source("ingress", Order.class)
                .stage(
                    "validate",
                    Order.class,
                    ValidOrder.class,
                    (order, out, ctx) -> {
                        if (order.valid()) {
                            out.push(new ValidOrder(order.id(), order.emittedAtNanos()));
                        }
                    },
                    StageSpec.singleThreaded()
                )
                .sink("egress", ValidOrder.class, order -> {
                    consumed.incrementAndGet();
                    endToEndLatency.recordElapsedSince(order.emittedAtNanos());
                }, StageSpec.singleThreaded())
                .edge("ingress", "validate", EdgeSpec.mpscRing(4096))
                .edge("validate", "egress", EdgeSpec.spscRing(4096))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Order.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("oneSourceValidateSink", "enqueue");
            endToEndLatency.print("oneSourceValidateSink", "endToEnd");
        }
    }

    public record Order(long id, boolean valid, long emittedAtNanos) {
    }

    public record ValidOrder(long id, long emittedAtNanos) {
    }
}
