package com.lattice.benchmark;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.stage.BatchPolicy;
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
public class BatchTopologyBenchmark {

    @Benchmark
    public void batchedValidateSink(final BatchState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, true, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @State(Scope.Benchmark)
    public static class BatchState {

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Order> emitter;
        long next;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("batched-validate-sink")
                .source("ingress", Order.class)
                .batchStage(
                    "validate",
                    Order.class,
                    ValidOrder.class,
                    (batch, out, ctx) -> {
                        for (int i = 0; i < batch.size(); i++) {
                            final Order order = batch.get(i);
                            if (order.valid()) {
                                out.push(new ValidOrder(order.id(), order.emittedAtNanos()));
                            }
                        }
                    },
                    StageSpec.singleThreaded().batch(BatchPolicy.maxItems(32))
                )
                .sink("egress", ValidOrder.class, order -> {
                    consumed.incrementAndGet();
                    endToEndLatency.recordElapsedSince(order.emittedAtNanos());
                }, StageSpec.singleThreaded())
                .edge("ingress", "validate", EdgeSpec.mpscRing(8192).batch(BatchPolicy.maxItems(32)))
                .edge("validate", "egress", EdgeSpec.spscRing(8192).batch(BatchPolicy.maxItems(32)))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Order.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("batchedValidateSink", "enqueue");
            endToEndLatency.print("batchedValidateSink", "endToEnd");
        }
    }

    public record Order(long id, boolean valid, long emittedAtNanos) {
    }

    public record ValidOrder(long id, long emittedAtNanos) {
    }
}
