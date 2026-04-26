package com.lattice.benchmark;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.SourceMode;
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
    public void oneSourceOneSinkSingleProducer(final SourceSinkSingleProducerState state) {
        final long started = System.nanoTime();
        state.emitter.emit(started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void oneSourceOneSinkPreallocatedSingleProducer(final SourceSinkPreallocatedSingleProducerState state) {
        final long started = System.nanoTime();
        final PreallocatedSignal signal = state.claim(started);
        state.emitter.emit(signal);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void oneSourceValidateSink(final ValidateState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, true, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void oneSourceValidateSinkSingleProducer(final ValidateSingleProducerState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, true, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void oneSourceValidateSinkFused(final ValidateFusedState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, true, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void oneSourceValidateSinkSingleProducerFused(final ValidateSingleProducerFusedState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, true, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void oneSourceNormalizeValidateSink(final NormalizeValidateState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, true, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void oneSourceNormalizeValidateSinkFused(final NormalizeValidateFusedState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, true, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void oneSourceThreeStageSink(final ThreeStageState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, true, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void oneSourceThreeStageSinkFused(final ThreeStageFusedState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, true, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void oneSourceThreeStageSinkPreallocatedFused(final ThreeStagePreallocatedFusedState state) {
        final long started = System.nanoTime();
        final ReusableOrder order = state.claim(started);
        state.emitter.emit(order);
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
    public static class SourceSinkSingleProducerState {

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Long> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("source-sink-single-producer")
                .source("ingress", Long.class, SourceMode.SINGLE_PRODUCER)
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
            enqueueLatency.print("oneSourceOneSinkSingleProducer", "enqueue");
            endToEndLatency.print("oneSourceOneSinkSingleProducer", "endToEnd");
        }
    }

    @State(Scope.Benchmark)
    public static class SourceSinkPreallocatedSingleProducerState {

        private static final int POOL_SIZE = 8192;

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        final PreallocatedSignal[] signals = preallocatedSignals();
        StaticGraph graph;
        Emitter<PreallocatedSignal> emitter;
        long next;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("source-sink-preallocated-single-producer")
                .source("ingress", PreallocatedSignal.class, SourceMode.SINGLE_PRODUCER)
                .sink("egress", PreallocatedSignal.class, signal -> {
                    consumed.incrementAndGet();
                    endToEndLatency.recordElapsedSince(signal.emittedAtNanos);
                    signal.consumedSequence = signal.sequence;
                }, StageSpec.singleThreaded())
                .edge("ingress", "egress", EdgeSpec.mpscRing(POOL_SIZE))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", PreallocatedSignal.class);
        }

        PreallocatedSignal claim(final long emittedAtNanos) {
            final long sequence = next++;
            final PreallocatedSignal signal = signals[(int) sequence & (POOL_SIZE - 1)];
            final long requiredConsumedSequence = sequence - POOL_SIZE;
            while (requiredConsumedSequence >= 0L && signal.consumedSequence < requiredConsumedSequence) {
                Thread.onSpinWait();
            }
            signal.sequence = sequence;
            signal.emittedAtNanos = emittedAtNanos;
            return signal;
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("oneSourceOneSinkPreallocatedSingleProducer", "enqueue");
            endToEndLatency.print("oneSourceOneSinkPreallocatedSingleProducer", "endToEnd");
        }

        private static PreallocatedSignal[] preallocatedSignals() {
            final PreallocatedSignal[] signals = new PreallocatedSignal[POOL_SIZE];
            for (int i = 0; i < signals.length; i++) {
                signals[i] = new PreallocatedSignal();
            }
            return signals;
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
            setupGraph("validate-sink", SourceMode.MULTI_PRODUCER, false);
        }

        void setupGraph(final String graphName, final SourceMode sourceMode, final boolean fusionEnabled) {
            final String previousFusion = System.getProperty("lattice.fusion.enabled");
            if (fusionEnabled) {
                System.setProperty("lattice.fusion.enabled", "true");
            } else {
                System.clearProperty("lattice.fusion.enabled");
            }
            try {
                graph = StaticGraph.builder(graphName)
                    .source("ingress", Order.class, sourceMode)
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
            } finally {
                restoreFusionProperty(previousFusion);
            }
            graph.start();
            emitter = graph.emitter("ingress", Order.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            tearDownGraph("oneSourceValidateSink");
        }

        void tearDownGraph(final String benchmarkName) {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print(benchmarkName, "enqueue");
            endToEndLatency.print(benchmarkName, "endToEnd");
        }

        private static void restoreFusionProperty(final String previousFusion) {
            if (previousFusion == null) {
                System.clearProperty("lattice.fusion.enabled");
            } else {
                System.setProperty("lattice.fusion.enabled", previousFusion);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class ValidateSingleProducerState extends ValidateState {
        @Override
        @Setup(Level.Trial)
        public void setup() {
            setupGraph("validate-sink-single-producer", SourceMode.SINGLE_PRODUCER, false);
        }

        @Override
        @TearDown(Level.Trial)
        public void tearDown() {
            tearDownGraph("oneSourceValidateSinkSingleProducer");
        }
    }

    @State(Scope.Benchmark)
    public static class ValidateFusedState extends ValidateState {
        @Override
        @Setup(Level.Trial)
        public void setup() {
            setupGraph("validate-sink-fused", SourceMode.MULTI_PRODUCER, true);
        }

        @Override
        @TearDown(Level.Trial)
        public void tearDown() {
            tearDownGraph("oneSourceValidateSinkFused");
        }
    }

    @State(Scope.Benchmark)
    public static class ValidateSingleProducerFusedState extends ValidateState {
        @Override
        @Setup(Level.Trial)
        public void setup() {
            setupGraph("validate-sink-single-producer-fused", SourceMode.SINGLE_PRODUCER, true);
        }

        @Override
        @TearDown(Level.Trial)
        public void tearDown() {
            tearDownGraph("oneSourceValidateSinkSingleProducerFused");
        }
    }

    @State(Scope.Benchmark)
    public static class NormalizeValidateState {

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Order> emitter;
        long next;

        @Setup(Level.Trial)
        public void setup() {
            setupGraph("normalize-validate-sink", false);
        }

        void setupGraph(final String graphName, final boolean fusionEnabled) {
            final String previousFusion = System.getProperty("lattice.fusion.enabled");
            if (fusionEnabled) {
                System.setProperty("lattice.fusion.enabled", "true");
            } else {
                System.clearProperty("lattice.fusion.enabled");
            }
            try {
                graph = StaticGraph.builder(graphName)
                    .source("ingress", Order.class)
                    .stage(
                        "normalize",
                        Order.class,
                        Order.class,
                        (order, out, ctx) -> out.push(order),
                        StageSpec.singleThreaded()
                    )
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
                    .edge("ingress", "normalize", EdgeSpec.mpscRing(4096))
                    .edge("normalize", "validate", EdgeSpec.spscRing(4096))
                    .edge("validate", "egress", EdgeSpec.spscRing(4096))
                    .build();
            } finally {
                restoreFusionProperty(previousFusion);
            }
            graph.start();
            emitter = graph.emitter("ingress", Order.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            tearDownGraph("oneSourceNormalizeValidateSink");
        }

        void tearDownGraph(final String benchmarkName) {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print(benchmarkName, "enqueue");
            endToEndLatency.print(benchmarkName, "endToEnd");
        }

        private static void restoreFusionProperty(final String previousFusion) {
            if (previousFusion == null) {
                System.clearProperty("lattice.fusion.enabled");
            } else {
                System.setProperty("lattice.fusion.enabled", previousFusion);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class NormalizeValidateFusedState extends NormalizeValidateState {
        @Override
        @Setup(Level.Trial)
        public void setup() {
            setupGraph("normalize-validate-sink-fused", true);
        }

        @Override
        @TearDown(Level.Trial)
        public void tearDown() {
            tearDownGraph("oneSourceNormalizeValidateSinkFused");
        }
    }

    @State(Scope.Benchmark)
    public static class ThreeStageState {

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Order> emitter;
        long next;

        @Setup(Level.Trial)
        public void setup() {
            setupGraph("three-stage-sink", false);
        }

        void setupGraph(final String graphName, final boolean fusionEnabled) {
            final String previousFusion = System.getProperty("lattice.fusion.enabled");
            if (fusionEnabled) {
                System.setProperty("lattice.fusion.enabled", "true");
            } else {
                System.clearProperty("lattice.fusion.enabled");
            }
            try {
                graph = StaticGraph.builder(graphName)
                    .source("ingress", Order.class)
                    .stage(
                        "normalize",
                        Order.class,
                        Order.class,
                        (order, out, ctx) -> out.push(order),
                        StageSpec.singleThreaded()
                    )
                    .stage(
                        "risk",
                        Order.class,
                        Order.class,
                        (order, out, ctx) -> out.push(order),
                        StageSpec.singleThreaded()
                    )
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
                    .edge("ingress", "normalize", EdgeSpec.mpscRing(4096))
                    .edge("normalize", "risk", EdgeSpec.spscRing(4096))
                    .edge("risk", "validate", EdgeSpec.spscRing(4096))
                    .edge("validate", "egress", EdgeSpec.spscRing(4096))
                    .build();
            } finally {
                restoreFusionProperty(previousFusion);
            }
            graph.start();
            emitter = graph.emitter("ingress", Order.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            tearDownGraph("oneSourceThreeStageSink");
        }

        void tearDownGraph(final String benchmarkName) {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print(benchmarkName, "enqueue");
            endToEndLatency.print(benchmarkName, "endToEnd");
        }

        private static void restoreFusionProperty(final String previousFusion) {
            if (previousFusion == null) {
                System.clearProperty("lattice.fusion.enabled");
            } else {
                System.setProperty("lattice.fusion.enabled", previousFusion);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class ThreeStageFusedState extends ThreeStageState {
        @Override
        @Setup(Level.Trial)
        public void setup() {
            setupGraph("three-stage-sink-fused", true);
        }

        @Override
        @TearDown(Level.Trial)
        public void tearDown() {
            tearDownGraph("oneSourceThreeStageSinkFused");
        }
    }

    @State(Scope.Benchmark)
    public static class ThreeStagePreallocatedFusedState {

        private static final int POOL_SIZE = 8192;

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        final ReusableOrder[] orders = preallocatedOrders();
        StaticGraph graph;
        Emitter<ReusableOrder> emitter;
        long next;

        @Setup(Level.Trial)
        public void setup() {
            final String previousFusion = System.getProperty("lattice.fusion.enabled");
            System.setProperty("lattice.fusion.enabled", "true");
            try {
                graph = StaticGraph.builder("three-stage-sink-preallocated-fused")
                    .source("ingress", ReusableOrder.class, SourceMode.SINGLE_PRODUCER)
                    .stage(
                        "normalize",
                        ReusableOrder.class,
                        ReusableOrder.class,
                        (order, out, ctx) -> out.push(order),
                        StageSpec.singleThreaded()
                    )
                    .stage(
                        "risk",
                        ReusableOrder.class,
                        ReusableOrder.class,
                        (order, out, ctx) -> out.push(order),
                        StageSpec.singleThreaded()
                    )
                    .stage(
                        "validate",
                        ReusableOrder.class,
                        ReusableOrder.class,
                        (order, out, ctx) -> {
                            if (order.valid) {
                                out.push(order);
                            }
                        },
                        StageSpec.singleThreaded()
                    )
                    .sink("egress", ReusableOrder.class, order -> {
                        consumed.incrementAndGet();
                        endToEndLatency.recordElapsedSince(order.emittedAtNanos);
                        order.consumedSequence = order.sequence;
                    }, StageSpec.singleThreaded())
                    .edge("ingress", "normalize", EdgeSpec.mpscRing(POOL_SIZE))
                    .edge("normalize", "risk", EdgeSpec.spscRing(POOL_SIZE))
                    .edge("risk", "validate", EdgeSpec.spscRing(POOL_SIZE))
                    .edge("validate", "egress", EdgeSpec.spscRing(POOL_SIZE))
                    .build();
            } finally {
                if (previousFusion == null) {
                    System.clearProperty("lattice.fusion.enabled");
                } else {
                    System.setProperty("lattice.fusion.enabled", previousFusion);
                }
            }
            graph.start();
            emitter = graph.emitter("ingress", ReusableOrder.class);
        }

        ReusableOrder claim(final long emittedAtNanos) {
            final long sequence = next++;
            final ReusableOrder order = orders[(int) sequence & (POOL_SIZE - 1)];
            final long requiredConsumedSequence = sequence - POOL_SIZE;
            while (requiredConsumedSequence >= 0L && order.consumedSequence < requiredConsumedSequence) {
                Thread.onSpinWait();
            }
            order.sequence = sequence;
            order.id = sequence;
            order.valid = true;
            order.emittedAtNanos = emittedAtNanos;
            return order;
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("oneSourceThreeStageSinkPreallocatedFused", "enqueue");
            endToEndLatency.print("oneSourceThreeStageSinkPreallocatedFused", "endToEnd");
        }

        private static ReusableOrder[] preallocatedOrders() {
            final ReusableOrder[] orders = new ReusableOrder[POOL_SIZE];
            for (int i = 0; i < orders.length; i++) {
                orders[i] = new ReusableOrder();
            }
            return orders;
        }
    }

    public static final class PreallocatedSignal {
        long sequence;
        long emittedAtNanos;
        volatile long consumedSequence = Long.MIN_VALUE;
    }

    public static final class ReusableOrder {
        long sequence;
        long id;
        boolean valid;
        long emittedAtNanos;
        volatile long consumedSequence = Long.MIN_VALUE;
    }

    public record Order(long id, boolean valid, long emittedAtNanos) {
    }

    public record ValidOrder(long id, long emittedAtNanos) {
    }
}
