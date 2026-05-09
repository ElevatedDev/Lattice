package io.github.elevateddev.lattice.benchmark;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.FusionSpec;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.routing.BroadcastSpec;
import io.github.elevateddev.lattice.routing.JoinSpec;
import io.github.elevateddev.lattice.routing.PartitionSpec;
import io.github.elevateddev.lattice.stage.BatchPolicy;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.Output;
import io.github.elevateddev.lattice.stage.StageSpec;
import io.github.elevateddev.lattice.wait.WaitSpec;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
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
public class TopologyShapeDisruptorBenchmark {

    private static final int RING_SIZE = 8192;
    private static final int POOL_SIZE = 1 << 18;
    private static final int BATCH_SIZE = 1024;
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final WaitSpec WAIT = WaitSpec.phased(100, 1_000_000_000, Duration.ZERO);
    private static final StageSpec STAGE = StageSpec.singleThreaded()
        .wait(WAIT);
    private static final EdgeSpec MPSC = EdgeSpec.mpscRing(RING_SIZE)
        .wait(WAIT)
        .batch(BatchPolicy.maxItems(BATCH_SIZE));
    private static final EdgeSpec SPSC = EdgeSpec.spscRing(RING_SIZE)
        .wait(WAIT)
        .batch(BatchPolicy.maxItems(BATCH_SIZE));
    private static final EdgeSpec SPSC_FUSIBLE = EdgeSpec.spscRing(RING_SIZE)
        .wait(WAIT);

    @Benchmark
    public void latticeSourceSinkSpsc(final LatticeSourceSinkState state) {
        state.emitter.emit(state.nextSignal());
    }

    @Benchmark
    public void disruptorSourceSinkSpsc(final DisruptorSourceSinkState state) {
        publish(state.ringBuffer, state.nextSignal());
    }

    @Benchmark
    @Group("latticeMpsc2Producer")
    @GroupThreads(2)
    public void latticeMpsc2Producer(final LatticeMpscState state, final ProducerPool producer) {
        state.emitter.emit(producer.nextSignal());
    }

    @Benchmark
    @Group("disruptorMpsc2Producer")
    @GroupThreads(2)
    public void disruptorMpsc2Producer(final DisruptorMpscState state, final ProducerPool producer) {
        publish(state.ringBuffer, producer.nextSignal());
    }

    @Benchmark
    @Group("latticeMpsc4Producer")
    @GroupThreads(4)
    public void latticeMpsc4Producer(final LatticeMpscState state, final ProducerPool producer) {
        state.emitter.emit(producer.nextSignal());
    }

    @Benchmark
    @Group("disruptorMpsc4Producer")
    @GroupThreads(4)
    public void disruptorMpsc4Producer(final DisruptorMpscState state, final ProducerPool producer) {
        publish(state.ringBuffer, producer.nextSignal());
    }

    @Benchmark
    @Group("latticeMpsc8Producer")
    @GroupThreads(8)
    public void latticeMpsc8Producer(final LatticeMpscState state, final ProducerPool producer) {
        state.emitter.emit(producer.nextSignal());
    }

    @Benchmark
    @Group("disruptorMpsc8Producer")
    @GroupThreads(8)
    public void disruptorMpsc8Producer(final DisruptorMpscState state, final ProducerPool producer) {
        publish(state.ringBuffer, producer.nextSignal());
    }

    @Benchmark
    public void latticePipelinePhysical(final LatticePipelinePhysicalState state) {
        state.emitter.emit(state.nextSignal());
    }

    @Benchmark
    public void latticePipelineFused(final LatticePipelineFusedState state) {
        state.emitter.emit(state.nextSignal());
    }

    @Benchmark
    public void disruptorPipelinePhysical(final DisruptorPipelinePhysicalState state) {
        publish(state.ringBuffer, state.nextSignal());
    }

    @Benchmark
    public void disruptorPipelineManuallyFused(final DisruptorPipelineFusedState state) {
        publish(state.ringBuffer, state.nextSignal());
    }

    @Benchmark
    public void latticeBroadcastTwoBranch(final LatticeBroadcastTwoBranchState state) {
        state.emitter.emit(state.nextImmutableSignal());
    }

    @Benchmark
    public void disruptorBroadcastTwoConsumer(final DisruptorBroadcastTwoConsumerState state) {
        publish(state.ringBuffer, state.nextImmutableSignal());
    }

    @Benchmark
    public void latticeBroadcastFourBranch(final LatticeBroadcastFourBranchState state) {
        state.emitter.emit(state.nextImmutableSignal());
    }

    @Benchmark
    public void disruptorBroadcastFourConsumer(final DisruptorBroadcastFourConsumerState state) {
        publish(state.ringBuffer, state.nextImmutableSignal());
    }

    @Benchmark
    public void latticePartitionFourLanes(final LatticePartitionFourLaneState state) {
        state.emitter.emit(state.nextImmutableSignal());
    }

    @Benchmark
    public void disruptorManualPartitionFourLanes(final DisruptorManualPartitionFourLaneState state) {
        state.publish(state.nextImmutableSignal());
    }

    @Benchmark
    public void latticeDependencyJoin(final LatticeDependencyJoinState state) {
        state.emitter.emit(state.nextImmutableSignal());
    }

    @Benchmark
    public void disruptorDependencyGraph(final DisruptorDependencyGraphState state) {
        publish(state.ringBuffer, state.nextImmutableSignal());
    }

    @State(Scope.Benchmark)
    public static class LatticeSourceSinkState extends SignalPool {
        final AtomicLong consumed = new AtomicLong();
        StaticGraph graph;
        Emitter<Signal> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("shape-lattice-source-sink-spsc")
                .source("ingress", Signal.class, SourceMode.SINGLE_PRODUCER)
                .sink("egress", Signal.class, ignored -> consumed.incrementAndGet(), STAGE)
                .edge("ingress", "egress", SPSC)
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Signal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireConsumed("latticeSourceSinkSpsc", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorSourceSinkState extends SignalPool {
        final AtomicLong consumed = new AtomicLong();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("shape-disruptor-source-sink-spsc", ProducerType.SINGLE);
            disruptor.handleEventsWith(countingHandler(consumed));
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorSourceSinkSpsc", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeMpscState {
        final AtomicLong consumed = new AtomicLong();
        StaticGraph graph;
        Emitter<Signal> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("shape-lattice-mpsc")
                .source("ingress", Signal.class, SourceMode.MULTI_PRODUCER)
                .sink("egress", Signal.class, ignored -> consumed.incrementAndGet(), STAGE)
                .edge("ingress", "egress", MPSC)
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Signal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireConsumed("latticeMpscScaling", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorMpscState {
        final AtomicLong consumed = new AtomicLong();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("shape-disruptor-mpsc", ProducerType.MULTI);
            disruptor.handleEventsWith(countingHandler(consumed));
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorMpscScaling", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticePipelinePhysicalState extends LatticePipelineState {
        @Override
        @Setup(Level.Trial)
        public void setup() {
            setup(false);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticePipelineFusedState extends LatticePipelineState {
        @Override
        @Setup(Level.Trial)
        public void setup() {
            setup(true);
        }
    }

    public abstract static class LatticePipelineState extends SignalPool {
        final AtomicLong consumed = new AtomicLong();
        StaticGraph graph;
        Emitter<Signal> emitter;

        abstract void setup();

        void setup(final boolean fused) {
            graph = StaticGraph.builder("shape-lattice-pipeline-" + (fused ? "fused" : "physical"))
                .fusion(fused ? FusionSpec.defaults() : FusionSpec.disabled())
                .source("ingress", Signal.class, SourceMode.SINGLE_PRODUCER)
                .stage("normalize", Signal.class, Signal.class, TopologyShapeDisruptorBenchmark::increment, STAGE)
                .stage("risk", Signal.class, Signal.class, TopologyShapeDisruptorBenchmark::increment, STAGE)
                .stage("validate", Signal.class, Signal.class, TopologyShapeDisruptorBenchmark::increment, STAGE)
                .sink("egress", Signal.class, ignored -> consumed.incrementAndGet(), STAGE)
                .edge("ingress", "normalize", SPSC_FUSIBLE)
                .edge("normalize", "risk", SPSC_FUSIBLE)
                .edge("risk", "validate", SPSC_FUSIBLE)
                .edge("validate", "egress", SPSC_FUSIBLE)
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Signal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireConsumed("latticePipeline", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorPipelinePhysicalState extends SignalPool {
        final AtomicLong consumed = new AtomicLong();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("shape-disruptor-pipeline-physical", ProducerType.SINGLE);
            disruptor.handleEventsWith(incrementHandler())
                .then(incrementHandler())
                .then(incrementHandler())
                .then(countingHandler(consumed));
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorPipelinePhysical", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorPipelineFusedState extends SignalPool {
        final AtomicLong consumed = new AtomicLong();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("shape-disruptor-pipeline-fused", ProducerType.SINGLE);
            disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                event.value++;
                event.value++;
                event.value++;
                consumed.incrementAndGet();
            });
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorPipelineManuallyFused", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeBroadcastTwoBranchState extends ImmutableSignalPool {
        final AtomicLong consumed = new AtomicLong();
        StaticGraph graph;
        Emitter<ImmutableSignal> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = broadcastGraph("shape-lattice-broadcast-two", 2, consumed);
            graph.start();
            emitter = graph.emitter("ingress", ImmutableSignal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireConsumed("latticeBroadcastTwoBranch", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeBroadcastFourBranchState extends ImmutableSignalPool {
        final AtomicLong consumed = new AtomicLong();
        StaticGraph graph;
        Emitter<ImmutableSignal> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = broadcastGraph("shape-lattice-broadcast-four", 4, consumed);
            graph.start();
            emitter = graph.emitter("ingress", ImmutableSignal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireConsumed("latticeBroadcastFourBranch", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorBroadcastTwoConsumerState extends ImmutableSignalPool {
        final AtomicLong firstConsumed = new AtomicLong();
        final AtomicLong secondConsumed = new AtomicLong();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("shape-disruptor-broadcast-two", ProducerType.SINGLE);
            disruptor.handleEventsWith(countingHandler(firstConsumed), countingHandler(secondConsumed));
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorBroadcastTwoConsumer", firstConsumed);
            requireConsumed("disruptorBroadcastTwoConsumer", secondConsumed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorBroadcastFourConsumerState extends ImmutableSignalPool {
        final AtomicLong firstConsumed = new AtomicLong();
        final AtomicLong secondConsumed = new AtomicLong();
        final AtomicLong thirdConsumed = new AtomicLong();
        final AtomicLong fourthConsumed = new AtomicLong();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("shape-disruptor-broadcast-four", ProducerType.SINGLE);
            disruptor.handleEventsWith(
                countingHandler(firstConsumed),
                countingHandler(secondConsumed),
                countingHandler(thirdConsumed),
                countingHandler(fourthConsumed)
            );
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorBroadcastFourConsumer", firstConsumed);
            requireConsumed("disruptorBroadcastFourConsumer", secondConsumed);
            requireConsumed("disruptorBroadcastFourConsumer", thirdConsumed);
            requireConsumed("disruptorBroadcastFourConsumer", fourthConsumed);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticePartitionFourLaneState extends ImmutableSignalPool {
        final AtomicLong consumed = new AtomicLong();
        StaticGraph graph;
        Emitter<ImmutableSignal> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("shape-lattice-partition-four")
                .source("ingress", ImmutableSignal.class, SourceMode.SINGLE_PRODUCER)
                .partition("partition", ImmutableSignal.class, PartitionSpec.byKey(ImmutableSignal::id, 4), STAGE)
                .sink("lane0", ImmutableSignal.class, ignored -> consumed.incrementAndGet(), STAGE)
                .sink("lane1", ImmutableSignal.class, ignored -> consumed.incrementAndGet(), STAGE)
                .sink("lane2", ImmutableSignal.class, ignored -> consumed.incrementAndGet(), STAGE)
                .sink("lane3", ImmutableSignal.class, ignored -> consumed.incrementAndGet(), STAGE)
                .edge("ingress", "partition", SPSC)
                .edge("partition", "lane0", SPSC)
                .edge("partition", "lane1", SPSC)
                .edge("partition", "lane2", SPSC)
                .edge("partition", "lane3", SPSC)
                .build();
            graph.start();
            emitter = graph.emitter("ingress", ImmutableSignal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireConsumed("latticePartitionFourLanes", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorManualPartitionFourLaneState extends ImmutableSignalPool {
        final AtomicLong consumed = new AtomicLong();
        Disruptor<ValueEvent> lane0;
        Disruptor<ValueEvent> lane1;
        Disruptor<ValueEvent> lane2;
        Disruptor<ValueEvent> lane3;
        RingBuffer<ValueEvent> ring0;
        RingBuffer<ValueEvent> ring1;
        RingBuffer<ValueEvent> ring2;
        RingBuffer<ValueEvent> ring3;

        @Setup(Level.Trial)
        public void setup() {
            lane0 = partitionLane("shape-disruptor-partition-lane0", consumed);
            lane1 = partitionLane("shape-disruptor-partition-lane1", consumed);
            lane2 = partitionLane("shape-disruptor-partition-lane2", consumed);
            lane3 = partitionLane("shape-disruptor-partition-lane3", consumed);
            ring0 = lane0.start();
            ring1 = lane1.start();
            ring2 = lane2.start();
            ring3 = lane3.start();
        }

        void publish(final ImmutableSignal signal) {
            switch ((int) (signal.id() & 3L)) {
                case 0 -> TopologyShapeDisruptorBenchmark.publish(ring0, signal);
                case 1 -> TopologyShapeDisruptorBenchmark.publish(ring1, signal);
                case 2 -> TopologyShapeDisruptorBenchmark.publish(ring2, signal);
                default -> TopologyShapeDisruptorBenchmark.publish(ring3, signal);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            lane0.shutdown();
            lane1.shutdown();
            lane2.shutdown();
            lane3.shutdown();
            requireConsumed("disruptorManualPartitionFourLanes", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeDependencyJoinState extends ImmutableSignalPool {
        final AtomicLong committed = new AtomicLong();
        StaticGraph graph;
        Emitter<ImmutableSignal> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("shape-lattice-dependency-join")
                .source("ingress", ImmutableSignal.class, SourceMode.SINGLE_PRODUCER)
                .stage("validate", ImmutableSignal.class, ImmutableSignal.class, TopologyShapeDisruptorBenchmark::pass, STAGE)
                .broadcast("fanout", ImmutableSignal.class, BroadcastSpec.copy(signal -> signal), STAGE)
                .stage("journal", ImmutableSignal.class, ImmutableSignal.class, TopologyShapeDisruptorBenchmark::pass, STAGE)
                .stage("risk", ImmutableSignal.class, ImmutableSignal.class, TopologyShapeDisruptorBenchmark::pass, STAGE)
                .join("join", ImmutableSignal.class, JoinSpec.<ImmutableSignal>allOf(group ->
                    (ImmutableSignal) group.valuesBySource().get("journal"))
                    .stampLong(item -> ((ImmutableSignal) item).id())
                    .capacity(RING_SIZE), STAGE)
                .sink("commit", ImmutableSignal.class, ignored -> committed.incrementAndGet(), STAGE)
                .edge("ingress", "validate", SPSC)
                .edge("validate", "fanout", SPSC)
                .edge("fanout", "journal", SPSC)
                .edge("fanout", "risk", SPSC)
                .edge("journal", "join", SPSC)
                .edge("risk", "join", SPSC)
                .edge("join", "commit", SPSC)
                .build();
            graph.start();
            emitter = graph.emitter("ingress", ImmutableSignal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireConsumed("latticeDependencyJoin", committed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorDependencyGraphState extends ImmutableSignalPool {
        final AtomicLong committed = new AtomicLong();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("shape-disruptor-dependency", ProducerType.SINGLE);
            final EventHandlerGroup<ValueEvent> validate = disruptor.handleEventsWith(noopHandler());
            validate
                .then(noopHandler(), noopHandler())
                .then(countingHandler(committed));
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorDependencyGraph", committed);
        }
    }

    @State(Scope.Thread)
    public static class ProducerPool extends SignalPool {
    }

    public static class SignalPool {
        private final Signal[] signals = new Signal[POOL_SIZE];
        private int cursor;

        public SignalPool() {
            for (int i = 0; i < signals.length; i++) {
                final Signal signal = new Signal();
                signal.id = i;
                signal.value = i;
                signals[i] = signal;
            }
        }

        Signal nextSignal() {
            final Signal signal = signals[cursor++ & (signals.length - 1)];
            signal.value = signal.id;
            return signal;
        }
    }

    public static class ImmutableSignalPool {
        private final ImmutableSignal[] signals = new ImmutableSignal[POOL_SIZE];
        private int cursor;

        public ImmutableSignalPool() {
            for (int i = 0; i < signals.length; i++) {
                signals[i] = new ImmutableSignal(i, i);
            }
        }

        ImmutableSignal nextImmutableSignal() {
            return signals[cursor++ & (signals.length - 1)];
        }
    }

    public static final class Signal {
        long id;
        long value;
    }

    public record ImmutableSignal(long id, long value) {
    }

    public static final class ValueEvent {
        long id;
        long value;
    }

    private static StaticGraph broadcastGraph(
        final String name,
        final int branches,
        final AtomicLong consumed
    ) {
        final StaticGraph.Builder builder = StaticGraph.builder(name)
            .source("ingress", ImmutableSignal.class, SourceMode.SINGLE_PRODUCER)
            .broadcast("fanout", ImmutableSignal.class, BroadcastSpec.copy(signal -> signal), STAGE)
            .edge("ingress", "fanout", SPSC);
        for (int i = 0; i < branches; i++) {
            final String lane = "lane" + i;
            builder
                .sink(lane, ImmutableSignal.class, ignored -> consumed.incrementAndGet(), STAGE)
                .edge("fanout", lane, SPSC);
        }
        return builder.build();
    }

    private static Disruptor<ValueEvent> partitionLane(final String threadName, final AtomicLong consumed) {
        final Disruptor<ValueEvent> disruptor = disruptor(threadName, ProducerType.SINGLE);
        disruptor.handleEventsWith(countingHandler(consumed));
        return disruptor;
    }

    private static Disruptor<ValueEvent> disruptor(final String threadName, final ProducerType producerType) {
        final EventFactory<ValueEvent> factory = ValueEvent::new;
        return new Disruptor<>(
            factory,
            RING_SIZE,
            namedThreadFactory(threadName),
            producerType,
            new YieldingWaitStrategy()
        );
    }

    private static void publish(final RingBuffer<ValueEvent> ringBuffer, final Signal signal) {
        final long sequence = ringBuffer.next();
        try {
            final ValueEvent event = ringBuffer.get(sequence);
            event.id = signal.id;
            event.value = signal.value;
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    private static void publish(final RingBuffer<ValueEvent> ringBuffer, final ImmutableSignal signal) {
        final long sequence = ringBuffer.next();
        try {
            final ValueEvent event = ringBuffer.get(sequence);
            event.id = signal.id();
            event.value = signal.value();
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    private static EventHandler<ValueEvent> countingHandler(final AtomicLong counter) {
        return (event, sequence, endOfBatch) -> counter.incrementAndGet();
    }

    private static EventHandler<ValueEvent> incrementHandler() {
        return (event, sequence, endOfBatch) -> event.value++;
    }

    private static EventHandler<ValueEvent> noopHandler() {
        return (event, sequence, endOfBatch) -> {
        };
    }

    private static void increment(final Signal signal, final Output<Signal> out, final Object ctx) {
        signal.value++;
        out.push(signal);
    }

    private static void pass(final ImmutableSignal signal, final Output<ImmutableSignal> out, final Object ctx) {
        out.push(signal);
    }

    private static void requireConsumed(final String benchmark, final AtomicLong consumed) {
        if (consumed.get() == 0L) {
            throw new IllegalStateException(benchmark + " did not consume any events");
        }
    }

    private static ThreadFactory namedThreadFactory(final String name) {
        return runnable -> {
            final Thread thread = new Thread(runnable);
            thread.setName(name);
            thread.setDaemon(true);
            return thread;
        };
    }

}
