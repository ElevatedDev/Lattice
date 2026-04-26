package com.lattice.benchmark;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.PreallocationSpec;
import com.lattice.graph.SourceMode;
import com.lattice.graph.StaticGraph;
import com.lattice.routing.BroadcastSpec;
import com.lattice.routing.JoinSpec;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.Emitter;
import com.lattice.stage.PreallocatedEmitter;
import com.lattice.stage.StageSpec;
import com.lattice.wait.WaitSpec;
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
public class ApplesToApplesDisruptorBenchmark {

    private static final int RING_SIZE = 8192;
    private static final int POOL_SIZE = 1 << 18;
    private static final int BATCH_SIZE = 1024;
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final WaitSpec WAIT = WaitSpec.phased(100, 1_000_000_000, Duration.ZERO);
    private static final StageSpec STAGE = StageSpec.singleThreaded()
        .wait(WAIT);
    private static final EdgeSpec SPSC = EdgeSpec.spscRing(RING_SIZE)
        .wait(WAIT)
        .batch(BatchPolicy.maxItems(BATCH_SIZE));
    private static final EdgeSpec SPSC_FUSIBLE = EdgeSpec.spscRing(RING_SIZE)
        .wait(WAIT);
    private static final EdgeSpec MPSC = EdgeSpec.mpscRing(RING_SIZE)
        .wait(WAIT)
        .batch(BatchPolicy.maxItems(BATCH_SIZE));

    @Benchmark
    public void latticeSpscPreallocated(final LatticeSpscPreallocatedState state) {
        final Signal signal = state.emitter.claim();
        signal.id = state.next++;
        signal.value = signal.id;
        state.emitter.emit(signal);
    }

    @Benchmark
    public void disruptorSpscPreallocated(final DisruptorSpscState state) {
        final long value = state.next++;
        final long sequence = state.ringBuffer.next();
        try {
            final ValueEvent event = state.ringBuffer.get(sequence);
            event.id = value;
            event.value = value;
        } finally {
            state.ringBuffer.publish(sequence);
        }
    }

    @Benchmark
    @Group("latticeMpscReference")
    @GroupThreads(4)
    public void latticeMpscReference(final LatticeMpscState state, final ProducerPool producer) {
        final Signal signal = producer.nextSignal();
        state.emitter.emit(signal);
    }

    @Benchmark
    @Group("disruptorMpscReference")
    @GroupThreads(4)
    public void disruptorMpscReference(final DisruptorMpscReferenceState state, final ProducerPool producer) {
        final Signal signal = producer.nextSignal();
        final long sequence = state.ringBuffer.next();
        try {
            final RefEvent event = state.ringBuffer.get(sequence);
            event.signal = signal;
        } finally {
            state.ringBuffer.publish(sequence);
        }
    }

    @Benchmark
    @Group("disruptorMpscCopy")
    @GroupThreads(4)
    public void disruptorMpscCopy(final DisruptorMpscCopyState state, final ProducerPool producer) {
        final Signal signal = producer.nextSignal();
        final long sequence = state.ringBuffer.next();
        try {
            final ValueEvent event = state.ringBuffer.get(sequence);
            event.id = signal.id;
            event.value = signal.value;
        } finally {
            state.ringBuffer.publish(sequence);
        }
    }

    @Benchmark
    public void latticeThreeStagePipelinePhysical(final LatticePipelinePhysicalState state) {
        state.emitter.emit(state.nextSignal());
    }

    @Benchmark
    public void latticeThreeStagePipelineFused(final LatticePipelineFusedState state) {
        state.emitter.emit(state.nextSignal());
    }

    @Benchmark
    public void disruptorThreeStagePipeline(final DisruptorPipelineState state) {
        final Signal signal = state.nextSignal();
        final long sequence = state.ringBuffer.next();
        try {
            final ValueEvent event = state.ringBuffer.get(sequence);
            event.id = signal.id;
            event.value = signal.value;
        } finally {
            state.ringBuffer.publish(sequence);
        }
    }

    @Benchmark
    public void latticeDependencyJoin(final LatticeDependencyState state) {
        state.emitter.emit(state.nextSignal());
    }

    @Benchmark
    public void disruptorDependencyGraph(final DisruptorDependencyState state) {
        final ImmutableSignal signal = state.nextSignal();
        final long sequence = state.ringBuffer.next();
        try {
            final ValueEvent event = state.ringBuffer.get(sequence);
            event.id = signal.id();
            event.value = signal.value();
        } finally {
            state.ringBuffer.publish(sequence);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeSpscPreallocatedState {
        final AtomicLong consumed = new AtomicLong();
        StaticGraph graph;
        PreallocatedEmitter<Signal> emitter;
        long next;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("aa-lattice-spsc-preallocated")
                .preallocatedSource("ingress", Signal.class, PreallocationSpec.pool(ignored -> new Signal())
                    .poolSize(RING_SIZE << 1))
                .sink("egress", Signal.class, signal -> consumed.incrementAndGet(), STAGE)
                .edge("ingress", "egress", EdgeSpec.spscRing(RING_SIZE).wait(WAIT))
                .build();
            graph.start();
            emitter = graph.preallocatedEmitter("ingress", Signal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireConsumed("latticeSpscPreallocated", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorSpscState {
        final AtomicLong consumed = new AtomicLong();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;
        long next;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("aa-disruptor-spsc", ProducerType.SINGLE);
            disruptor.handleEventsWith((event, sequence, endOfBatch) -> consumed.incrementAndGet());
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorSpscPreallocated", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeMpscState {
        final AtomicLong consumed = new AtomicLong();
        StaticGraph graph;
        Emitter<Signal> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("aa-lattice-mpsc-reference")
                .source("ingress", Signal.class, SourceMode.MULTI_PRODUCER)
                .sink("egress", Signal.class, signal -> consumed.incrementAndGet(), STAGE)
                .edge("ingress", "egress", MPSC)
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Signal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireConsumed("latticeMpscReference", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorMpscReferenceState {
        final AtomicLong consumed = new AtomicLong();
        Disruptor<RefEvent> disruptor;
        RingBuffer<RefEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptorRef("aa-disruptor-mpsc-ref", ProducerType.MULTI);
            disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                consumed.incrementAndGet();
                event.signal = null;
            });
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorMpscReference", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorMpscCopyState {
        final AtomicLong consumed = new AtomicLong();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("aa-disruptor-mpsc-copy", ProducerType.MULTI);
            disruptor.handleEventsWith((event, sequence, endOfBatch) -> consumed.incrementAndGet());
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorMpscCopy", consumed);
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

    public abstract static class LatticePipelineState extends PooledSignals {
        final AtomicLong consumed = new AtomicLong();
        StaticGraph graph;
        Emitter<Signal> emitter;

        abstract void setup();

        void setup(final boolean fused) {
            final String previousFusion = System.getProperty("lattice.fusion.enabled");
            setFusion(fused);
            try {
                graph = StaticGraph.builder("aa-lattice-pipeline-" + (fused ? "fused" : "physical"))
                    .source("ingress", Signal.class, SourceMode.SINGLE_PRODUCER)
                    .stage("normalize", Signal.class, Signal.class, ApplesToApplesDisruptorBenchmark::increment, STAGE)
                    .stage("risk", Signal.class, Signal.class, ApplesToApplesDisruptorBenchmark::increment, STAGE)
                    .stage("validate", Signal.class, Signal.class, ApplesToApplesDisruptorBenchmark::increment, STAGE)
                    .sink("egress", Signal.class, signal -> consumed.incrementAndGet(), STAGE)
                    .edge("ingress", "normalize", SPSC_FUSIBLE)
                    .edge("normalize", "risk", SPSC_FUSIBLE)
                    .edge("risk", "validate", SPSC_FUSIBLE)
                    .edge("validate", "egress", SPSC_FUSIBLE)
                    .build();
            } finally {
                restoreFusion(previousFusion);
            }
            graph.start();
            emitter = graph.emitter("ingress", Signal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireConsumed("latticeThreeStagePipeline", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorPipelineState extends PooledSignals {
        final AtomicLong consumed = new AtomicLong();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("aa-disruptor-pipeline", ProducerType.SINGLE);
            disruptor.handleEventsWith(incrementHandler())
                .then(incrementHandler())
                .then(incrementHandler())
                .then((event, sequence, endOfBatch) -> consumed.incrementAndGet());
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorThreeStagePipeline", consumed);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeDependencyState extends PooledImmutableSignals {
        final AtomicLong committed = new AtomicLong();
        StaticGraph graph;
        Emitter<ImmutableSignal> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("aa-lattice-dependency-join")
                .source("ingress", ImmutableSignal.class, SourceMode.SINGLE_PRODUCER)
                .stage("validate", ImmutableSignal.class, ImmutableSignal.class, ApplesToApplesDisruptorBenchmark::pass, STAGE)
                .broadcast("fanout", ImmutableSignal.class, BroadcastSpec.copy(), STAGE)
                .stage("journal", ImmutableSignal.class, ImmutableSignal.class, ApplesToApplesDisruptorBenchmark::pass, STAGE)
                .stage("risk", ImmutableSignal.class, ImmutableSignal.class, ApplesToApplesDisruptorBenchmark::pass, STAGE)
                .join("join", ImmutableSignal.class, JoinSpec.<ImmutableSignal>allOf(group ->
                    (ImmutableSignal) group.valuesBySource().get("journal"))
                    .stampLong(item -> ((ImmutableSignal) item).id())
                    .capacity(RING_SIZE), STAGE)
                .sink("commit", ImmutableSignal.class, signal -> committed.incrementAndGet(), STAGE)
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
    public static class DisruptorDependencyState extends PooledImmutableSignals {
        final AtomicLong committed = new AtomicLong();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("aa-disruptor-dependency", ProducerType.SINGLE);
            final EventHandlerGroup<ValueEvent> validate = disruptor.handleEventsWith(noopHandler());
            validate.then(noopHandler(), noopHandler())
                .then((event, sequence, endOfBatch) -> committed.incrementAndGet());
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireConsumed("disruptorDependencyGraph", committed);
        }
    }

    @State(Scope.Thread)
    public static class ProducerPool extends PooledSignals {
    }

    public static class PooledSignals {
        private final Signal[] signals = new Signal[POOL_SIZE];
        private int cursor;

        public PooledSignals() {
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

    public static class PooledImmutableSignals {
        private final ImmutableSignal[] signals = new ImmutableSignal[POOL_SIZE];
        private int cursor;

        public PooledImmutableSignals() {
            for (int i = 0; i < signals.length; i++) {
                signals[i] = new ImmutableSignal(i, i);
            }
        }

        ImmutableSignal nextSignal() {
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

    public static final class RefEvent {
        Signal signal;
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

    private static Disruptor<RefEvent> disruptorRef(final String threadName, final ProducerType producerType) {
        final EventFactory<RefEvent> factory = RefEvent::new;
        return new Disruptor<>(
            factory,
            RING_SIZE,
            namedThreadFactory(threadName),
            producerType,
            new YieldingWaitStrategy()
        );
    }

    private static void requireConsumed(final String benchmark, final AtomicLong consumed) {
        if (consumed.get() == 0L) {
            throw new IllegalStateException(benchmark + " did not consume any events");
        }
    }

    private static ThreadFactory namedThreadFactory(final String name) {
        return runnable -> {
            final Thread thread = Thread.ofPlatform().unstarted(runnable);
            thread.setName(name);
            thread.setDaemon(true);
            return thread;
        };
    }

    private static void increment(final Signal signal, final com.lattice.stage.Output<Signal> out, final Object ctx) {
        signal.value++;
        out.push(signal);
    }

    private static void pass(
        final ImmutableSignal signal,
        final com.lattice.stage.Output<ImmutableSignal> out,
        final Object ctx
    ) {
        out.push(signal);
    }

    private static EventHandler<ValueEvent> incrementHandler() {
        return (event, sequence, endOfBatch) -> event.value++;
    }

    private static EventHandler<ValueEvent> noopHandler() {
        return (event, sequence, endOfBatch) -> {
        };
    }

    private static void setFusion(final boolean fused) {
        if (fused) {
            System.setProperty("lattice.fusion.enabled", "true");
        } else {
            System.clearProperty("lattice.fusion.enabled");
        }
    }

    private static void restoreFusion(final String previousFusion) {
        if (previousFusion == null) {
            System.clearProperty("lattice.fusion.enabled");
        } else {
            System.setProperty("lattice.fusion.enabled", previousFusion);
        }
    }
}
