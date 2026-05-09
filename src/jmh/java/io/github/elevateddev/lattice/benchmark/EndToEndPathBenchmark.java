package io.github.elevateddev.lattice.benchmark;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.FusionSpec;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.routing.BroadcastSpec;
import io.github.elevateddev.lattice.routing.JoinSpec;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.Output;
import io.github.elevateddev.lattice.stage.StageSpec;
import io.github.elevateddev.lattice.wait.WaitSpec;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
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
public class EndToEndPathBenchmark {

    private static final int RING_SIZE = 8192;
    private static final int POOL_SIZE = 1 << 18;
    private static final int SIDE_TABLE_SIZE = 1024;
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final WaitSpec WAIT = WaitSpec.busySpin();
    private static final StageSpec STAGE = StageSpec.singleThreaded().wait(WAIT);
    private static final EdgeSpec SPSC = EdgeSpec.spscRing(RING_SIZE).wait(WAIT);
    private static final long[] SIDE_TABLE = sideTable();

    @Benchmark
    public long latticeSourceSinkCompleted(final LatticeSourceSinkState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.raw;
    }

    @Benchmark
    public long disruptorSourceSinkCompleted(final DisruptorSourceSinkState state) {
        final Order order = state.nextOrder();
        publish(state.ringBuffer, order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.raw;
    }

    @Benchmark
    public long latticePipelinePhysicalCompleted(final LatticePipelinePhysicalState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticePipelineInlineFusedCompleted(final LatticePipelineInlineFusedState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long disruptorPipelinePhysicalCompleted(final DisruptorPipelinePhysicalState state) {
        final Order order = state.nextOrder();
        publish(state.ringBuffer, order);
        awaitCompleted(state.completedSequence, order.sequence);
        return state.completedChecksum;
    }

    @Benchmark
    public long disruptorPipelineManuallyFusedCompleted(final DisruptorPipelineFusedState state) {
        final Order order = state.nextOrder();
        publish(state.ringBuffer, order);
        awaitCompleted(state.completedSequence, order.sequence);
        return state.completedChecksum;
    }

    @Benchmark
    public long latticeBroadcastTwoBranchCompleted(final LatticeBroadcastTwoBranchState state) {
        final RouteSignal signal = state.nextSignal();
        state.emitter.emit(signal);
        awaitCompleted(state.firstCompletedSequence, signal.sequence);
        awaitCompleted(state.secondCompletedSequence, signal.sequence);
        return signal.value;
    }

    @Benchmark
    public long disruptorBroadcastTwoConsumerCompleted(final DisruptorBroadcastTwoConsumerState state) {
        final RouteSignal signal = state.nextSignal();
        publish(state.ringBuffer, signal);
        awaitCompleted(state.firstCompletedSequence, signal.sequence);
        awaitCompleted(state.secondCompletedSequence, signal.sequence);
        return signal.value;
    }

    @Benchmark
    public long latticeDependencyJoinCompleted(final LatticeDependencyJoinState state) {
        final RouteSignal signal = state.nextSignal();
        state.emitter.emit(signal);
        awaitCompleted(state.completedSequence, signal.sequence);
        return signal.value;
    }

    @Benchmark
    public long disruptorDependencyGraphCompleted(final DisruptorDependencyGraphState state) {
        final RouteSignal signal = state.nextSignal();
        publish(state.ringBuffer, signal);
        awaitCompleted(state.completedSequence, signal.sequence);
        return signal.value;
    }

    @State(Scope.Benchmark)
    public static class LatticeSourceSinkState extends OrderPool {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        StaticGraph graph;
        Emitter<Order> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("e2e-lattice-source-sink")
                .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
                .sink("egress", Order.class, order -> completedSequence.lazySet(order.sequence), STAGE)
                .edge("ingress", "egress", SPSC)
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Order.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireCompleted("latticeSourceSinkCompleted", completedSequence);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorSourceSinkState extends OrderPool {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        Disruptor<OrderEvent> disruptor;
        RingBuffer<OrderEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = orderDisruptor("e2e-disruptor-source-sink");
            disruptor.handleEventsWith((event, sequence, endOfBatch) -> completedSequence.lazySet(event.sequence));
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireCompleted("disruptorSourceSinkCompleted", completedSequence);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticePipelinePhysicalState extends LatticePipelineState {
        @Override
        @Setup(Level.Trial)
        public void setup() {
            setup(false, false, "e2e-lattice-pipeline-physical");
        }
    }

    @State(Scope.Benchmark)
    public static class LatticePipelineInlineFusedState extends LatticePipelineState {
        @Override
        @Setup(Level.Trial)
        public void setup() {
            setup(true, true, "e2e-lattice-pipeline-inline-fused");
        }
    }

    public abstract static class LatticePipelineState extends OrderPool {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        StaticGraph graph;
        Emitter<Order> emitter;

        abstract void setup();

        void setup(final boolean fusionEnabled, final boolean inlineSourceFusion, final String graphName) {
            graph = StaticGraph.builder(graphName)
                .fusion(fusionEnabled ? FusionSpec.defaults().inlineSources(inlineSourceFusion) : FusionSpec.disabled())
                .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
                .stage("parse", Order.class, Order.class, EndToEndPathBenchmark::parse, STAGE)
                .stage("enrich", Order.class, Order.class, EndToEndPathBenchmark::enrich, STAGE)
                .stage("risk", Order.class, Order.class, EndToEndPathBenchmark::risk, STAGE)
                .sink("commit", Order.class, order -> {
                    serialize(order);
                    completedSequence.lazySet(order.sequence);
                }, STAGE)
                .edge("ingress", "parse", SPSC)
                .edge("parse", "enrich", SPSC)
                .edge("enrich", "risk", SPSC)
                .edge("risk", "commit", SPSC)
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Order.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireCompleted("latticePipelineCompleted", completedSequence);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorPipelinePhysicalState extends OrderPool {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        volatile long completedChecksum;
        Disruptor<OrderEvent> disruptor;
        RingBuffer<OrderEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = orderDisruptor("e2e-disruptor-pipeline-physical");
            disruptor.handleEventsWith(parseHandler())
                .then(enrichHandler())
                .then(riskHandler())
                .then((event, sequence, endOfBatch) -> {
                    serialize(event);
                    completedChecksum = event.checksum;
                    completedSequence.lazySet(event.sequence);
                });
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireCompleted("disruptorPipelinePhysicalCompleted", completedSequence);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorPipelineFusedState extends OrderPool {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        volatile long completedChecksum;
        Disruptor<OrderEvent> disruptor;
        RingBuffer<OrderEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = orderDisruptor("e2e-disruptor-pipeline-fused");
            disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                parse(event);
                enrich(event);
                risk(event);
                serialize(event);
                completedChecksum = event.checksum;
                completedSequence.lazySet(event.sequence);
            });
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireCompleted("disruptorPipelineManuallyFusedCompleted", completedSequence);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeBroadcastTwoBranchState extends RouteSignalPool {
        final AtomicLong firstCompletedSequence = new AtomicLong(-1L);
        final AtomicLong secondCompletedSequence = new AtomicLong(-1L);
        StaticGraph graph;
        Emitter<RouteSignal> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("e2e-lattice-broadcast-two")
                .source("ingress", RouteSignal.class, SourceMode.SINGLE_PRODUCER)
                .broadcast("fanout", RouteSignal.class, BroadcastSpec.copy(signal -> signal), STAGE)
                .sink("journal", RouteSignal.class,
                    signal -> firstCompletedSequence.lazySet(signal.sequence), STAGE)
                .sink("risk", RouteSignal.class,
                    signal -> secondCompletedSequence.lazySet(signal.sequence), STAGE)
                .edge("ingress", "fanout", SPSC)
                .edge("fanout", "journal", SPSC)
                .edge("fanout", "risk", SPSC)
                .build();
            graph.start();
            emitter = graph.emitter("ingress", RouteSignal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireCompleted("latticeBroadcastTwoBranchCompleted", firstCompletedSequence);
            requireCompleted("latticeBroadcastTwoBranchCompleted", secondCompletedSequence);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorBroadcastTwoConsumerState extends RouteSignalPool {
        final AtomicLong firstCompletedSequence = new AtomicLong(-1L);
        final AtomicLong secondCompletedSequence = new AtomicLong(-1L);
        Disruptor<SignalEvent> disruptor;
        RingBuffer<SignalEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = signalDisruptor("e2e-disruptor-broadcast-two");
            disruptor.handleEventsWith(
                (event, sequence, endOfBatch) -> firstCompletedSequence.lazySet(event.sequence),
                (event, sequence, endOfBatch) -> secondCompletedSequence.lazySet(event.sequence)
            );
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireCompleted("disruptorBroadcastTwoConsumerCompleted", firstCompletedSequence);
            requireCompleted("disruptorBroadcastTwoConsumerCompleted", secondCompletedSequence);
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeDependencyJoinState extends RouteSignalPool {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        StaticGraph graph;
        Emitter<RouteSignal> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("e2e-lattice-dependency-join")
                .source("ingress", RouteSignal.class, SourceMode.SINGLE_PRODUCER)
                .stage("validate", RouteSignal.class, RouteSignal.class,
                    EndToEndPathBenchmark::pass, STAGE)
                .broadcast("fanout", RouteSignal.class, BroadcastSpec.copy(signal -> signal), STAGE)
                .stage("journal", RouteSignal.class, RouteSignal.class,
                    EndToEndPathBenchmark::pass, STAGE)
                .stage("risk", RouteSignal.class, RouteSignal.class,
                    EndToEndPathBenchmark::pass, STAGE)
                .join("join", RouteSignal.class, JoinSpec.<RouteSignal>allOf(group ->
                    group.value("journal", RouteSignal.class).orElseThrow())
                    .stampLong(item -> ((RouteSignal) item).sequence)
                    .capacity(RING_SIZE), STAGE)
                .sink("commit", RouteSignal.class,
                    signal -> completedSequence.lazySet(signal.sequence), STAGE)
                .edge("ingress", "validate", SPSC)
                .edge("validate", "fanout", SPSC)
                .edge("fanout", "journal", SPSC)
                .edge("fanout", "risk", SPSC)
                .edge("journal", "join", SPSC)
                .edge("risk", "join", SPSC)
                .edge("join", "commit", SPSC)
                .build();
            graph.start();
            emitter = graph.emitter("ingress", RouteSignal.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            requireCompleted("latticeDependencyJoinCompleted", completedSequence);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorDependencyGraphState extends RouteSignalPool {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        Disruptor<SignalEvent> disruptor;
        RingBuffer<SignalEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = signalDisruptor("e2e-disruptor-dependency");
            final EventHandlerGroup<SignalEvent> validate = disruptor.handleEventsWith(noopSignalHandler());
            validate
                .then(noopSignalHandler(), noopSignalHandler())
                .then((event, sequence, endOfBatch) -> completedSequence.lazySet(event.sequence));
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireCompleted("disruptorDependencyGraphCompleted", completedSequence);
        }
    }

    public static class OrderPool {
        private final Order[] orders = new Order[POOL_SIZE];
        private long cursor;

        public OrderPool() {
            for (int i = 0; i < orders.length; i++) {
                orders[i] = new Order();
            }
        }

        Order nextOrder() {
            final long sequence = cursor++;
            final Order order = orders[(int) sequence & (orders.length - 1)];
            order.sequence = sequence;
            order.raw = (sequence + 1L) * 0x9E3779B97F4A7C15L;
            order.key = 0L;
            order.enriched = 0L;
            order.checksum = 0L;
            order.riskFlags = 0;
            order.passed = false;
            return order;
        }
    }

    public static class RouteSignalPool {
        private final RouteSignal[] signals = new RouteSignal[POOL_SIZE];
        private long cursor;

        public RouteSignalPool() {
            for (int i = 0; i < signals.length; i++) {
                signals[i] = new RouteSignal();
            }
        }

        RouteSignal nextSignal() {
            final long sequence = cursor++;
            final RouteSignal signal = signals[(int) sequence & (signals.length - 1)];
            signal.sequence = sequence;
            signal.value = sequence * 31L;
            return signal;
        }
    }

    public static final class Order {
        long sequence;
        long raw;
        long key;
        long enriched;
        long checksum;
        int riskFlags;
        boolean passed;
        final byte[] scratch = new byte[16];
    }

    public static final class OrderEvent {
        long sequence;
        long raw;
        long key;
        long enriched;
        long checksum;
        int riskFlags;
        boolean passed;
        final byte[] scratch = new byte[16];
    }

    public static final class RouteSignal {
        long sequence;
        long value;
    }

    public static final class SignalEvent {
        long sequence;
        long value;
    }

    private static Disruptor<OrderEvent> orderDisruptor(final String threadName) {
        final EventFactory<OrderEvent> factory = OrderEvent::new;
        return new Disruptor<>(
            factory,
            RING_SIZE,
            namedThreadFactory(threadName),
            ProducerType.SINGLE,
            new BusySpinWaitStrategy()
        );
    }

    private static Disruptor<SignalEvent> signalDisruptor(final String threadName) {
        final EventFactory<SignalEvent> factory = SignalEvent::new;
        return new Disruptor<>(
            factory,
            RING_SIZE,
            namedThreadFactory(threadName),
            ProducerType.SINGLE,
            new BusySpinWaitStrategy()
        );
    }

    private static void publish(final RingBuffer<OrderEvent> ringBuffer, final Order order) {
        final long sequence = ringBuffer.next();
        try {
            final OrderEvent event = ringBuffer.get(sequence);
            event.sequence = order.sequence;
            event.raw = order.raw;
            event.key = 0L;
            event.enriched = 0L;
            event.checksum = 0L;
            event.riskFlags = 0;
            event.passed = false;
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    private static void publish(final RingBuffer<SignalEvent> ringBuffer, final RouteSignal signal) {
        final long sequence = ringBuffer.next();
        try {
            final SignalEvent event = ringBuffer.get(sequence);
            event.sequence = signal.sequence;
            event.value = signal.value;
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    private static void parse(final Order order, final Output<Order> out, final Object ctx) {
        parse(order);
        out.push(order);
    }

    private static void enrich(final Order order, final Output<Order> out, final Object ctx) {
        enrich(order);
        out.push(order);
    }

    private static void risk(final Order order, final Output<Order> out, final Object ctx) {
        risk(order);
        out.push(order);
    }

    private static void pass(final RouteSignal signal, final Output<RouteSignal> out, final Object ctx) {
        out.push(signal);
    }

    private static void parse(final Order order) {
        long mix = order.raw;
        mix ^= mix >>> 33;
        mix *= 0xFF51AFD7ED558CCDL;
        mix ^= mix >>> 33;
        mix *= 0xC4CEB9FE1A85EC53L;
        mix ^= mix >>> 33;
        order.key = mix;
    }

    private static void enrich(final Order order) {
        final int slot = (int) (order.key & (SIDE_TABLE_SIZE - 1));
        final long enriched = SIDE_TABLE[slot] ^ order.key;
        order.enriched = enriched;
        order.checksum = order.checksum * 31L + enriched;
    }

    private static void risk(final Order order) {
        final long v = order.enriched;
        int flags = 0;
        if (v > 0L) {
            flags |= 1;
        }
        if ((v & 0xFFFFL) > 0x8000L) {
            flags |= 2;
        }
        if ((v >>> 16) % 7L == 0L) {
            flags |= 4;
        }
        if ((v >>> 24) % 13L == 0L) {
            flags |= 8;
        }
        if (Long.bitCount(v) > 32) {
            flags |= 16;
        }
        order.riskFlags = flags;
        order.passed = (flags & 0b11000) == 0;
    }

    private static void serialize(final Order order) {
        final byte[] buf = order.scratch;
        final long v = order.enriched ^ order.checksum;
        buf[0] = (byte) v;
        buf[1] = (byte) (v >>> 8);
        buf[2] = (byte) (v >>> 16);
        buf[3] = (byte) (v >>> 24);
        buf[4] = (byte) (v >>> 32);
        buf[5] = (byte) (v >>> 40);
        buf[6] = (byte) (v >>> 48);
        buf[7] = (byte) (v >>> 56);
        buf[8] = (byte) order.riskFlags;
        buf[9] = order.passed ? (byte) 1 : (byte) 0;
        order.checksum ^= v;
    }

    private static void parse(final OrderEvent event) {
        long mix = event.raw;
        mix ^= mix >>> 33;
        mix *= 0xFF51AFD7ED558CCDL;
        mix ^= mix >>> 33;
        mix *= 0xC4CEB9FE1A85EC53L;
        mix ^= mix >>> 33;
        event.key = mix;
    }

    private static void enrich(final OrderEvent event) {
        final int slot = (int) (event.key & (SIDE_TABLE_SIZE - 1));
        final long enriched = SIDE_TABLE[slot] ^ event.key;
        event.enriched = enriched;
        event.checksum = event.checksum * 31L + enriched;
    }

    private static void risk(final OrderEvent event) {
        final long v = event.enriched;
        int flags = 0;
        if (v > 0L) {
            flags |= 1;
        }
        if ((v & 0xFFFFL) > 0x8000L) {
            flags |= 2;
        }
        if ((v >>> 16) % 7L == 0L) {
            flags |= 4;
        }
        if ((v >>> 24) % 13L == 0L) {
            flags |= 8;
        }
        if (Long.bitCount(v) > 32) {
            flags |= 16;
        }
        event.riskFlags = flags;
        event.passed = (flags & 0b11000) == 0;
    }

    private static void serialize(final OrderEvent event) {
        final byte[] buf = event.scratch;
        final long v = event.enriched ^ event.checksum;
        buf[0] = (byte) v;
        buf[1] = (byte) (v >>> 8);
        buf[2] = (byte) (v >>> 16);
        buf[3] = (byte) (v >>> 24);
        buf[4] = (byte) (v >>> 32);
        buf[5] = (byte) (v >>> 40);
        buf[6] = (byte) (v >>> 48);
        buf[7] = (byte) (v >>> 56);
        buf[8] = (byte) event.riskFlags;
        buf[9] = event.passed ? (byte) 1 : (byte) 0;
        event.checksum ^= v;
    }

    private static EventHandler<OrderEvent> parseHandler() {
        return (event, sequence, endOfBatch) -> parse(event);
    }

    private static EventHandler<OrderEvent> enrichHandler() {
        return (event, sequence, endOfBatch) -> enrich(event);
    }

    private static EventHandler<OrderEvent> riskHandler() {
        return (event, sequence, endOfBatch) -> risk(event);
    }

    private static EventHandler<SignalEvent> noopSignalHandler() {
        return (event, sequence, endOfBatch) -> {
        };
    }

    private static void awaitCompleted(final AtomicLong completedSequence, final long expectedSequence) {
        while (completedSequence.get() < expectedSequence) {
            Thread.onSpinWait();
        }
    }

    private static void requireCompleted(final String benchmark, final AtomicLong completedSequence) {
        if (completedSequence.get() < 0L) {
            throw new IllegalStateException(benchmark + " did not complete any events");
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

    private static long[] sideTable() {
        final long[] table = new long[SIDE_TABLE_SIZE];
        long mix = 0x9E3779B97F4A7C15L;
        for (int i = 0; i < table.length; i++) {
            mix ^= (long) i * 0x100000001B3L;
            table[i] = mix;
        }
        return table;
    }
}
