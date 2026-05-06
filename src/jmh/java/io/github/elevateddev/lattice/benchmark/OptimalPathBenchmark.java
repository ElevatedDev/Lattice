package io.github.elevateddev.lattice.benchmark;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.FusionSpec;
import io.github.elevateddev.lattice.graph.GraphPlacementSpec;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.placement.PinPolicy;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.Output;
import io.github.elevateddev.lattice.stage.StageSpec;
import io.github.elevateddev.lattice.wait.WaitSpec;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
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
public class OptimalPathBenchmark {

    private static final String PINNED_CPU_PROPERTY = "lattice.bench.cpuA";
    private static final int RING_SIZE = 8192;
    private static final int POOL_SIZE = 1 << 18;
    private static final int SIDE_TABLE_SIZE = 1024;
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final WaitSpec WAIT = WaitSpec.busySpin();
    private static final StageSpec STAGE = StageSpec.singleThreaded().wait(WAIT);
    private static final EdgeSpec SPSC = EdgeSpec.spscRing(RING_SIZE).wait(WAIT);
    private static final long[] SIDE_TABLE = sideTable();

    @Benchmark
    public long latticePhysicalCompleted(final LatticePhysicalState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticeFusedCompleted(final LatticeFusedState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticePinnedFusedCompleted(final LatticePinnedFusedState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticeInlineFusedCompleted(final LatticeInlineFusedState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long disruptorManualFusedCompleted(final DisruptorManualFusedState state) {
        final Order order = state.nextOrder();
        final long sequence = state.ringBuffer.next();
        try {
            state.ringBuffer.get(sequence).order = order;
        } finally {
            state.ringBuffer.publish(sequence);
        }
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @State(Scope.Benchmark)
    public static class LatticePhysicalState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "optimal-lattice-physical",
                FusionSpec.disabled(),
                GraphPlacementSpec.off(),
                STAGE,
                STAGE,
                STAGE,
                STAGE,
                "latticePhysicalCompleted"
            );
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeFusedState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "optimal-lattice-fused",
                FusionSpec.defaults(),
                GraphPlacementSpec.off(),
                STAGE,
                STAGE,
                STAGE,
                STAGE,
                "latticeFusedCompleted"
            );
        }
    }

    @State(Scope.Benchmark)
    public static class LatticePinnedFusedState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "optimal-lattice-pinned-fused",
                FusionSpec.defaults(),
                GraphPlacementSpec.off()
                    .strict(true)
                    .firstTouch(true),
                pinnedStage(cpuProperty(PINNED_CPU_PROPERTY, 0)),
                STAGE,
                STAGE,
                STAGE,
                "latticePinnedFusedCompleted"
            );
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeInlineFusedState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "optimal-lattice-inline-fused",
                FusionSpec.defaults()
                    .inlineSources(true)
                    .elideInlineSourcePhysicalPath(true),
                GraphPlacementSpec.off(),
                STAGE,
                STAGE,
                STAGE,
                STAGE,
                "latticeInlineFusedCompleted"
            );
        }
    }

    public abstract static class LatticeState extends PooledOrders {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        StaticGraph graph;
        Emitter<Order> emitter;
        String benchmarkName;

        void setup(
            final String graphName,
            final FusionSpec fusionSpec,
            final GraphPlacementSpec placementSpec,
            final StageSpec parseSpec,
            final StageSpec enrichSpec,
            final StageSpec riskSpec,
            final StageSpec commitSpec,
            final String benchmarkName
        ) {
            try {
                this.benchmarkName = benchmarkName;
                graph = StaticGraph.builder(graphName)
                    .fusion(fusionSpec)
                    .placement(placementSpec)
                    .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
                    .stage("parse", Order.class, Order.class, OptimalPathBenchmark::parse, parseSpec)
                    .stage("enrich", Order.class, Order.class, OptimalPathBenchmark::enrich, enrichSpec)
                    .stage("risk", Order.class, Order.class, OptimalPathBenchmark::risk, riskSpec)
                    .sink("commit", Order.class, order -> {
                        serialize(order);
                        completedSequence.lazySet(order.sequence);
                    }, commitSpec)
                    .edge("ingress", "parse", SPSC)
                    .edge("parse", "enrich", SPSC)
                    .edge("enrich", "risk", SPSC)
                    .edge("risk", "commit", SPSC)
                    .build();
                graph.start();
                emitter = graph.emitter("ingress", Order.class);
            } catch (final RuntimeException | Error ex) {
                throw ex;
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            try {
                if (emitter != null) {
                    emitter.close();
                }
                if (graph != null) {
                    graph.stop(STOP_TIMEOUT);
                }
            } finally {
                requireCompleted(benchmarkName, completedSequence);
            }
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorManualFusedState extends PooledOrders {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        Disruptor<OrderEvent> disruptor;
        RingBuffer<OrderEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            final EventFactory<OrderEvent> factory = OrderEvent::new;
            disruptor = new Disruptor<>(
                factory,
                RING_SIZE,
                namedThreadFactory("optimal-disruptor-manual-fused"),
                ProducerType.SINGLE,
                new BusySpinWaitStrategy()
            );
            final EventHandler<OrderEvent> fused = (event, sequence, endOfBatch) -> {
                final Order order = event.order;
                parse(order);
                enrich(order);
                risk(order);
                serialize(order);
                completedSequence.lazySet(order.sequence);
                event.order = null;
            };
            disruptor.handleEventsWith(fused);
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireCompleted("disruptorManualFusedCompleted", completedSequence);
        }
    }

    public static class PooledOrders {
        private final Order[] orders = new Order[POOL_SIZE];
        private long cursor;

        public PooledOrders() {
            for (int i = 0; i < orders.length; i++) {
                orders[i] = new Order();
            }
        }

        Order nextOrder() {
            final long sequence = cursor++;
            final int index = (int) sequence & (orders.length - 1);
            final Order order = orders[index];
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
        Order order;
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

    private static void awaitCompleted(final AtomicLong completedSequence, final long expectedSequence) {
        while (completedSequence.get() < expectedSequence) {
            Thread.onSpinWait();
        }
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

    private static void requireCompleted(final String benchmark, final AtomicLong completedSequence) {
        if (completedSequence.get() < 0L) {
            throw new IllegalStateException(benchmark + " did not complete any events");
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

    private static StageSpec pinnedStage(final int cpu) {
        return STAGE.pin(PinPolicy.cpu(cpu));
    }

    private static int cpuProperty(final String property, final int fallback) {
        final int cpu = Integer.getInteger(property, fallback);
        if (cpu < 0) {
            throw new IllegalArgumentException(property + " must not be negative");
        }
        return cpu;
    }

}
