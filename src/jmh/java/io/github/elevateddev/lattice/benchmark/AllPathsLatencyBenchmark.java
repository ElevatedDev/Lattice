package io.github.elevateddev.lattice.benchmark;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.FusionSpec;
import io.github.elevateddev.lattice.graph.GraphPlacementSpec;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.nativeaccess.NativeTopology;
import io.github.elevateddev.lattice.placement.PinPolicy;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.Output;
import io.github.elevateddev.lattice.stage.StageSpec;
import io.github.elevateddev.lattice.wait.WaitSpec;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class AllPathsLatencyBenchmark {

    private static final String PINNED_CPU_A_PROPERTY = "lattice.bench.cpuA";
    private static final String PINNED_CPU_B_PROPERTY = "lattice.bench.cpuB";
    private static final String PINNED_CPU_C_PROPERTY = "lattice.bench.cpuC";
    private static final String PINNED_CPU_D_PROPERTY = "lattice.bench.cpuD";
    private static final int RING_SIZE = 8192;
    private static final int POOL_SIZE = 1 << 18;
    private static final int SIDE_TABLE_SIZE = 1024;
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final WaitSpec WAIT = WaitSpec.busySpin();
    private static final StageSpec STAGE = StageSpec.singleThreaded().wait(WAIT);
    private static final EdgeSpec SPSC = EdgeSpec.spscRing(RING_SIZE).wait(WAIT);
    private static final long[] SIDE_TABLE = sideTable();

    @Benchmark
    public long latticePhysicalEndToEnd(final LatticePhysicalState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticePhysicalPinnedCpuEndToEnd(final LatticePhysicalPinnedCpuState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticeFusedEndToEnd(final LatticeFusedState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticeFusedPinnedCpuEndToEnd(final LatticeFusedPinnedCpuState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticeSourceInlineEndToEnd(final LatticeSourceInlineState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticeSourceInlineElidedEndToEnd(final LatticeSourceInlineElidedState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticePinnedCpuElisionRequestedEndToEnd(final LatticePinnedCpuElisionRequestedState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticePhysicalStrictTopologyEndToEnd(final LatticePhysicalStrictTopologyState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticeFusedStrictTopologyEndToEnd(final LatticeFusedStrictTopologyState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long latticeStrictTopologyElisionRequestedEndToEnd(
        final LatticeStrictTopologyElisionRequestedState state
    ) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long disruptorPhysicalEndToEnd(final DisruptorPhysicalState state) {
        final Order order = state.nextOrder();
        publish(state.ringBuffer, order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long disruptorPhysicalPinnedCpuEndToEnd(final DisruptorPhysicalPinnedCpuState state) {
        final Order order = state.nextOrder();
        publish(state.ringBuffer, order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long disruptorManualFusedEndToEnd(final DisruptorManualFusedState state) {
        final Order order = state.nextOrder();
        publish(state.ringBuffer, order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @Benchmark
    public long disruptorManualFusedPinnedCpuEndToEnd(final DisruptorManualFusedPinnedCpuState state) {
        final Order order = state.nextOrder();
        publish(state.ringBuffer, order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @State(Scope.Benchmark)
    public static class LatticePhysicalState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup("all-paths-lattice-physical", FusionSpec.disabled(), "latticePhysicalEndToEnd");
        }
    }

    @State(Scope.Benchmark)
    public static class LatticePhysicalPinnedCpuState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "all-paths-lattice-physical-pinned-cpu",
                FusionSpec.disabled(),
                strictFirstTouchPlacement(),
                pinnedStage(PINNED_CPU_A_PROPERTY, 0),
                pinnedStage(PINNED_CPU_B_PROPERTY, 1),
                pinnedStage(PINNED_CPU_C_PROPERTY, 2),
                pinnedStage(PINNED_CPU_D_PROPERTY, 3),
                "latticePhysicalPinnedCpuEndToEnd"
            );
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeFusedState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup("all-paths-lattice-fused", FusionSpec.defaults(), "latticeFusedEndToEnd");
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeFusedPinnedCpuState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "all-paths-lattice-fused-pinned-cpu",
                FusionSpec.defaults(),
                strictFirstTouchPlacement(),
                pinnedStage(PINNED_CPU_A_PROPERTY, 0),
                STAGE,
                STAGE,
                STAGE,
                "latticeFusedPinnedCpuEndToEnd"
            );
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeSourceInlineState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "all-paths-lattice-source-inline",
                FusionSpec.defaults().inlineSources(true),
                "latticeSourceInlineEndToEnd"
            );
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeSourceInlineElidedState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "all-paths-lattice-source-inline-elided",
                FusionSpec.defaults()
                    .inlineSources(true)
                    .elideInlineSourcePhysicalPath(true),
                "latticeSourceInlineElidedEndToEnd"
            );
        }
    }

    @State(Scope.Benchmark)
    public static class LatticePinnedCpuElisionRequestedState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "all-paths-lattice-pinned-cpu-elision-requested",
                FusionSpec.defaults()
                    .inlineSources(true)
                    .elideInlineSourcePhysicalPath(true),
                strictFirstTouchPlacement(),
                pinnedStage(PINNED_CPU_A_PROPERTY, 0),
                STAGE,
                STAGE,
                STAGE,
                "latticePinnedCpuElisionRequestedEndToEnd"
            );
        }
    }

    @State(Scope.Benchmark)
    public static class LatticePhysicalStrictTopologyState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "all-paths-lattice-physical-strict-topology",
                FusionSpec.disabled(),
                strictTopologyPlacement(),
                STAGE,
                STAGE,
                STAGE,
                STAGE,
                "latticePhysicalStrictTopologyEndToEnd"
            );
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeFusedStrictTopologyState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "all-paths-lattice-fused-strict-topology",
                FusionSpec.defaults(),
                strictTopologyPlacement(),
                STAGE,
                STAGE,
                STAGE,
                STAGE,
                "latticeFusedStrictTopologyEndToEnd"
            );
        }
    }

    @State(Scope.Benchmark)
    public static class LatticeStrictTopologyElisionRequestedState extends LatticeState {
        @Setup(Level.Trial)
        public void setup() {
            setup(
                "all-paths-lattice-strict-topology-elision-requested",
                FusionSpec.defaults()
                    .inlineSources(true)
                    .elideInlineSourcePhysicalPath(true),
                strictTopologyPlacement(),
                STAGE,
                STAGE,
                STAGE,
                STAGE,
                "latticeStrictTopologyElisionRequestedEndToEnd"
            );
        }
    }

    public abstract static class LatticeState extends PooledOrders {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        StaticGraph graph;
        Emitter<Order> emitter;
        String benchmarkName;

        void setup(final String graphName, final FusionSpec fusionSpec, final String benchmarkName) {
            setup(
                graphName,
                fusionSpec,
                GraphPlacementSpec.off(),
                STAGE,
                STAGE,
                STAGE,
                STAGE,
                benchmarkName
            );
        }

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
            this.benchmarkName = benchmarkName;
            graph = StaticGraph.builder(graphName)
                .fusion(fusionSpec)
                .placement(placementSpec)
                .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
                .stage("parse", Order.class, Order.class, AllPathsLatencyBenchmark::parse, parseSpec)
                .stage("enrich", Order.class, Order.class, AllPathsLatencyBenchmark::enrich, enrichSpec)
                .stage("risk", Order.class, Order.class, AllPathsLatencyBenchmark::risk, riskSpec)
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
    public static class DisruptorPhysicalState extends PooledOrders {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        Disruptor<OrderEvent> disruptor;
        RingBuffer<OrderEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("all-paths-disruptor-physical");
            disruptor.handleEventsWith(parseHandler())
                .then(enrichHandler())
                .then(riskHandler())
                .then((event, sequence, endOfBatch) -> {
                    final Order order = event.order;
                    serialize(order);
                    completedSequence.lazySet(order.sequence);
                    event.order = null;
                });
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireCompleted("disruptorPhysicalEndToEnd", completedSequence);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorPhysicalPinnedCpuState extends PooledOrders {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        Disruptor<OrderEvent> disruptor;
        RingBuffer<OrderEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            final int cpuA = cpuProperty(PINNED_CPU_A_PROPERTY, 0);
            final int cpuB = cpuProperty(PINNED_CPU_B_PROPERTY, 1);
            final int cpuC = cpuProperty(PINNED_CPU_C_PROPERTY, 2);
            final int cpuD = cpuProperty(PINNED_CPU_D_PROPERTY, 3);
            requireNativePinning(cpuA, cpuB, cpuC, cpuD);
            disruptor = disruptor(
                "all-paths-disruptor-physical-pinned-cpu",
                pinnedThreadFactory("all-paths-disruptor-physical-pinned-cpu", cpuA, cpuB, cpuC, cpuD)
            );
            disruptor.handleEventsWith(parseHandler())
                .then(enrichHandler())
                .then(riskHandler())
                .then((event, sequence, endOfBatch) -> {
                    final Order order = event.order;
                    serialize(order);
                    completedSequence.lazySet(order.sequence);
                    event.order = null;
                });
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireCompleted("disruptorPhysicalPinnedCpuEndToEnd", completedSequence);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorManualFusedState extends PooledOrders {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        Disruptor<OrderEvent> disruptor;
        RingBuffer<OrderEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = disruptor("all-paths-disruptor-manual-fused");
            disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                final Order order = event.order;
                parse(order);
                enrich(order);
                risk(order);
                serialize(order);
                completedSequence.lazySet(order.sequence);
                event.order = null;
            });
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireCompleted("disruptorManualFusedEndToEnd", completedSequence);
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorManualFusedPinnedCpuState extends PooledOrders {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        Disruptor<OrderEvent> disruptor;
        RingBuffer<OrderEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            final int cpuA = cpuProperty(PINNED_CPU_A_PROPERTY, 0);
            requireNativePinning(cpuA);
            disruptor = disruptor(
                "all-paths-disruptor-manual-fused-pinned-cpu",
                pinnedThreadFactory("all-paths-disruptor-manual-fused-pinned-cpu", cpuA)
            );
            disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
                final Order order = event.order;
                parse(order);
                enrich(order);
                risk(order);
                serialize(order);
                completedSequence.lazySet(order.sequence);
                event.order = null;
            });
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            requireCompleted("disruptorManualFusedPinnedCpuEndToEnd", completedSequence);
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

    private static void publish(final RingBuffer<OrderEvent> ringBuffer, final Order order) {
        final long sequence = ringBuffer.next();
        try {
            ringBuffer.get(sequence).order = order;
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    private static Disruptor<OrderEvent> disruptor(final String threadName) {
        return disruptor(threadName, namedThreadFactory(threadName));
    }

    private static Disruptor<OrderEvent> disruptor(final String threadName, final ThreadFactory threadFactory) {
        final EventFactory<OrderEvent> factory = OrderEvent::new;
        return new Disruptor<>(
            factory,
            RING_SIZE,
            threadFactory,
            ProducerType.SINGLE,
            new BusySpinWaitStrategy()
        );
    }

    private static EventHandler<OrderEvent> parseHandler() {
        return (event, sequence, endOfBatch) -> parse(event.order);
    }

    private static EventHandler<OrderEvent> enrichHandler() {
        return (event, sequence, endOfBatch) -> enrich(event.order);
    }

    private static EventHandler<OrderEvent> riskHandler() {
        return (event, sequence, endOfBatch) -> risk(event.order);
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

    private static GraphPlacementSpec strictFirstTouchPlacement() {
        return GraphPlacementSpec.off()
            .strict(true)
            .firstTouch(true);
    }

    private static GraphPlacementSpec strictTopologyPlacement() {
        return GraphPlacementSpec.off()
            .topologyAware(true)
            .strict(true)
            .firstTouch(true);
    }

    private static StageSpec pinnedStage(final String property, final int fallback) {
        return STAGE.pin(PinPolicy.cpu(cpuProperty(property, fallback)));
    }

    private static int cpuProperty(final String property, final int fallback) {
        final int cpu = Integer.getInteger(property, fallback);
        if (cpu < 0) {
            throw new IllegalArgumentException(property + " must not be negative");
        }
        return cpu;
    }

    private static void requireNativePinning(final int... cpus) {
        if (!NativeTopology.isLoaded()) {
            throw new IllegalStateException("native topology library is required for pinned Disruptor profiles: "
                + NativeTopology.loadFailureMessage());
        }
        if (!NativeTopology.capabilities().affinity()) {
            throw new IllegalStateException("native affinity is unavailable");
        }
        for (final int cpu : cpus) {
            if (!NativeTopology.isCpuAllowed(cpu)) {
                throw new IllegalArgumentException("CPU " + cpu + " is outside the process affinity mask");
            }
        }
    }

    private static ThreadFactory pinnedThreadFactory(final String name, final int... cpus) {
        final AtomicInteger nextCpu = new AtomicInteger();
        return runnable -> {
            final int cpu = cpus[Math.floorMod(nextCpu.getAndIncrement(), cpus.length)];
            final Thread thread = Thread.ofPlatform().unstarted(() -> {
                NativeTopology.pinCurrentThreadToCpu(cpu);
                if (NativeTopology.capabilities().localMemoryPolicy()) {
                    NativeTopology.setLocalAllocationPolicy();
                }
                runnable.run();
            });
            thread.setName(name + "-cpu-" + cpu);
            thread.setDaemon(true);
            return thread;
        };
    }

    private static ThreadFactory namedThreadFactory(final String name) {
        return runnable -> {
            final Thread thread = Thread.ofPlatform().unstarted(runnable);
            thread.setName(name);
            thread.setDaemon(true);
            return thread;
        };
    }
}
