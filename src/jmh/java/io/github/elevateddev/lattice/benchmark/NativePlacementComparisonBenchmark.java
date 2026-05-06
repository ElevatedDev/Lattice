package io.github.elevateddev.lattice.benchmark;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.FusionSpec;
import io.github.elevateddev.lattice.graph.GraphPlacementSpec;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.nativeaccess.NativeCapabilities;
import io.github.elevateddev.lattice.nativeaccess.NativeTopology;
import io.github.elevateddev.lattice.nativeaccess.NativeTopologyException;
import io.github.elevateddev.lattice.placement.PinPolicy;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.Output;
import io.github.elevateddev.lattice.stage.StageSpec;
import io.github.elevateddev.lattice.wait.WaitSpec;
import java.time.Duration;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class NativePlacementComparisonBenchmark {

    private static final String CPU_A_PROPERTY = "lattice.bench.cpuA";
    private static final String CPU_B_PROPERTY = "lattice.bench.cpuB";
    private static final String CPU_C_PROPERTY = "lattice.bench.cpuC";
    private static final String NUMA_NODE_PROPERTY = "lattice.bench.numaNode";

    private static final int RING_SIZE = 8192;
    private static final int POOL_SIZE = 1 << 17;
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final WaitSpec WAIT = WaitSpec.busySpin();
    private static final EdgeSpec SPSC = EdgeSpec.spscRing(RING_SIZE).wait(WAIT);

    @Benchmark
    @Fork(jvmArgsAppend = {
        "-Dlattice.native.enabled=false"
    })
    public long pinnedCompletedWithoutNative(final NoNativePinnedState state) {
        return emitAndWait(state);
    }

    @Benchmark
    public long pinnedCompletedWithNative(
        final NativePinnedState state,
        final NativeProducerThreadState producerThreadState
    ) {
        return emitAndWait(state);
    }

    private static long emitAndWait(final BasePinnedState state) {
        final Order order = state.nextOrder();
        state.emitter.emit(order);
        awaitCompleted(state.completedSequence, order.sequence);
        return order.checksum;
    }

    @State(Scope.Thread)
    public static class NativeProducerThreadState {

        @Setup(Level.Trial)
        public void setup(final NativePinnedState state) {
            state.pinProducerThread();
        }
    }

    @State(Scope.Benchmark)
    public static class NoNativePinnedState extends BasePinnedState {

        @Override
        boolean requireNativePlacement() {
            return false;
        }

        @Override
        String graphName() {
            return "native-placement-comparison-disabled";
        }
    }

    @State(Scope.Benchmark)
    public static class NativePinnedState extends BasePinnedState {

        @Override
        boolean requireNativePlacement() {
            return true;
        }

        @Override
        String graphName() {
            return "native-placement-comparison-enabled";
        }
    }

    @State(Scope.Benchmark)
    public abstract static class BasePinnedState {
        final AtomicLong completedSequence = new AtomicLong(-1L);
        final Order[] orders = new Order[POOL_SIZE];

        @Param({"none", "cpu", "core", "cpuSet", "numaNode", "inheritCpuset"})
        public String pinPolicy;

        StaticGraph graph;
        Emitter<Order> emitter;
        long cursor;

        BasePinnedState() {
            for (int i = 0; i < orders.length; i++) {
                orders[i] = new Order();
            }
        }

        @Setup(Level.Trial)
        public void setup() {
            if (requireNativePlacement()) {
                validateNativePlacement();
            }

            try {
                graph = StaticGraph.builder(graphName())
                    .fusion(FusionSpec.disabled())
                    .placement(GraphPlacementSpec.off()
                        .strict(requireNativePlacement())
                        .firstTouch(true))
                    .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
                    .stage("parse", Order.class, Order.class, NativePlacementComparisonBenchmark::parse,
                        pinnedStageSpec(stageCpu()))
                    .sink("commit", Order.class, order -> {
                        serialize(order);
                        completedSequence.lazySet(order.sequence);
                    }, pinnedStageSpec(sinkCpu()))
                    .edge("ingress", "parse", SPSC)
                    .edge("parse", "commit", SPSC)
                    .build();
                graph.start();
                emitter = graph.emitter("ingress", Order.class);
            } catch (final RuntimeException | Error ex) {
                cleanupFailedSetup(ex);
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
                requireCompleted(graphName(), completedSequence);
            }
        }

        Order nextOrder() {
            final long sequence = cursor++;
            final Order order = orders[(int) sequence & (orders.length - 1)];
            order.sequence = sequence;
            order.raw = (sequence + 1L) * 0x9E3779B97F4A7C15L;
            order.key = 0L;
            order.checksum = 0L;
            order.riskFlags = 0;
            return order;
        }

        void pinProducerThread() {
            if (!pinsProducerThread()) {
                return;
            }
            validateNativePlacement();
            final int cpu = producerCpu();
            try {
                NativeTopology.pinCurrentThreadToCpu(cpu);
                final NativeCapabilities capabilities = NativeTopology.capabilities();
                if (capabilities.currentCpu() && NativeTopology.currentCpu() != cpu) {
                    throw new IllegalStateException("producer thread did not pin to CPU " + cpu);
                }
            } catch (final NativeTopologyException ex) {
                throw new IllegalStateException("failed to pin benchmark producer thread to CPU " + cpu, ex);
            }
        }

        abstract boolean requireNativePlacement();

        abstract String graphName();

        private StageSpec pinnedStageSpec(final int preferredCpu) {
            return StageSpec.singleThreaded().wait(WAIT).pin(pinPolicy(preferredCpu));
        }

        private PinPolicy pinPolicy(final int preferredCpu) {
            return switch (pinPolicy) {
                case "none" -> PinPolicy.none();
                case "cpu" -> PinPolicy.cpu(preferredCpu);
                case "core" -> PinPolicy.core(preferredCpu);
                case "cpuSet" -> PinPolicy.cpuSet(cpuSet(stageCpu(), sinkCpu()));
                case "numaNode" -> PinPolicy.numaNode(numaNode());
                case "inheritCpuset" -> PinPolicy.inheritCpuset();
                default -> throw new IllegalArgumentException("unsupported pinPolicy: " + pinPolicy);
            };
        }

        private boolean pinsProducerThread() {
            return switch (pinPolicy) {
                case "cpu", "core", "cpuSet" -> true;
                default -> false;
            };
        }

        private void validateNativePlacement() {
            if (!NativeTopology.isLoaded()) {
                throw new IllegalStateException("native placement benchmark requires the native library. "
                    + "Pass -Dlattice.native.library.path=<shared-library> or set java.library.path. Failure: "
                    + NativeTopology.loadFailureMessage());
            }

            final NativeCapabilities capabilities;
            try {
                capabilities = NativeTopology.capabilities();
            } catch (final NativeTopologyException ex) {
                throw new IllegalStateException("failed to query native topology capabilities", ex);
            }
            switch (pinPolicy) {
                case "none" -> {
                }
                case "cpu", "core" -> {
                    requireAffinity(capabilities);
                    validateCpu(producerCpu(), CPU_A_PROPERTY);
                    validateCpu(stageCpu(), CPU_B_PROPERTY);
                    validateCpu(sinkCpu(), CPU_C_PROPERTY);
                }
                case "cpuSet" -> {
                    requireAffinity(capabilities);
                    validateCpu(producerCpu(), CPU_A_PROPERTY);
                    validateCpu(stageCpu(), CPU_B_PROPERTY);
                    validateCpu(sinkCpu(), CPU_C_PROPERTY);
                }
                case "numaNode" -> {
                    requireAffinity(capabilities);
                    if (!capabilities.numaQuery()) {
                        throw new IllegalStateException("numaNode policy requires native NUMA query support");
                    }
                    validateNumaNode(numaNode());
                }
                case "inheritCpuset" -> {
                    if (!capabilities.linux()) {
                        throw new IllegalStateException("inheritCpuset policy requires Linux native support");
                    }
                }
                default -> throw new IllegalArgumentException("unsupported pinPolicy: " + pinPolicy);
            }
        }

        private void requireAffinity(final NativeCapabilities capabilities) {
            if (!capabilities.affinity()) {
                throw new IllegalStateException(pinPolicy + " policy requires native affinity support");
            }
        }

        private void validateCpu(final int cpu, final String property) {
            if (cpu < 0) {
                throw new IllegalStateException(property + " must be non-negative");
            }
            final int maxCpu;
            try {
                maxCpu = NativeTopology.maxCpuCount();
            } catch (final NativeTopologyException ex) {
                throw new IllegalStateException("failed to query max CPU count", ex);
            }
            if (cpu >= maxCpu) {
                throw new IllegalStateException(property + "=" + cpu + " is outside maxCpuCount=" + maxCpu);
            }
        }

        private int producerCpu() {
            return Integer.getInteger(CPU_A_PROPERTY, 0);
        }

        private int stageCpu() {
            return Integer.getInteger(CPU_B_PROPERTY, 1);
        }

        private int sinkCpu() {
            return Integer.getInteger(CPU_C_PROPERTY, 2);
        }

        private int numaNode() {
            final Integer configured = Integer.getInteger(NUMA_NODE_PROPERTY);
            if (configured != null) {
                return configured;
            }
            if (!requireNativePlacement() || !NativeTopology.isLoaded()) {
                return 0;
            }
            try {
                return NativeTopology.numaNodeOfCpu(stageCpu());
            } catch (final NativeTopologyException ex) {
                throw new IllegalStateException("failed to infer NUMA node from " + CPU_B_PROPERTY, ex);
            }
        }

        private void validateNumaNode(final int numaNode) {
            if (numaNode < 0) {
                throw new IllegalStateException(NUMA_NODE_PROPERTY + " must be non-negative");
            }
            try {
                final int stageNode = NativeTopology.numaNodeOfCpu(stageCpu());
                if (stageNode != numaNode && System.getProperty(NUMA_NODE_PROPERTY) == null) {
                    throw new IllegalStateException("inferred NUMA node " + numaNode
                        + " does not match stage CPU node " + stageNode);
                }
            } catch (final NativeTopologyException ex) {
                throw new IllegalStateException("failed to validate NUMA node " + numaNode, ex);
            }
        }

        private void cleanupFailedSetup(final Throwable failure) {
            if (emitter != null) {
                try {
                    emitter.close();
                } catch (final RuntimeException ex) {
                    failure.addSuppressed(ex);
                }
            }
            if (graph != null) {
                try {
                    graph.stop(STOP_TIMEOUT);
                } catch (final RuntimeException ex) {
                    failure.addSuppressed(ex);
                }
            }
        }
    }

    public static final class Order {
        long sequence;
        long raw;
        long key;
        long checksum;
        int riskFlags;
        final byte[] scratch = new byte[16];
    }

    private static void parse(final Order order, final Output<Order> out, final Object ctx) {
        long mix = order.raw;
        mix ^= mix >>> 33;
        mix *= 0xFF51AFD7ED558CCDL;
        mix ^= mix >>> 33;
        mix *= 0xC4CEB9FE1A85EC53L;
        mix ^= mix >>> 33;
        order.key = mix;
        order.checksum = mix ^ order.sequence;
        out.push(order);
    }

    private static void serialize(final Order order) {
        final byte[] buf = order.scratch;
        final long v = order.key ^ order.checksum;
        buf[0] = (byte) v;
        buf[1] = (byte) (v >>> 8);
        buf[2] = (byte) (v >>> 16);
        buf[3] = (byte) (v >>> 24);
        buf[4] = (byte) (v >>> 32);
        buf[5] = (byte) (v >>> 40);
        buf[6] = (byte) (v >>> 48);
        buf[7] = (byte) (v >>> 56);
        order.riskFlags = Long.bitCount(v);
        order.checksum ^= v;
    }

    private static BitSet cpuSet(final int firstCpu, final int secondCpu) {
        final BitSet cpus = new BitSet();
        cpus.set(firstCpu);
        cpus.set(secondCpu);
        return cpus;
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

}
