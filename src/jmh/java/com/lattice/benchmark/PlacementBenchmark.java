package com.lattice.benchmark;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphPlacementSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.placement.MemoryMode;
import com.lattice.placement.PinPolicy;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import com.lattice.nativeaccess.NativeCapabilities;
import com.lattice.nativeaccess.NativeTopology;
import com.lattice.nativeaccess.NativeTopologyException;
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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class PlacementBenchmark {

    private static final String CPU_A_PROPERTY = "lattice.bench.cpuA";
    private static final String CPU_B_PROPERTY = "lattice.bench.cpuB";
    private static final String CPU_CROSS_PROPERTY = "lattice.bench.cpuCross";

    @Benchmark
    public void spscPlacement(final SpscPlacementState state, final SpscProducerThreadState producerThreadState) {
        final long started = System.nanoTime();
        state.emitter.emit(started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    @Group("mpscPlacement")
    @GroupThreads(4)
    public void mpscPlacement(final MpscPlacementState state, final MpscProducerThreadState producerThreadState) {
        final long started = System.nanoTime();
        state.emitter.emit(started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @State(Scope.Thread)
    public static class SpscProducerThreadState {

        @Setup(Level.Trial)
        public void setup(final SpscPlacementState state) {
            pinProducerThread(state.placement, state.pinning);
        }
    }

    @State(Scope.Thread)
    public static class MpscProducerThreadState {

        @Setup(Level.Trial)
        public void setup(final MpscPlacementState state) {
            pinProducerThread(state.placement, state.pinning);
        }
    }

    @State(Scope.Benchmark)
    public static class SpscPlacementState {
        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();

        @Param({"sameCorePair", "sameSocketSpsc", "crossSocketSpsc", "containerCpuset"})
        public String placement;

        @Param({"false", "true"})
        public boolean pinning;

        @Param({"false", "true"})
        public boolean firstTouchPlacement;

        @Param({"onHeap", "offHeapHandles"})
        public String memory;

        StaticGraph graph;
        Emitter<Long> emitter;

        @Setup(Level.Trial)
        public void setup() {
            validateSpscPlacement(placement, pinning);

            try {
                graph = StaticGraph.builder("placement-spsc")
                    .placement(GraphPlacementSpec.off()
                        .strict(pinning)
                        .firstTouch(firstTouchPlacement))
                    .source("ingress", Long.class)
                    .stage("stage", Long.class, Long.class, (value, out, ctx) -> out.push(value + 1L),
                        StageSpec.singleThreaded().pin(stagePolicy(placement, pinning)))
                    .sink("egress", Long.class, emittedAtNanos -> {
                        consumed.incrementAndGet();
                        endToEndLatency.recordElapsedSince(emittedAtNanos - 1L);
                    }, StageSpec.singleThreaded().pin(sinkPolicy(placement, pinning)))
                    .edge("ingress", "stage", EdgeSpec.mpscRing(8192).memory(memoryMode(memory)))
                    .edge("stage", "egress", EdgeSpec.spscRing(8192).memory(memoryMode(memory)))
                    .build();
                graph.start();
                emitter = graph.emitter("ingress", Long.class);
            } catch (final RuntimeException | Error ex) {
                cleanupFailedSetup(graph, emitter, ex);
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
                    graph.stop(Duration.ofSeconds(10));
                }
            } finally {
                final String benchmarkName = "spscPlacement-" + placement + "-pinning-" + pinning
                    + "-firstTouch-" + firstTouchPlacement + "-" + memory;
                enqueueLatency.print(benchmarkName, "enqueue");
                endToEndLatency.print(benchmarkName, "endToEnd");
            }
        }
    }

    @State(Scope.Benchmark)
    public static class MpscPlacementState {
        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();

        @Param({"sameSocketMpsc", "crossSocketMpsc", "containerCpuset"})
        public String placement;

        @Param({"false", "true"})
        public boolean pinning;

        @Param({"false", "true"})
        public boolean firstTouchPlacement;

        @Param({"onHeap", "offHeapHandles"})
        public String memory;

        StaticGraph graph;
        Emitter<Long> emitter;

        @Setup(Level.Trial)
        public void setup() {
            validateMpscPlacement(placement, pinning);

            try {
                graph = StaticGraph.builder("placement-mpsc")
                    .placement(GraphPlacementSpec.off()
                        .strict(pinning)
                        .firstTouch(firstTouchPlacement))
                    .source("ingress", Long.class)
                    .sink("egress", Long.class, emittedAtNanos -> {
                        consumed.incrementAndGet();
                        endToEndLatency.recordElapsedSince(emittedAtNanos);
                    }, StageSpec.singleThreaded().pin(mpscSinkPolicy(placement, pinning)))
                    .edge("ingress", "egress", EdgeSpec.mpscRing(8192).memory(memoryMode(memory)))
                    .build();
                graph.start();
                emitter = graph.emitter("ingress", Long.class);
            } catch (final RuntimeException | Error ex) {
                cleanupFailedSetup(graph, emitter, ex);
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
                    graph.stop(Duration.ofSeconds(10));
                }
            } finally {
                final String benchmarkName = "mpscPlacement-" + placement + "-pinning-" + pinning
                    + "-firstTouch-" + firstTouchPlacement + "-" + memory;
                enqueueLatency.print(benchmarkName, "enqueue");
                endToEndLatency.print(benchmarkName, "endToEnd");
            }
        }
    }

    private static PinPolicy stagePolicy(final String placement, final boolean pinning) {
        if (!pinning) {
            return PinPolicy.none();
        }
        return switch (placement) {
            case "sameCorePair", "sameSocketSpsc", "crossSocketSpsc" -> PinPolicy.cpu(cpu(CPU_A_PROPERTY, 0));
            case "containerCpuset" -> PinPolicy.inheritCpuset();
            default -> PinPolicy.none();
        };
    }

    private static PinPolicy sinkPolicy(final String placement, final boolean pinning) {
        if (!pinning) {
            return PinPolicy.none();
        }
        return switch (placement) {
            case "sameCorePair" -> PinPolicy.cpu(cpu(CPU_A_PROPERTY, 0));
            case "sameSocketSpsc" -> PinPolicy.cpu(cpu(CPU_B_PROPERTY, 1));
            case "crossSocketSpsc" -> PinPolicy.cpu(cpu(CPU_CROSS_PROPERTY, 1));
            case "containerCpuset" -> PinPolicy.inheritCpuset();
            default -> PinPolicy.none();
        };
    }

    private static PinPolicy mpscSinkPolicy(final String placement, final boolean pinning) {
        if (!pinning) {
            return PinPolicy.none();
        }
        return switch (placement) {
            case "sameSocketMpsc" -> PinPolicy.cpu(cpu(CPU_B_PROPERTY, 1));
            case "crossSocketMpsc" -> PinPolicy.cpu(cpu(CPU_CROSS_PROPERTY, 1));
            case "containerCpuset" -> PinPolicy.inheritCpuset();
            default -> PinPolicy.none();
        };
    }

    private static void validateSpscPlacement(final String placement, final boolean pinning) {
        if (!pinning) {
            return;
        }

        final NativeCapabilities capabilities = requireNativeCapabilities(placement);

        switch (placement) {
            case "containerCpuset" -> {
            }
            case "sameCorePair" -> {
                requireAffinity(capabilities, placement);
                validateCpuProperty(CPU_A_PROPERTY, 0, placement);
            }
            case "sameSocketSpsc" -> {
                requireAffinity(capabilities, placement);
                requireNumaQuery(capabilities, placement);
                final int producerCpu = validateCpuProperty(CPU_A_PROPERTY, 0, placement);
                final int sinkCpu = validateCpuProperty(CPU_B_PROPERTY, 1, placement);
                requireDistinctCpus(producerCpu, CPU_A_PROPERTY, sinkCpu, CPU_B_PROPERTY, placement);
                requireSameNumaNode(producerCpu, sinkCpu, placement);
            }
            case "crossSocketSpsc" -> {
                requireAffinity(capabilities, placement);
                requireNumaQuery(capabilities, placement);
                final int producerCpu = validateCpuProperty(CPU_A_PROPERTY, 0, placement);
                final int sinkCpu = validateCpuProperty(CPU_CROSS_PROPERTY, 1, placement);
                requireDistinctCpus(producerCpu, CPU_A_PROPERTY, sinkCpu, CPU_CROSS_PROPERTY, placement);
                requireDifferentNumaNode(producerCpu, sinkCpu, placement);
            }
            default -> throw misconfigured(placement, "unknown SPSC placement mode");
        }
    }

    private static void validateMpscPlacement(final String placement, final boolean pinning) {
        if (!pinning) {
            return;
        }

        final NativeCapabilities capabilities = requireNativeCapabilities(placement);

        switch (placement) {
            case "containerCpuset" -> {
            }
            case "sameSocketMpsc" -> {
                requireAffinity(capabilities, placement);
                requireNumaQuery(capabilities, placement);
                final int producerCpu = validateCpuProperty(CPU_A_PROPERTY, 0, placement);
                final int sinkCpu = validateCpuProperty(CPU_B_PROPERTY, 1, placement);
                requireDistinctCpus(producerCpu, CPU_A_PROPERTY, sinkCpu, CPU_B_PROPERTY, placement);
                requireSameNumaNode(producerCpu, sinkCpu, placement);
            }
            case "crossSocketMpsc" -> {
                requireAffinity(capabilities, placement);
                requireNumaQuery(capabilities, placement);
                final int producerCpu = validateCpuProperty(CPU_A_PROPERTY, 0, placement);
                final int sinkCpu = validateCpuProperty(CPU_CROSS_PROPERTY, 1, placement);
                requireDistinctCpus(producerCpu, CPU_A_PROPERTY, sinkCpu, CPU_CROSS_PROPERTY, placement);
                requireDifferentNumaNode(producerCpu, sinkCpu, placement);
            }
            default -> throw misconfigured(placement, "unknown MPSC placement mode");
        }
    }

    private static void pinProducerThread(final String placement, final boolean pinning) {
        if (!pinning || "containerCpuset".equals(placement)) {
            return;
        }

        final NativeCapabilities capabilities = requireNativeCapabilities(placement);
        requireAffinity(capabilities, placement);
        final int cpu = validateCpuProperty(CPU_A_PROPERTY, 0, placement);

        try {
            NativeTopology.pinCurrentThreadToCpu(cpu);
            if (capabilities.currentCpu()) {
                final int observedCpu = NativeTopology.currentCpu();
                if (observedCpu != cpu) {
                    throw misconfigured(placement, "producer thread pinned to CPU " + observedCpu
                        + " instead of requested CPU " + cpu);
                }
            }
        } catch (final NativeTopologyException ex) {
            throw misconfigured(placement, "failed to pin benchmark producer thread to CPU " + cpu
                + ": " + ex.getMessage(), ex);
        }
    }

    private static NativeCapabilities requireNativeCapabilities(final String placement) {
        if (!NativeTopology.isLoaded()) {
            throw misconfigured(placement, "pinning requires the native topology library");
        }
        try {
            return NativeTopology.capabilities();
        } catch (final NativeTopologyException ex) {
            throw misconfigured(placement, "failed to query native topology capabilities: " + ex.getMessage(), ex);
        }
    }

    private static void requireAffinity(final NativeCapabilities capabilities, final String placement) {
        if (!capabilities.affinity()) {
            throw misconfigured(placement, "pinning requires native affinity support");
        }
    }

    private static void requireNumaQuery(final NativeCapabilities capabilities, final String placement) {
        if (!capabilities.numaQuery()) {
            throw misconfigured(placement, "socket-labeled modes require native NUMA topology queries");
        }
    }

    private static int validateCpuProperty(final String property, final int fallback, final String placement) {
        final int cpu = cpu(property, fallback);
        if (cpu < 0) {
            throw misconfigured(placement, property + " must be non-negative");
        }

        final int maxCpuCount;
        try {
            maxCpuCount = NativeTopology.maxCpuCount();
        } catch (final NativeTopologyException ex) {
            throw misconfigured(placement, "failed to query max CPU count: " + ex.getMessage(), ex);
        }

        if (cpu >= maxCpuCount) {
            throw misconfigured(placement, property + "=" + cpu + " is outside maxCpuCount=" + maxCpuCount);
        }
        return cpu;
    }

    private static void requireDistinctCpus(
        final int leftCpu,
        final String leftProperty,
        final int rightCpu,
        final String rightProperty,
        final String placement
    ) {
        if (leftCpu == rightCpu) {
            throw misconfigured(placement, leftProperty + " and " + rightProperty + " resolve to the same CPU "
                + leftCpu);
        }
    }

    private static void requireSameNumaNode(final int leftCpu, final int rightCpu, final String placement) {
        final int leftNode = numaNodeOfCpu(leftCpu, placement);
        final int rightNode = numaNodeOfCpu(rightCpu, placement);
        if (leftNode != rightNode) {
            throw misconfigured(placement, "expected CPUs " + leftCpu + " and " + rightCpu
                + " on the same NUMA node but found " + leftNode + " and " + rightNode);
        }
    }

    private static void requireDifferentNumaNode(final int leftCpu, final int rightCpu, final String placement) {
        final int leftNode = numaNodeOfCpu(leftCpu, placement);
        final int rightNode = numaNodeOfCpu(rightCpu, placement);
        if (leftNode == rightNode) {
            throw misconfigured(placement, "expected CPUs " + leftCpu + " and " + rightCpu
                + " on different NUMA nodes but both resolved to " + leftNode);
        }
    }

    private static int numaNodeOfCpu(final int cpu, final String placement) {
        try {
            return NativeTopology.numaNodeOfCpu(cpu);
        } catch (final NativeTopologyException ex) {
            throw misconfigured(placement, "failed to query NUMA node for CPU " + cpu + ": " + ex.getMessage(), ex);
        }
    }

    private static MemoryMode memoryMode(final String memory) {
        return "offHeapHandles".equals(memory) ? MemoryMode.offHeapHandles() : MemoryMode.onHeapSlots();
    }

    private static int cpu(final String property, final int fallback) {
        return Integer.getInteger(property, fallback);
    }

    private static void cleanupFailedSetup(final StaticGraph graph, final Emitter<?> emitter, final Throwable failure) {
        if (emitter != null) {
            try {
                emitter.close();
            } catch (final RuntimeException ex) {
                failure.addSuppressed(ex);
            }
        }
        if (graph != null) {
            try {
                graph.stop(Duration.ofSeconds(10));
            } catch (final RuntimeException ex) {
                failure.addSuppressed(ex);
            }
        }
    }

    private static IllegalStateException misconfigured(final String placement, final String message) {
        return new IllegalStateException("PlacementBenchmark " + placement + ": " + message);
    }

    private static IllegalStateException misconfigured(
        final String placement,
        final String message,
        final Throwable cause
    ) {
        return new IllegalStateException("PlacementBenchmark " + placement + ": " + message, cause);
    }
}
