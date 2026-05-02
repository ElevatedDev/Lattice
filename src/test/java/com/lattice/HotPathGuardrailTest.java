package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.FusionSpec;
import com.lattice.graph.PreallocationSpec;
import com.lattice.graph.SourceMode;
import com.lattice.graph.StaticGraph;
import com.lattice.internal.edge.MpscRingEdge;
import com.lattice.internal.edge.SpscRingEdge;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.placement.MemoryMode;
import com.lattice.stage.Emitter;
import com.lattice.stage.PreallocatedEmitter;
import com.lattice.stage.StageSpec;
import com.lattice.wait.WaitSpec;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HotPathGuardrailTest {

    private static final int GRAPH_RING_CAPACITY = 32_768;
    private static final int GRAPH_WARMUP_EMITS = 10_000;
    private static final int GRAPH_MEASURED_EMITS = 20_000;
    private static final int GRAPH_SIGNAL_POOL_SIZE = 1 << 15;
    private static final long GRAPH_EMIT_ALLOCATION_LIMIT = 4_096L;
    private static final Duration GRAPH_STOP_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration GRAPH_CONSUME_TIMEOUT = Duration.ofSeconds(5);
    private static final WaitSpec GRAPH_WAIT = WaitSpec.blocking();
    private static final StageSpec GRAPH_STAGE = StageSpec.singleThreaded().wait(GRAPH_WAIT);
    private static final EdgeSpec GRAPH_SPSC = EdgeSpec.spscRing(GRAPH_RING_CAPACITY).wait(GRAPH_WAIT);
    private static final EdgeSpec GRAPH_MPSC = EdgeSpec.mpscRing(GRAPH_RING_CAPACITY).wait(GRAPH_WAIT);

    @Test
    void spscOfferPollSteadyStateDoesNotAllocate() {
        final com.sun.management.ThreadMXBean bean = allocationBean();
        if (bean == null) {
            return;
        }
        final SpscRingEdge edge = new SpscRingEdge("producer", "consumer", 1024, edgeMetrics(), graphMetrics());
        final Object payload = new Object();

        for (int i = 0; i < 100_000; i++) {
            edge.offer(payload);
            edge.poll();
        }

        final long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
        for (int i = 0; i < 250_000; i++) {
            edge.offer(payload);
            edge.poll();
        }
        final long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;
        assertTrue(allocated <= 1024, "steady-state SPSC hot path allocated " + allocated + " bytes");
    }

    @Test
    void mpscOfferPollSteadyStateDoesNotAllocateOnHeapSlots() {
        assertMpscOfferPollSteadyStateDoesNotAllocate(MemoryMode.onHeapSlots());
    }

    @Test
    void mpscOfferPollSteadyStateDoesNotAllocateOffHeapHandles() {
        assertMpscOfferPollSteadyStateDoesNotAllocate(MemoryMode.offHeapHandles());
    }

    @Test
    void physicalPlanKeepsGuardrailDecisionsForPhysicalFusedInlineAndMpscIngress() {
        final Object physicalPlan = runtimePlan(buildPipelineGraph("guardrail-physical-plan", false, false));
        assertEquals(List.of("normalize", "risk", "validate", "egress"), recordList(physicalPlan, "workerOrder"));
        assertTrue(recordMap(physicalPlan, "fusedStages").isEmpty());
        assertTrue(recordMap(physicalPlan, "inlineSourceBindings").isEmpty());
        assertEquals(4, recordMap(physicalPlan, "senderDecisions").size());

        final Object fusedPlan = runtimePlan(buildPipelineGraph("guardrail-fused-plan", true, false));
        assertEquals(List.of("normalize"), recordList(fusedPlan, "workerOrder"));
        assertEquals(Set.of("normalize->risk", "risk->validate", "validate->egress"),
            recordSet(fusedPlan, "elidedEdgeKeys"));
        final Map<?, ?> fusedStages = recordMap(fusedPlan, "fusedStages");
        assertEquals(1, fusedStages.size());
        final Object fusedStage = fusedStages.get("normalize");
        assertNotNull(fusedStage);
        assertEquals(List.of("risk", "validate"), recordList(fusedStage, "stageNames"));
        assertEquals("egress", recordValue(recordValue(fusedStage, "sinkPlan"), "sinkName"));
        assertTrue(recordMap(fusedPlan, "inlineSourceBindings").isEmpty());
        assertEquals(Set.of("ingress->normalize"), recordMap(fusedPlan, "senderDecisions").keySet());

        final Object inlinePlan = runtimePlan(buildPipelineGraph("guardrail-inline-plan", true, true));
        assertEquals(List.of("normalize"), recordList(inlinePlan, "workerOrder"));
        final Object inlineBinding = recordMap(inlinePlan, "inlineSourceBindings").get("normalize");
        assertNotNull(inlineBinding);
        assertEquals("normalize", recordValue(inlineBinding, "workerName"));
        assertEquals("ingress", recordValue(inlineBinding, "sourceName"));
        assertEquals("ingress->normalize", recordValue(inlineBinding, "edgeKey"));

        final Object mpscPlan = runtimePlan(buildMpscIngressGraph("guardrail-mpsc-plan", ignored -> { }));
        assertEquals(List.of("egress"), recordList(mpscPlan, "workerOrder"));
        assertTrue(recordMap(mpscPlan, "inlineSourceBindings").isEmpty());
        final Object mpscEdge = recordMap(mpscPlan, "edgeDecisions").get("ingress->egress");
        assertNotNull(mpscEdge);
        assertEquals("MPSC_RING", recordValue(mpscEdge, "implementationKind").toString());
        assertEquals("NORMAL", recordValue(mpscEdge, "useKind").toString());
        assertEquals("egress", recordValue(mpscEdge, "allocationOwner"));
        assertEquals(false, recordValue(mpscEdge, "sourceIngressCloseGuard"));
        final Object mpscSender = recordMap(mpscPlan, "senderDecisions").get("ingress->egress");
        assertNotNull(mpscSender);
        assertEquals("ingress", recordValue(mpscSender, "ownerName"));
        assertEquals("EDGE", recordValue(mpscSender, "senderKind").toString());
        assertEquals("EDGE", recordValue(mpscSender, "outputKind").toString());
    }

    @Test
    void graphEmitSteadyStateDoesNotAllocateAcrossPhysicalFusedAndInlinePaths() {
        final com.sun.management.ThreadMXBean bean = allocationBean();
        if (bean == null) {
            return;
        }

        assertPipelineEmitSteadyStateDoesNotAllocate(bean, GraphPath.PHYSICAL);
        assertPipelineEmitSteadyStateDoesNotAllocate(bean, GraphPath.FUSED);
        assertPipelineEmitSteadyStateDoesNotAllocate(bean, GraphPath.INLINE_FUSED);
    }

    @Test
    void mpscGraphEmitSteadyStateDoesNotAllocate() {
        final com.sun.management.ThreadMXBean bean = allocationBean();
        if (bean == null) {
            return;
        }

        final RunningGraph graph = startMpscIngressGraph("guardrail-mpsc-emit");
        try {
            emitRange(graph, 0, GRAPH_WARMUP_EMITS, false);
            awaitConsumed(graph.consumed(), GRAPH_WARMUP_EMITS - 1L);

            final long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
            emitRange(graph, GRAPH_WARMUP_EMITS, GRAPH_WARMUP_EMITS + GRAPH_MEASURED_EMITS, false);
            final long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;

            awaitConsumed(graph.consumed(), GRAPH_WARMUP_EMITS + GRAPH_MEASURED_EMITS - 1L);
            assertTrue(allocated <= GRAPH_EMIT_ALLOCATION_LIMIT,
                "steady-state graph MPSC source emit path allocated " + allocated + " bytes");
        } finally {
            graph.close();
        }
    }

    @Test
    void preallocatedGraphClaimEmitSteadyStateDoesNotAllocate() {
        final com.sun.management.ThreadMXBean bean = allocationBean();
        if (bean == null) {
            return;
        }

        final RunningPreallocatedGraph graph = startPreallocatedGraph("guardrail-preallocated-emit");
        try {
            preallocatedEmitRange(graph, 0, GRAPH_WARMUP_EMITS);
            awaitConsumed(graph.consumed(), GRAPH_WARMUP_EMITS - 1L);

            final long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
            preallocatedEmitRange(graph, GRAPH_WARMUP_EMITS, GRAPH_WARMUP_EMITS + GRAPH_MEASURED_EMITS);
            final long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;

            awaitConsumed(graph.consumed(), GRAPH_WARMUP_EMITS + GRAPH_MEASURED_EMITS - 1L);
            assertTrue(allocated <= GRAPH_EMIT_ALLOCATION_LIMIT,
                "steady-state preallocated source claim/emit path allocated " + allocated + " bytes");
        } finally {
            graph.close();
        }
    }

    @Test
    void hotPathPackagesDoNotUseReflectionOrLogging() throws Exception {
        final Path root = Path.of("src", "main", "java", "com", "lattice", "internal");
        try (Stream<Path> files = Files.walk(root)) {
            final String joined = files
                .filter(path -> path.toString().endsWith(".java"))
                .filter(path -> !path.toString().contains("jfr"))
                .map(HotPathGuardrailTest::read)
                .reduce("", String::concat);

            assertFalse(joined.contains("java.lang.reflect"));
            assertFalse(joined.contains("Logger"));
            assertFalse(joined.contains("System.out"));
        }
    }

    private static void assertMpscOfferPollSteadyStateDoesNotAllocate(final MemoryMode memoryMode) {
        final com.sun.management.ThreadMXBean bean = allocationBean();
        if (bean == null) {
            return;
        }
        final MpscRingEdge edge = new MpscRingEdge(
            "producer",
            "consumer",
            1024,
            memoryMode,
            edgeMetrics(),
            graphMetrics()
        );
        edge.firstTouch("consumer");
        final Object payload = new Object();

        for (int i = 0; i < 100_000; i++) {
            if (!edge.offer(payload) || edge.poll() != payload) {
                throw new AssertionError("MPSC warmup failed for " + memoryMode.kind());
            }
        }

        long allocated = measuredMpscOfferPollAllocation(bean, edge, payload, memoryMode);
        if (allocated > 1024) {
            allocated = measuredMpscOfferPollAllocation(bean, edge, payload, memoryMode);
        }
        assertTrue(allocated <= 1024,
            "steady-state MPSC hot path allocated " + allocated + " bytes for " + memoryMode.kind());
    }

    private static long measuredMpscOfferPollAllocation(
        final com.sun.management.ThreadMXBean bean,
        final MpscRingEdge edge,
        final Object payload,
        final MemoryMode memoryMode
    ) {
        final long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
        for (int i = 0; i < 250_000; i++) {
            if (!edge.offer(payload) || edge.poll() != payload) {
                throw new AssertionError("MPSC steady-state failed for " + memoryMode.kind());
            }
        }
        return bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;
    }

    private static void assertPipelineEmitSteadyStateDoesNotAllocate(
        final com.sun.management.ThreadMXBean bean,
        final GraphPath path
    ) {
        final RunningGraph graph = startPipelineGraph("guardrail-" + path.graphNameSuffix(), path);
        try {
            emitRange(graph, 0, GRAPH_WARMUP_EMITS, path.inlineExpected());
            awaitConsumed(graph.consumed(), GRAPH_WARMUP_EMITS - 1L);

            final long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
            emitRange(graph, GRAPH_WARMUP_EMITS, GRAPH_WARMUP_EMITS + GRAPH_MEASURED_EMITS, path.inlineExpected());
            final long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;

            awaitConsumed(graph.consumed(), GRAPH_WARMUP_EMITS + GRAPH_MEASURED_EMITS - 1L);
            assertTrue(allocated <= GRAPH_EMIT_ALLOCATION_LIMIT,
                "steady-state graph " + path.label() + " emit path allocated " + allocated + " bytes");
        } finally {
            graph.close();
        }
    }

    private static RunningGraph startPipelineGraph(final String graphName, final GraphPath path) {
        final AtomicLong consumed = new AtomicLong(-1L);
        final StaticGraph graph = buildPipelineGraph(graphName, path.fusionEnabled(), path.inlineSourceFusion(), consumed);
        graph.start();
        return new RunningGraph(graph, graph.emitter("ingress", Signal.class), consumed, new SignalPool());
    }

    private static StaticGraph buildPipelineGraph(
        final String graphName,
        final boolean fusionEnabled,
        final boolean inlineSourceFusion
    ) {
        return buildPipelineGraph(graphName, fusionEnabled, inlineSourceFusion, new AtomicLong());
    }

    private static StaticGraph buildPipelineGraph(
        final String graphName,
        final boolean fusionEnabled,
        final boolean inlineSourceFusion,
        final AtomicLong consumed
    ) {
        return StaticGraph.builder(graphName)
                .fusion(fusionEnabled
                    ? FusionSpec.defaults().inlineSources(inlineSourceFusion)
                    : FusionSpec.disabled())
                .source("ingress", Signal.class, SourceMode.SINGLE_PRODUCER)
                .stage("normalize", Signal.class, Signal.class, HotPathGuardrailTest::increment, GRAPH_STAGE)
                .stage("risk", Signal.class, Signal.class, HotPathGuardrailTest::increment, GRAPH_STAGE)
                .stage("validate", Signal.class, Signal.class, HotPathGuardrailTest::increment, GRAPH_STAGE)
                .sink("egress", Signal.class, signal -> consumed.lazySet(signal.sequence), GRAPH_STAGE)
                .edge("ingress", "normalize", GRAPH_SPSC)
                .edge("normalize", "risk", GRAPH_SPSC)
                .edge("risk", "validate", GRAPH_SPSC)
                .edge("validate", "egress", GRAPH_SPSC)
                .build();
    }

    private static RunningGraph startMpscIngressGraph(final String graphName) {
        final AtomicLong consumed = new AtomicLong(-1L);
        final StaticGraph graph = buildMpscIngressGraph(graphName, signal -> consumed.lazySet(signal.sequence));
        graph.start();
        return new RunningGraph(graph, graph.emitter("ingress", Signal.class), consumed, new SignalPool());
    }

    private static StaticGraph buildMpscIngressGraph(final String graphName, final Consumer<Signal> sink) {
        return StaticGraph.builder(graphName)
                .fusion(FusionSpec.defaults().inlineSources(true))
                .source("ingress", Signal.class, SourceMode.MULTI_PRODUCER)
                .sink("egress", Signal.class, sink, GRAPH_STAGE)
                .edge("ingress", "egress", GRAPH_MPSC)
                .build();
    }

    private static RunningPreallocatedGraph startPreallocatedGraph(final String graphName) {
        final AtomicLong consumed = new AtomicLong(-1L);
        final StaticGraph graph = StaticGraph.builder(graphName)
                .preallocatedSource("ingress", Signal.class,
                    PreallocationSpec.pool(ignored -> new Signal()).poolSize(GRAPH_RING_CAPACITY << 1))
                .sink("egress", Signal.class, signal -> consumed.lazySet(signal.sequence), GRAPH_STAGE)
                .edge("ingress", "egress", GRAPH_SPSC)
                .build();
        graph.start();
        return new RunningPreallocatedGraph(
            graph,
            graph.preallocatedEmitter("ingress", Signal.class),
            consumed
        );
    }

    private static void emitRange(
        final RunningGraph graph,
        final long startInclusive,
        final long endExclusive,
        final boolean requireInlineCompletion
    ) {
        for (long sequence = startInclusive; sequence < endExclusive; sequence++) {
            graph.emitter().emit(graph.pool().next(sequence));
            if (requireInlineCompletion && graph.consumed().get() != sequence) {
                throw new AssertionError("inline fused emit returned before consuming sequence " + sequence);
            }
        }
    }

    private static void preallocatedEmitRange(
        final RunningPreallocatedGraph graph,
        final long startInclusive,
        final long endExclusive
    ) {
        for (long sequence = startInclusive; sequence < endExclusive; sequence++) {
            final Signal signal = graph.emitter().claim();
            signal.sequence = sequence;
            signal.value = sequence;
            graph.emitter().emit(signal);
        }
    }

    private static void awaitConsumed(final AtomicLong consumed, final long expected) {
        final long deadline = System.nanoTime() + GRAPH_CONSUME_TIMEOUT.toNanos();
        while (consumed.get() < expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertTrue(consumed.get() >= expected,
            "timed out waiting for graph to consume sequence " + expected + ", last consumed " + consumed.get());
    }

    private static void increment(
        final Signal signal,
        final com.lattice.stage.Output<Signal> output,
        final Object context
    ) {
        signal.value++;
        output.push(signal);
    }

    private static Object runtimePlan(final StaticGraph graph) {
        try {
            final Field field = graph.getClass().getDeclaredField("runtimePlan");
            field.setAccessible(true);
            return field.get(graph);
        } catch (final ReflectiveOperationException ex) {
            throw new AssertionError(ex);
        }
    }

    private static List<?> recordList(final Object record, final String accessor) {
        return (List<?>) recordValue(record, accessor);
    }

    private static Map<?, ?> recordMap(final Object record, final String accessor) {
        return (Map<?, ?>) recordValue(record, accessor);
    }

    private static Set<?> recordSet(final Object record, final String accessor) {
        return (Set<?>) recordValue(record, accessor);
    }

    private static Object recordValue(final Object record, final String accessor) {
        try {
            final Method method = record.getClass().getDeclaredMethod(accessor);
            method.setAccessible(true);
            return method.invoke(record);
        } catch (final ReflectiveOperationException ex) {
            throw new AssertionError(ex);
        }
    }

    private static String read(final Path path) {
        try {
            return Files.readString(path);
        } catch (final Exception ex) {
            throw new AssertionError(ex);
        }
    }

    private static com.sun.management.ThreadMXBean allocationBean() {
        final java.lang.management.ThreadMXBean platformBean = ManagementFactory.getThreadMXBean();
        if (!(platformBean instanceof com.sun.management.ThreadMXBean bean) || !bean.isThreadAllocatedMemorySupported()) {
            return null;
        }
        bean.setThreadAllocatedMemoryEnabled(true);
        return bean;
    }

    private static EdgeMetrics edgeMetrics() {
        return new EdgeMetrics("producer", "consumer");
    }

    private static GraphMetrics graphMetrics() {
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put("producer", new StageMetrics("producer"));
        stages.put("consumer", new StageMetrics("consumer"));
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put("producer->consumer", new EdgeMetrics("producer", "consumer"));
        return new GraphMetrics("guardrail", stages, edges);
    }

    private enum GraphPath {
        PHYSICAL("physical", false, false, false),
        FUSED("fused", true, false, false),
        INLINE_FUSED("inline fused", true, true, true);

        private final String label;
        private final boolean fusionEnabled;
        private final boolean inlineSourceFusion;
        private final boolean inlineExpected;

        GraphPath(
            final String label,
            final boolean fusionEnabled,
            final boolean inlineSourceFusion,
            final boolean inlineExpected
        ) {
            this.label = label;
            this.fusionEnabled = fusionEnabled;
            this.inlineSourceFusion = inlineSourceFusion;
            this.inlineExpected = inlineExpected;
        }

        String label() {
            return label;
        }

        String graphNameSuffix() {
            return name().toLowerCase(java.util.Locale.ROOT).replace('_', '-');
        }

        boolean fusionEnabled() {
            return fusionEnabled;
        }

        boolean inlineSourceFusion() {
            return inlineSourceFusion;
        }

        boolean inlineExpected() {
            return inlineExpected;
        }
    }

    private record RunningGraph(
        StaticGraph graph,
        Emitter<Signal> emitter,
        AtomicLong consumed,
        SignalPool pool
    ) implements AutoCloseable {
        @Override
        public void close() {
            if (!emitter.isClosed()) {
                emitter.close();
            }
            assertTrue(graph.stop(GRAPH_STOP_TIMEOUT), "guardrail graph did not stop: " + graph.plan().name());
        }
    }

    private record RunningPreallocatedGraph(
        StaticGraph graph,
        PreallocatedEmitter<Signal> emitter,
        AtomicLong consumed
    ) implements AutoCloseable {
        @Override
        public void close() {
            if (!emitter.isClosed()) {
                emitter.close();
            }
            assertTrue(graph.stop(GRAPH_STOP_TIMEOUT), "guardrail graph did not stop: " + graph.plan().name());
        }
    }

    private static final class SignalPool {
        private final Signal[] signals = new Signal[GRAPH_SIGNAL_POOL_SIZE];
        private int cursor;

        SignalPool() {
            for (int i = 0; i < signals.length; i++) {
                signals[i] = new Signal();
            }
        }

        Signal next(final long sequence) {
            final Signal signal = signals[cursor++ & (signals.length - 1)];
            signal.sequence = sequence;
            signal.value = sequence;
            return signal;
        }
    }

    private static final class Signal {
        long sequence;
        long value;
    }
}
