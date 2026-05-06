package io.github.elevateddev.lattice;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.FusionSpec;
import io.github.elevateddev.lattice.graph.GraphBuildException;
import io.github.elevateddev.lattice.graph.GraphRuntimeException;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.graph.PreallocationSpec;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.internal.edge.MpscRingEdge;
import io.github.elevateddev.lattice.internal.edge.SpscRingEdge;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import io.github.elevateddev.lattice.metrics.StageMetrics;
import io.github.elevateddev.lattice.placement.MemoryMode;
import io.github.elevateddev.lattice.routing.JoinGroup;
import io.github.elevateddev.lattice.routing.JoinSpec;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.PreallocatedEmitter;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenSourceSanityTest {

    @Test
    void publicApiRejectsInvalidBuilderAndSpecInputs() {
        assertThrows(NullPointerException.class, () -> graph(null));
        assertThrows(IllegalArgumentException.class, () -> graph(" "));
        assertThrows(IllegalArgumentException.class, () -> EdgeSpec.mpscRing(0));
        assertThrows(NullPointerException.class, () -> EdgeSpec.mpscRing(8).overflow(null));
        assertThrows(IllegalArgumentException.class, () -> PreallocationSpec.fixedPool(new Reusable[0]));
        assertThrows(IllegalArgumentException.class, () -> JoinSpec.allOf(group -> 1).capacity(0));

        assertThrows(GraphBuildException.class, () -> graph("missing-source")
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .build());
        assertThrows(GraphBuildException.class, () -> graph("unknown-edge-target")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "missing", EdgeSpec.mpscRing(8))
            .build());
        assertThrows(GraphBuildException.class, () -> graph("duplicate-edge")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build());
    }

    @Test
    void preallocatedEmitterEnforcesClaimOwnershipAndFixedPoolReuse() {
        final Reusable[] pool = new Reusable[128];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new Reusable(i);
        }
        final StaticGraph graph = graph("fixed-preallocated")
            .preallocatedSource("ingress", Reusable.class, PreallocationSpec.fixedPool(pool))
            .sink("egress", Reusable.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();

        final PreallocatedEmitter<Reusable> ingress = graph.preallocatedEmitter("ingress", Reusable.class);
        final Reusable first = ingress.claim();
        assertSame(pool[0], first);
        assertThrows(GraphRuntimeException.class, () -> ingress.emit(new Reusable(-1)));
        assertThrows(GraphRuntimeException.class, () -> ingress.tryEmit(pool[1]));
        assertThrows(GraphRuntimeException.class, () -> ingress.discard(pool[1]));
        ingress.discard(first);

        final Set<Reusable> claimed = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int i = 0; i < pool.length; i++) {
            final Reusable item = ingress.claim();
            claimed.add(item);
            ingress.discard(item);
        }
        assertEquals(pool.length, claimed.size());
        for (final Reusable item : pool) {
            assertTrue(claimed.contains(item));
        }
        ingress.close();
        assertTrue(graph.stop(Duration.ofSeconds(1)));
    }

    @Test
    void spscAndMpscEdgesPreserveExactlyOnceUnderContention() throws Exception {
        assertSpscContentionPreservesFifo();
        assertMpscContentionPreservesPerProducerFifo();
    }

    @Test
    void stageFusionIsSemanticallyEquivalentToPhysicalPipeline() throws Exception {
        final PipelineResult physical = runPipelineWithFusion(false);
        final PipelineResult fused = runPipelineWithFusion(true);

        assertIterableEquals(physical.outputs(), fused.outputs());
        assertEquals(physical.normalizeConsumed(), fused.normalizeConsumed());
        assertEquals(physical.validateConsumed(), fused.validateConsumed());
        assertEquals(physical.egressConsumed(), fused.egressConsumed());
    }

    @Test
    void joinLongStampPathCorrelatesOutOfOrderBranchesWithoutObjectStampExtractor() throws Exception {
        final List<String> joined = Collections.synchronizedList(new ArrayList<>());
        final JoinSpec<String> spec = JoinSpec.<String>allOf(OpenSourceSanityTest::formatLongStampJoin)
            .stampLong(item -> ((JoinItem) item).stamp());
        assertTrue(spec.longStamp());

        final StaticGraph graph = graph("long-stamp-join")
            .source("left", JoinItem.class)
            .source("right", JoinItem.class)
            .join("join", String.class, spec, StageSpec.singleThreaded())
            .sink("egress", String.class, joined::add, StageSpec.singleThreaded())
            .edge("left", "join", EdgeSpec.mpscRing(16))
            .edge("right", "join", EdgeSpec.mpscRing(16))
            .edge("join", "egress", EdgeSpec.spscRing(16))
            .build();

        graph.start();
        graph.emitter("left", JoinItem.class).emit(new JoinItem(1, "l1"));
        graph.emitter("right", JoinItem.class).emit(new JoinItem(2, "r2"));
        graph.emitter("left", JoinItem.class).emit(new JoinItem(2, "l2"));
        graph.emitter("right", JoinItem.class).emit(new JoinItem(1, "r1"));
        graph.emitter("left", JoinItem.class).close();
        graph.emitter("right", JoinItem.class).close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        final List<String> sorted = new ArrayList<>(joined);
        Collections.sort(sorted);
        assertEquals(List.of("1:l1/r1", "2:l2/r2"), sorted);
        assertEquals(2, graph.metrics().stage("join").completedJoinGroups());
    }

    @Test
    void missingBranchDiscardPolicyDropsIncompleteGroupsAndRecordsThem() throws Exception {
        final List<String> joined = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("join-discard-missing")
            .stampedSource("left", String.class)
            .stampedSource("right", String.class)
            .join("join", String.class,
                JoinSpec.allOf(group -> "unexpected").missingBranches(JoinSpec.MissingBranchPolicy.DISCARD),
                StageSpec.singleThreaded())
            .sink("egress", String.class, joined::add, StageSpec.singleThreaded())
            .edge("left", "join", EdgeSpec.mpscRing(8))
            .edge("right", "join", EdgeSpec.mpscRing(8))
            .edge("join", "egress", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        graph.emitter("left", String.class).emit("left-only");
        graph.emitter("left", String.class).close();
        graph.emitter("right", String.class).close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertTrue(joined.isEmpty());
        assertEquals(0, graph.metrics().stage("join").completedJoinGroups());
        assertEquals(1, graph.metrics().stage("join").missingJoinBranches());
    }

    @Test
    void metricsCountersFollowConfiguredHotCounterContract() {
        final MetricsSpec metricsSpec = MetricsSpec.off().hotCounters(true);
        final StageMetrics stage = new StageMetrics("stage", metricsSpec);
        final EdgeMetrics edge = new EdgeMetrics("from", "to", "", MemoryMode.MemoryKind.ON_HEAP_SLOTS, metricsSpec);
        final GraphMetrics graph = graphMetrics("metrics", stage, edge, metricsSpec);

        stage.recordConsume();
        stage.recordBatch(3, 10);
        edge.recordEmit();
        edge.recordConsume();
        graph.recordEmit();
        graph.recordConsume();

        assertEquals(1, stage.consumedCount());
        assertEquals(3, stage.processedMessages());
        assertEquals(1, edge.emittedCount());
        assertEquals(1, edge.consumedCount());
        assertEquals(1, graph.emittedCount());
        assertEquals(1, graph.consumedCount());

        stage.recordException();
        edge.recordFailedOffer();
        graph.recordFailedOffer();
        assertEquals(1, stage.stageExceptions());
        assertEquals(1, edge.failedOffers());
        assertEquals(1, graph.failedOffers());
    }

    @Test
    void lifecycleStopBeforeStartPreventsLaterEmissionOrRestart() {
        final StaticGraph graph = graph("lifecycle-before-start")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();

        assertTrue(graph.stop(Duration.ofSeconds(1)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertNotNull(graph.metrics().stopTime());

        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        assertFalse(ingress.tryEmit(1));
        assertThrows(GraphRuntimeException.class, () -> ingress.emit(1));
        assertThrows(GraphRuntimeException.class, graph::start);
    }

    @Test
    void abortBeforeStartIsStoppedAndIdempotent() {
        final StaticGraph graph = graph("abort-before-start")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();

        graph.abort();
        graph.abort();

        assertEquals(GraphState.STOPPED, graph.state());
        assertNotNull(graph.metrics().stopTime());
        assertTrue(graph.failure().isEmpty());
    }

    private static void assertSpscContentionPreservesFifo() throws Exception {
        final int total = 10_000;
        final SpscRingEdge edge = new SpscRingEdge("source", "sink", 64, edgeMetrics(), graphMetrics());
        final ExecutorService executor = Executors.newFixedThreadPool(2);

        final Future<?> producer = executor.submit(() -> {
            for (int i = 0; i < total; i++) {
                while (!edge.offer(i)) {
                    Thread.onSpinWait();
                }
            }
        });
        final Future<List<Integer>> consumer = executor.submit(() -> {
            final List<Integer> consumed = new ArrayList<>(total);
            while (consumed.size() < total) {
                final Object item = edge.poll();
                if (item == null) {
                    Thread.onSpinWait();
                } else {
                    consumed.add((Integer) item);
                }
            }
            return consumed;
        });

        producer.get(5, TimeUnit.SECONDS);
        final List<Integer> consumed = consumer.get(5, TimeUnit.SECONDS);
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        for (int i = 0; i < total; i++) {
            assertEquals(i, consumed.get(i));
        }
        assertTrue(edge.isEmpty());
    }

    private static void assertMpscContentionPreservesPerProducerFifo() throws Exception {
        final int producers = 4;
        final int perProducer = 512;
        final int total = producers * perProducer;
        final MpscRingEdge edge = new MpscRingEdge(
            "source",
            "sink",
            128,
            MemoryMode.onHeapSlots(),
            edgeMetrics(),
            graphMetrics()
        );
        edge.firstTouch("sink");
        final ExecutorService executor = Executors.newFixedThreadPool(producers + 1);
        final CountDownLatch ready = new CountDownLatch(producers);
        final AtomicReference<String> orderingFailure = new AtomicReference<>();
        final AtomicInteger consumedCount = new AtomicInteger();
        final int[] nextExpectedByProducer = new int[producers];

        final List<Future<?>> producerFutures = new ArrayList<>();
        for (int producer = 0; producer < producers; producer++) {
            final int producerId = producer;
            producerFutures.add(executor.submit(() -> {
                ready.countDown();
                try {
                    assertTrue(ready.await(5, TimeUnit.SECONDS));
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
                for (int sequence = 0; sequence < perProducer; sequence++) {
                    final int encoded = (producerId << 16) | sequence;
                    while (!edge.offer(encoded)) {
                        Thread.onSpinWait();
                    }
                }
            }));
        }

        final Future<?> consumer = executor.submit(() -> {
            while (consumedCount.get() < total) {
                final Object item = edge.poll();
                if (item == null) {
                    Thread.onSpinWait();
                    continue;
                }
                final int encoded = (Integer) item;
                final int producer = encoded >>> 16;
                final int sequence = encoded & 0xFFFF;
                if (sequence != nextExpectedByProducer[producer]) {
                    orderingFailure.compareAndSet(null,
                        "producer " + producer + " expected " + nextExpectedByProducer[producer]
                            + " but saw " + sequence);
                }
                nextExpectedByProducer[producer]++;
                consumedCount.incrementAndGet();
            }
        });

        consumer.get(10, TimeUnit.SECONDS);
        for (final Future<?> producerFuture : producerFutures) {
            producerFuture.get(5, TimeUnit.SECONDS);
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertNull(orderingFailure.get());
        assertEquals(total, consumedCount.get());
        assertTrue(edge.isEmpty());
        assertTrue(Arrays.stream(nextExpectedByProducer).allMatch(value -> value == perProducer));
    }

    private static PipelineResult runPipelineWithFusion(final boolean enabled) throws Exception {
        final List<Integer> outputs = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph(enabled ? "fused-equivalence" : "physical-equivalence")
                .fusion(enabled ? FusionSpec.defaults() : FusionSpec.disabled())
                .metrics(MetricsSpec.off().hotCounters(true).fusedLogicalEdgeCounters(true))
                .source("ingress", Integer.class)
                .stage("normalize", Integer.class, Integer.class, (value, out, ctx) -> out.push(value + 1),
                    StageSpec.singleThreaded())
                .stage("validate", Integer.class, Integer.class, (value, out, ctx) -> {
                    if ((value & 1) == 0) {
                        out.push(value * 10);
                    }
                }, StageSpec.singleThreaded())
                .sink("egress", Integer.class, outputs::add, StageSpec.singleThreaded())
                .edge("ingress", "normalize", EdgeSpec.mpscRing(64))
                .edge("normalize", "validate", EdgeSpec.spscRing(64))
                .edge("validate", "egress", EdgeSpec.spscRing(64))
                .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 20; i++) {
            ingress.emit(i);
        }
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        return new PipelineResult(
            List.copyOf(outputs),
            graph.metrics().stage("normalize").consumedCount(),
            graph.metrics().stage("validate").consumedCount(),
            graph.metrics().stage("egress").consumedCount()
        );
    }

    private static String formatLongStampJoin(final JoinGroup group) {
        final JoinItem left = group.value("left", JoinItem.class).orElseThrow();
        final JoinItem right = group.value("right", JoinItem.class).orElseThrow();
        return group.longStamp() + ":" + left.value() + "/" + right.value();
    }

    private static EdgeMetrics edgeMetrics() {
        return new EdgeMetrics("source", "sink", "", MemoryMode.MemoryKind.ON_HEAP_SLOTS,
            MetricsSpec.off().hotCounters(true));
    }

    private static StaticGraph.Builder graph(final String name) {
        return StaticGraph.builder(name)
            .metrics(MetricsSpec.off().hotCounters(true).fusedLogicalEdgeCounters(true));
    }

    private static GraphMetrics graphMetrics() {
        final MetricsSpec metricsSpec = MetricsSpec.off().hotCounters(true);
        final StageMetrics source = new StageMetrics("source", metricsSpec);
        final StageMetrics sink = new StageMetrics("sink", metricsSpec);
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put(source.name(), source);
        stages.put(sink.name(), sink);
        final EdgeMetrics edge = new EdgeMetrics("source", "sink", "", MemoryMode.MemoryKind.ON_HEAP_SLOTS,
            metricsSpec);
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put(edge.from() + "->" + edge.to(), edge);
        return new GraphMetrics("edge-test", stages, edges, metricsSpec);
    }

    private static GraphMetrics graphMetrics(
        final String name,
        final StageMetrics stage,
        final EdgeMetrics edge
    ) {
        return graphMetrics(name, stage, edge, MetricsSpec.off());
    }

    private static GraphMetrics graphMetrics(
        final String name,
        final StageMetrics stage,
        final EdgeMetrics edge,
        final MetricsSpec metricsSpec
    ) {
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put(stage.name(), stage);
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put(edge.from() + "->" + edge.to(), edge);
        return new GraphMetrics(name, stages, edges, metricsSpec);
    }

    private record Reusable(int id) {
    }

    private record JoinItem(long stamp, String value) {
    }

    private record PipelineResult(
        List<Integer> outputs,
        long normalizeConsumed,
        long validateConsumed,
        long egressConsumed
    ) {
    }
}
