package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.routing.JoinSpec;
import com.lattice.routing.Stamped;
import com.lattice.slab.SlabHandle;
import com.lattice.slab.SlabPool;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageExceptionAction;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeRegressionTest {

    @Test
    void dropOldestDoesNotCountDroppedItemsAsConsumedAndPreservesSurvivorOrder() throws Exception {
        final CountDownLatch firstEntered = new CountDownLatch(1);
        final CountDownLatch releaseFirst = new CountDownLatch(1);
        final List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = StaticGraph.builder("drop-oldest-metrics")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, value -> {
                firstEntered.countDown();
                await(releaseFirst);
                consumed.add(value);
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(2).overflow(OverflowPolicy.dropOldest()))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);
        assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
        ingress.emit(2);
        ingress.emit(3);
        ingress.emit(4);
        releaseFirst.countDown();
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(List.of(1, 3, 4), List.copyOf(consumed));
        assertEquals(consumed.size(), graph.metrics().edge("ingress", "egress").consumedCount());
        assertEquals(1, graph.metrics().edge("ingress", "egress").droppedOldest());
    }

    @Test
    void abortReleasesQueuedSlabHandlePayloads() throws Exception {
        final SlabPool<String> plainPool = new SlabPool<>("abort-plain", 2);
        final CountDownLatch plainEntered = new CountDownLatch(1);
        final CountDownLatch releasePlain = new CountDownLatch(1);
        final StaticGraph plain = StaticGraph.builder("abort-plain")
            .source("ingress", SlabHandle.class)
            .sink("egress", SlabHandle.class, ignored -> {
                plainEntered.countDown();
                await(releasePlain);
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(2))
            .build();

        plain.start();
        plain.emitter("ingress", SlabHandle.class).emit(plainPool.acquire("active"));
        assertTrue(plainEntered.await(5, TimeUnit.SECONDS));
        plain.emitter("ingress", SlabHandle.class).emit(plainPool.acquire("queued"));
        plain.abort();
        releasePlain.countDown();
        assertTrue(plain.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(0, plainPool.leakedCount());
    }

    @Test
    void abortReleasesQueuedStampedSlabHandlePayloads() throws Exception {
        final SlabPool<String> stampedPool = new SlabPool<>("abort-stamped", 2);
        final CountDownLatch stampedEntered = new CountDownLatch(1);
        final CountDownLatch releaseStamped = new CountDownLatch(1);
        final StaticGraph stamped = StaticGraph.builder("abort-stamped")
            .stampedSource("ingress", SlabHandle.class)
            .sink("egress", Stamped.class, ignored -> {
                stampedEntered.countDown();
                await(releaseStamped);
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(2))
            .build();

        stamped.start();
        stamped.emitter("ingress", SlabHandle.class).emit(stampedPool.acquire("active"));
        assertTrue(stampedEntered.await(5, TimeUnit.SECONDS));
        stamped.emitter("ingress", SlabHandle.class).emit(stampedPool.acquire("queued"));
        stamped.abort();
        releaseStamped.countDown();
        assertTrue(stamped.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(0, stampedPool.leakedCount());
    }

    @Test
    void poisonReleasesQueuedSlabHandlePayloads() throws Exception {
        final SlabPool<String> pool = new SlabPool<>("poison-release", 2);
        final StaticGraph graph = StaticGraph.builder("poison-release")
            .exceptionHandler((graphName, stageName, failure, context) -> StageExceptionAction.POISON_STAGE)
            .source("ingress", SlabHandle.class)
            .stage("explode", SlabHandle.class, SlabHandle.class, (handle, out, ctx) -> {
                throw new IllegalStateException("poison");
            }, StageSpec.singleThreaded())
            .sink("egress", SlabHandle.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "explode", EdgeSpec.mpscRing(2))
            .edge("explode", "egress", EdgeSpec.spscRing(2))
            .build();

        graph.start();
        graph.emitter("ingress", SlabHandle.class).emit(pool.acquire("active"));
        graph.emitter("ingress", SlabHandle.class).emit(pool.acquire("queued"));

        assertEventually(() -> graph.state() == GraphState.STOPPED, Duration.ofSeconds(5));
        assertEquals(0, pool.leakedCount());
    }

    @Test
    void anyOfDuplicateFailRejectsLaterSourceForAlreadyEmittedStamp() throws Exception {
        final StaticGraph graph = StaticGraph.builder("any-duplicate-fail")
            .stampedSource("left", String.class)
            .stampedSource("right", String.class)
            .join("join", String.class, JoinSpec.anyOf(group -> group.triggeringSource())
                .duplicates(JoinSpec.DuplicatePolicy.FAIL), StageSpec.singleThreaded())
            .sink("egress", String.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("left", "join", EdgeSpec.mpscRing(8))
            .edge("right", "join", EdgeSpec.mpscRing(8))
            .edge("join", "egress", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        graph.emitter("left", String.class).emit("l0");
        graph.emitter("right", String.class).emit("r0");
        graph.emitter("left", String.class).close();
        graph.emitter("right", String.class).close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(1)));
        assertEquals(GraphState.FAILED, graph.state());
        assertTrue(graph.failure().isPresent());
        assertEquals(1, graph.metrics().stage("join").duplicateJoinStamps());
    }

    @Test
    void anyOfDuplicateCountAndIgnoreAreObservableWithoutFailingGraph() throws Exception {
        assertAnyOfDuplicatePolicy(JoinSpec.DuplicatePolicy.COUNT, 1);
        assertAnyOfDuplicatePolicy(JoinSpec.DuplicatePolicy.IGNORE, 0);
    }

    @Test
    void passThroughStageKeepsSlabPermitUntilDownstreamSinkConsumesHandle() throws Exception {
        final SlabPool<String> pool = new SlabPool<>("pass-through", 1);
        final CountDownLatch sinkEntered = new CountDownLatch(1);
        final CountDownLatch releaseSink = new CountDownLatch(1);
        final StaticGraph graph = StaticGraph.builder("pass-through")
            .source("ingress", SlabHandle.class)
            .stage("identity", SlabHandle.class, SlabHandle.class, (handle, out, ctx) -> out.push(handle),
                StageSpec.singleThreaded())
            .sink("egress", SlabHandle.class, ignored -> {
                sinkEntered.countDown();
                assertEquals(0, pool.availablePermits());
                await(releaseSink);
            }, StageSpec.singleThreaded())
            .edge("ingress", "identity", EdgeSpec.mpscRing(2))
            .edge("identity", "egress", EdgeSpec.spscRing(2))
            .build();

        graph.start();
        graph.emitter("ingress", SlabHandle.class).emit(pool.acquire("payload"));
        assertTrue(sinkEntered.await(5, TimeUnit.SECONDS));
        releaseSink.countDown();
        graph.emitter("ingress", SlabHandle.class).close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(0, pool.leakedCount());
        assertEquals(1, pool.availablePermits());
    }

    @Test
    void quiesceDoesNotReturnWhileSinkIsActivelyProcessingItem() throws Exception {
        final CountDownLatch sinkEntered = new CountDownLatch(1);
        final CountDownLatch releaseSink = new CountDownLatch(1);
        final StaticGraph graph = StaticGraph.builder("quiesce-active")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, ignored -> {
                sinkEntered.countDown();
                await(releaseSink);
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(4))
            .build();

        graph.start();
        graph.emitter("ingress", Integer.class).emit(1);
        assertTrue(sinkEntered.await(5, TimeUnit.SECONDS));
        final CompletableFuture<Boolean> quiesced = CompletableFuture.supplyAsync(
            () -> graph.quiesce(Duration.ofSeconds(5)));

        Thread.sleep(100);
        assertFalse(quiesced.isDone());
        releaseSink.countDown();
        assertTrue(quiesced.get(5, TimeUnit.SECONDS));
        graph.emitter("ingress", Integer.class).close();
        graph.stop(Duration.ofSeconds(5));
    }

    @Test
    void abortWhileOutputIsBackpressuredDoesNotFailGraph() throws Exception {
        final CountDownLatch sinkEntered = new CountDownLatch(1);
        final CountDownLatch releaseSink = new CountDownLatch(1);
        final StaticGraph graph = StaticGraph.builder("abort-backpressured-output")
            .source("ingress", Integer.class)
            .stage("identity", Integer.class, Integer.class, (value, out, ctx) -> out.push(value),
                StageSpec.singleThreaded())
            .sink("egress", Integer.class, ignored -> {
                sinkEntered.countDown();
                await(releaseSink);
            }, StageSpec.singleThreaded())
            .edge("ingress", "identity", EdgeSpec.mpscRing(8))
            .edge("identity", "egress", EdgeSpec.spscRing(1).overflow(OverflowPolicy.block()))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);
        assertTrue(sinkEntered.await(5, TimeUnit.SECONDS));
        ingress.emit(2);
        ingress.emit(3);

        assertEventually(() -> graph.metrics().stage("identity").blockedOutputs() > 0, Duration.ofSeconds(5));
        graph.abort();
        releaseSink.countDown();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertTrue(graph.failure().isEmpty());
    }

    @Test
    void metricsRecordingIgnoresOversizedLatencyValues() {
        final StageMetrics stage = new StageMetrics("stage");
        final EdgeMetrics edge = new EdgeMetrics("from", "to");

        assertDoesNotThrow(() -> stage.recordBatch(1, Long.MAX_VALUE));
        assertDoesNotThrow(() -> edge.recordResidenceNanos(Long.MAX_VALUE));
    }

    private static void assertAnyOfDuplicatePolicy(final JoinSpec.DuplicatePolicy policy, final long duplicates)
        throws Exception {
        final List<String> joined = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = StaticGraph.builder("any-duplicate-" + policy.name().toLowerCase())
            .stampedSource("left", String.class)
            .stampedSource("right", String.class)
            .join("join", String.class, JoinSpec.anyOf(group -> group.triggeringSource() + ":" + group.stamp())
                .duplicates(policy), StageSpec.singleThreaded())
            .sink("egress", String.class, joined::add, StageSpec.singleThreaded())
            .edge("left", "join", EdgeSpec.mpscRing(8))
            .edge("right", "join", EdgeSpec.mpscRing(8))
            .edge("join", "egress", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        graph.emitter("left", String.class).emit("l0");
        graph.emitter("right", String.class).emit("r0");
        graph.emitter("left", String.class).close();
        graph.emitter("right", String.class).close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(List.of("left:0"), List.copyOf(joined));
        assertEquals(duplicates, graph.metrics().stage("join").duplicateJoinStamps());
    }

    private static void await(final CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError(ex);
        }
    }

    private static void assertEventually(final java.util.function.BooleanSupplier condition, final Duration timeout)
        throws InterruptedException {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean());
    }
}
