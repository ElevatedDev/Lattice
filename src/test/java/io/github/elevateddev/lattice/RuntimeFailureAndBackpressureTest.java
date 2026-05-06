package io.github.elevateddev.lattice;

import io.github.elevateddev.lattice.edge.BackpressureException;
import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.edge.OverflowPolicy;
import io.github.elevateddev.lattice.graph.GraphRuntimeException;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeFailureAndBackpressureTest {

    private static final MetricsSpec TEST_METRICS = MetricsSpec.off().hotCounters(true);

    @Test
    void stageExceptionFailsGraphAndAbortsWorkers() throws Exception {
        final StaticGraph graph = graph("failure")
            .source("ingress", Integer.class)
            .stage("explode", Integer.class, Integer.class, (value, out, ctx) -> {
                throw new IllegalStateException("boom-" + value);
            }, StageSpec.singleThreaded())
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "explode", EdgeSpec.mpscRing(8))
            .edge("explode", "egress", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);

        assertEventually(() -> graph.state() == GraphState.FAILED, Duration.ofSeconds(5));
        assertEventually(ingress::isClosed, Duration.ofSeconds(5));
        assertTrue(graph.failure().isPresent());
        assertEquals(1, graph.metrics().stage("explode").stageExceptions());
    }

    @Test
    void failFastOverflowRejectsWhenEdgeIsFull() throws Exception {
        final CountDownLatch sinkEntered = new CountDownLatch(1);
        final CountDownLatch releaseSink = new CountDownLatch(1);
        final StaticGraph graph = graph("backpressure")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, ignored -> {
                sinkEntered.countDown();
                try {
                    assertTrue(releaseSink.await(5, TimeUnit.SECONDS));
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(2).overflow(OverflowPolicy.failFast()))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);
        assertTrue(sinkEntered.await(5, TimeUnit.SECONDS));
        ingress.emit(2);
        ingress.emit(3);

        assertThrows(BackpressureException.class, () -> ingress.emit(4));
        assertTrue(graph.metrics().failedOffers() > 0);

        releaseSink.countDown();
        ingress.close();
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
    }

    @Test
    void blockForOverflowThrowsOnEmitTimeoutAndGraphDrainsAcceptedWork() throws Exception {
        final CountDownLatch sinkEntered = new CountDownLatch(1);
        final CountDownLatch releaseSink = new CountDownLatch(1);
        final List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("block-for-backpressure")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, value -> {
                sinkEntered.countDown();
                try {
                    assertTrue(releaseSink.await(5, TimeUnit.SECONDS));
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
                consumed.add(value);
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(2).overflow(OverflowPolicy.blockFor(Duration.ofMillis(10))))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);
        assertTrue(sinkEntered.await(5, TimeUnit.SECONDS));
        ingress.emit(2);
        ingress.emit(3);

        assertThrows(BackpressureException.class, () -> ingress.emit(4));
        assertEquals(GraphState.RUNNING, graph.state());
        assertTrue(graph.metrics().overloadActivations() > 0);
        assertTrue(graph.metrics().stage("ingress").blockedOutputs() > 0);
        assertTrue(graph.metrics().edge("ingress", "egress").blockedOffers() > 0);
        assertTrue(graph.metrics().backpressureNanos() > 0);

        releaseSink.countDown();
        ingress.close();
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(List.of(1, 2, 3), List.copyOf(consumed));
    }

    @Test
    void abortWakesProducerBlockedOnFullIngressEdge() throws Exception {
        final CountDownLatch sinkEntered = new CountDownLatch(1);
        final CountDownLatch releaseSink = new CountDownLatch(1);
        final StaticGraph graph = graph("abort-blocked-producer")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, ignored -> {
                sinkEntered.countDown();
                try {
                    releaseSink.await(30, TimeUnit.SECONDS);
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(2).overflow(OverflowPolicy.block()))
            .build();
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);
        assertTrue(sinkEntered.await(5, TimeUnit.SECONDS));
        ingress.emit(2);
        ingress.emit(3);

        final Future<?> blockedEmit = executor.submit(() -> {
            ingress.emit(4);
            return null;
        });
        assertEventually(() -> graph.metrics().stage("ingress").blockedOutputs() > 0, Duration.ofSeconds(5));

        graph.abort();
        releaseSink.countDown();

        final ExecutionException failure = assertThrows(ExecutionException.class,
            () -> blockedEmit.get(5, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof GraphRuntimeException);
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
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

    private static StaticGraph.Builder graph(final String name) {
        return StaticGraph.builder(name).metrics(TEST_METRICS);
    }
}
