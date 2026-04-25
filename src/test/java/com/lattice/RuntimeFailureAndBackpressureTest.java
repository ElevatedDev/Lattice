package com.lattice;

import com.lattice.edge.BackpressureException;
import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeFailureAndBackpressureTest {

    @Test
    void stageExceptionFailsGraphAndAbortsWorkers() throws Exception {
        final StaticGraph graph = StaticGraph.builder("failure")
            .source("ingress", Integer.class)
            .stage("explode", Integer.class, Integer.class, (value, out, ctx) -> {
                throw new IllegalStateException("boom-" + value);
            }, StageSpec.singleThreaded())
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "explode", EdgeSpec.mpscRing(8))
            .edge("explode", "egress", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        graph.emitter("ingress", Integer.class).emit(1);

        assertEventually(() -> graph.state() == GraphState.FAILED, Duration.ofSeconds(5));
        assertTrue(graph.failure().isPresent());
        assertEquals(1, graph.metrics().stage("explode").stageExceptions());
    }

    @Test
    void failFastOverflowRejectsWhenEdgeIsFull() throws Exception {
        final CountDownLatch sinkEntered = new CountDownLatch(1);
        final CountDownLatch releaseSink = new CountDownLatch(1);
        final StaticGraph graph = StaticGraph.builder("backpressure")
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
