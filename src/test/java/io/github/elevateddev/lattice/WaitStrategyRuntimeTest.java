package io.github.elevateddev.lattice;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.edge.OverflowPolicy;
import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.StageSpec;
import io.github.elevateddev.lattice.wait.WaitSpec;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WaitStrategyRuntimeTest {

    private static final MetricsSpec TEST_METRICS = MetricsSpec.off().hotCounters(true);

    @Test
    void blockingWorkerWaitRecordsParkMetricsWithoutLosingDelivery() throws Exception {
        final CountDownLatch consumed = new CountDownLatch(1);
        final StaticGraph graph = graph("blocking-worker-wait")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, ignored -> consumed.countDown(),
                StageSpec.singleThreaded().wait(WaitSpec.blocking()))
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();

        graph.start();
        assertEventually(() -> graph.metrics().stage("egress").parkCount() > 0, Duration.ofSeconds(5));

        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);
        ingress.close();

        assertTrue(consumed.await(5, TimeUnit.SECONDS));
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertTrue(graph.metrics().stage("egress").parkCount() > 0);
    }

    @Test
    void phasedProducerBackpressureRecordsSpinYieldAndParkMetrics() throws Exception {
        final CountDownLatch sinkEntered = new CountDownLatch(1);
        final CountDownLatch releaseSink = new CountDownLatch(1);
        final StaticGraph graph = graph("phased-producer-wait")
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
            .edge("ingress", "egress", EdgeSpec.mpscRing(2)
                .wait(WaitSpec.phased(1, 1, Duration.ofNanos(1)))
                .overflow(OverflowPolicy.block()))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);
        assertTrue(sinkEntered.await(5, TimeUnit.SECONDS));
        ingress.emit(2);
        ingress.emit(3);

        assertFalse(ingress.emit(4, Duration.ofMillis(25)));
        assertTrue(graph.metrics().stage("ingress").spinCount() > 0);
        assertTrue(graph.metrics().stage("ingress").yieldCount() > 0);
        assertTrue(graph.metrics().stage("ingress").parkCount() > 0);
        assertTrue(graph.metrics().edge("ingress", "egress").spinCount() > 0);
        assertTrue(graph.metrics().edge("ingress", "egress").yieldCount() > 0);
        assertTrue(graph.metrics().edge("ingress", "egress").parkCount() > 0);

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

    private static StaticGraph.Builder graph(final String name) {
        return StaticGraph.builder(name).metrics(TEST_METRICS);
    }
}
