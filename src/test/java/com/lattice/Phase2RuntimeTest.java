package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import com.lattice.metrics.WorkerState;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageExceptionAction;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Phase2RuntimeTest {

    @Test
    void batchAndSingleMessageStagesCoexistInOneGraph() throws Exception {
        final List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = StaticGraph.builder("batch-mixed")
            .source("ingress", Integer.class)
            .batchStage(
                "batch-double",
                Integer.class,
                Integer.class,
                (batch, out, ctx) -> {
                    for (int i = 0; i < batch.size(); i++) {
                        out.push(batch.get(i) * 2);
                    }
                },
                StageSpec.singleThreaded().batch(BatchPolicy.maxItems(8))
            )
            .stage(
                "single-add",
                Integer.class,
                Integer.class,
                (value, out, ctx) -> out.push(value + 1),
                StageSpec.singleThreaded()
            )
            .sink("egress", Integer.class, consumed::add, StageSpec.singleThreaded())
            .edge("ingress", "batch-double", EdgeSpec.mpscRing(64).batch(BatchPolicy.maxItems(8)))
            .edge("batch-double", "single-add", EdgeSpec.spscRing(64).batch(BatchPolicy.maxItems(8)))
            .edge("single-add", "egress", EdgeSpec.spscRing(64))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 32; i++) {
            ingress.emit(i);
        }
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(32, consumed.size());
        assertTrue(consumed.contains(63));
        assertTrue(graph.metrics().stage("batch-double").batchesProcessed() > 0);
        assertEquals(32, graph.metrics().stage("batch-double").processedMessages());
    }

    @Test
    void timedBackpressureReturnsFalseAndRecordsOverload() throws Exception {
        final CountDownLatch sinkEntered = new CountDownLatch(1);
        final CountDownLatch releaseSink = new CountDownLatch(1);
        final StaticGraph graph = StaticGraph.builder("timed-backpressure")
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
            .edge("ingress", "egress", EdgeSpec.mpscRing(2).overflow(OverflowPolicy.block()))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);
        assertTrue(sinkEntered.await(5, TimeUnit.SECONDS));
        ingress.emit(2);
        ingress.emit(3);

        assertFalse(ingress.emit(4, Duration.ofMillis(10)));
        assertTrue(graph.metrics().overloadActivations() > 0);
        assertTrue(graph.metrics().stage("ingress").blockedOutputs() > 0);
        assertTrue(graph.metrics().backpressureNanos() > 0);

        releaseSink.countDown();
        ingress.close();
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
    }

    @Test
    void quiesceAndResumeAreDeterministic() throws Exception {
        final List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = StaticGraph.builder("quiesce")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, consumed::add, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(32))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 10; i++) {
            ingress.emit(i);
        }

        assertTrue(graph.quiesce(Duration.ofSeconds(5)));
        assertEquals(GraphState.QUIESCING, graph.state());
        assertFalse(ingress.tryEmit(10));
        assertThrows(GraphRuntimeException.class, () -> ingress.emit(10));

        graph.resume();
        assertEquals(GraphState.RUNNING, graph.state());
        ingress.emit(10);
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(11, consumed.size());
    }

    @Test
    void exceptionHandlerCanPoisonOnlyTheFailingStage() throws Exception {
        final StaticGraph graph = StaticGraph.builder("poison")
            .exceptionHandler((graphName, stageName, failure, context) -> StageExceptionAction.POISON_STAGE)
            .source("ingress", Integer.class)
            .stage("explode", Integer.class, Integer.class, (value, out, ctx) -> {
                throw new IllegalStateException("boom");
            }, StageSpec.singleThreaded())
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "explode", EdgeSpec.mpscRing(8))
            .edge("explode", "egress", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        graph.emitter("ingress", Integer.class).emit(1);

        assertEventually(() -> graph.metrics().stage("explode").workerState() == WorkerState.POISONED, Duration.ofSeconds(5));
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(1, graph.metrics().stage("explode").stageExceptions());
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
