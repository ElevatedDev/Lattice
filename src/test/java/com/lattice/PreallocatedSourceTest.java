package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.FusionSpec;
import com.lattice.graph.GraphBuildException;
import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.PreallocationSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.routing.BroadcastSpec;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.PreallocatedEmitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreallocatedSourceTest {

    @Test
    void publicPreallocatedSourceReusesPoolAndPreservesValues() throws Exception {
        final List<Long> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = StaticGraph.builder("preallocated-source")
            .preallocatedSource(
                "ingress",
                ReusableMessage.class,
                PreallocationSpec.pool(ignored -> new ReusableMessage()).poolSize(128)
            )
            .sink("egress", ReusableMessage.class, message -> consumed.add(message.value), StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();

        assertTrue(graph.plan().node("ingress").orElseThrow().preallocatedSource());
        graph.start();
        final PreallocatedEmitter<ReusableMessage> ingress =
            graph.preallocatedEmitter("ingress", ReusableMessage.class);
        final Set<ReusableMessage> claimed = Collections.newSetFromMap(new IdentityHashMap<>());

        for (int i = 0; i < 160; i++) {
            final ReusableMessage message = ingress.claim();
            claimed.add(message);
            message.value = i;
            ingress.emit(message);
        }
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(128, ingress.poolSize());
        assertTrue(ingress.reuseBound() < ingress.poolSize());
        assertEquals(128, claimed.size());
        assertEquals(160, consumed.size());
        for (int i = 0; i < consumed.size(); i++) {
            assertEquals(i, consumed.get(i));
        }
    }

    @Test
    void preallocatedSourceKeepsSingleOutstandingClaim() {
        final StaticGraph graph = StaticGraph.builder("preallocated-outstanding")
            .preallocatedSource(
                "ingress",
                ReusableMessage.class,
                PreallocationSpec.pool(ignored -> new ReusableMessage()).poolSize(128)
            )
            .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();

        final PreallocatedEmitter<ReusableMessage> ingress =
            graph.preallocatedEmitter("ingress", ReusableMessage.class);
        final ReusableMessage first = ingress.claim();

        assertThrows(GraphRuntimeException.class, ingress::claim);
        assertFalse(ingress.tryEmit(first));
        assertThrows(GraphRuntimeException.class, ingress::claim);

        ingress.discard(first);
        final ReusableMessage second = ingress.claim();
        ingress.discard(second);
        ingress.close();
        graph.stop(Duration.ofSeconds(1));
    }

    @Test
    void preallocatedClaimIsRejectedAfterClose() {
        final StaticGraph graph = StaticGraph.builder("preallocated-closed-claim")
            .preallocatedSource(
                "ingress",
                ReusableMessage.class,
                PreallocationSpec.pool(ignored -> new ReusableMessage()).poolSize(128)
            )
            .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();
        final PreallocatedEmitter<ReusableMessage> ingress =
            graph.preallocatedEmitter("ingress", ReusableMessage.class);

        final ReusableMessage claimed = ingress.claim();
        ingress.discard(claimed);
        ingress.close();

        assertTrue(ingress.isClosed());
        assertThrows(GraphRuntimeException.class, ingress::claim);
        assertTrue(graph.stop(Duration.ofSeconds(1)));
    }

    @Test
    void preallocatedTimedEmitFailureKeepsClaimOutstandingUntilDiscarded() throws Exception {
        final CountDownLatch sinkEntered = new CountDownLatch(1);
        final CountDownLatch releaseSink = new CountDownLatch(1);
        final StaticGraph graph = StaticGraph.builder("preallocated-timed-failure")
            .preallocatedSource(
                "ingress",
                ReusableMessage.class,
                PreallocationSpec.pool(ignored -> new ReusableMessage()).poolSize(128)
            )
            .sink("egress", ReusableMessage.class, ignored -> {
                sinkEntered.countDown();
                try {
                    assertTrue(releaseSink.await(5, TimeUnit.SECONDS));
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.spscRing(1))
            .build();

        graph.start();
        final PreallocatedEmitter<ReusableMessage> ingress =
            graph.preallocatedEmitter("ingress", ReusableMessage.class);
        final ReusableMessage first = ingress.claim();
        ingress.emit(first);
        assertTrue(sinkEntered.await(5, TimeUnit.SECONDS));
        final ReusableMessage second = ingress.claim();
        assertFalse(ingress.emit(second, Duration.ofMillis(10)));
        assertThrows(GraphRuntimeException.class, ingress::claim);
        ingress.discard(second);

        releaseSink.countDown();
        ingress.close();
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
    }

    @Test
    void preallocatedThrowingEmitClearsClaimAsDocumented() {
        final StaticGraph graph = StaticGraph.builder("preallocated-throw-clears")
            .preallocatedSource(
                "ingress",
                ReusableMessage.class,
                PreallocationSpec.pool(ignored -> new ReusableMessage()).poolSize(128)
            )
            .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();
        final PreallocatedEmitter<ReusableMessage> ingress =
            graph.preallocatedEmitter("ingress", ReusableMessage.class);

        final ReusableMessage first = ingress.claim();
        assertThrows(GraphRuntimeException.class, () -> ingress.emit(first));

        final ReusableMessage second = ingress.claim();
        ingress.discard(second);
        ingress.close();
        assertTrue(graph.stop(Duration.ofSeconds(1)));
    }

    @Test
    void normalEmitterRejectsPreallocatedSource() {
        final StaticGraph graph = StaticGraph.builder("preallocated-normal-reject")
            .preallocatedSource(
                "ingress",
                ReusableMessage.class,
                PreallocationSpec.pool(ignored -> new ReusableMessage()).poolSize(128)
            )
            .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();

        assertThrows(GraphRuntimeException.class, () -> graph.emitter("ingress", ReusableMessage.class));
    }

    @Test
    void preallocatedEmitterRejectsNormalSource() {
        final StaticGraph graph = StaticGraph.builder("normal-preallocated-reject")
            .source("ingress", ReusableMessage.class)
            .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();

        assertThrows(GraphRuntimeException.class, () -> graph.preallocatedEmitter("ingress", ReusableMessage.class));
    }

    @Test
    void preallocatedPoolMustCoverReuseBound() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("preallocated-small")
            .preallocatedSource(
                "ingress",
                ReusableMessage.class,
                PreallocationSpec.pool(ignored -> new ReusableMessage()).poolSize(64)
            )
            .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build());
    }

    @Test
    void preallocatedSourceRejectsNonLinearReuseDomain() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("preallocated-broadcast")
            .preallocatedSource(
                "ingress",
                ReusableMessage.class,
                PreallocationSpec.pool(ignored -> new ReusableMessage()).poolSize(256)
            )
            .broadcast("fanout", ReusableMessage.class, BroadcastSpec.copy(message -> message), StageSpec.singleThreaded())
            .sink("a", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .sink("b", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "fanout", EdgeSpec.mpscRing(8))
            .edge("fanout", "a", EdgeSpec.spscRing(8))
            .edge("fanout", "b", EdgeSpec.spscRing(8))
            .build());
    }

    @Test
    void preallocatedSourceRejectsUnsafeOverflowMemoryBatchAndRoutingShapes() {
        assertThrows(GraphBuildException.class, () -> preallocatedToSink("preallocated-fail-fast",
            EdgeSpec.mpscRing(8).overflow(OverflowPolicy.failFast())));
        assertThrows(GraphBuildException.class, () -> preallocatedToSink("preallocated-drop-latest",
            EdgeSpec.mpscRing(8).overflow(OverflowPolicy.dropLatest())));
        assertThrows(GraphBuildException.class, () -> preallocatedToSink("preallocated-block-for",
            EdgeSpec.mpscRing(8).overflow(OverflowPolicy.blockFor(Duration.ofMillis(1)))));
        assertThrows(GraphBuildException.class, () -> preallocatedToSink("preallocated-offheap",
            EdgeSpec.mpscRing(8).memory(com.lattice.placement.MemoryMode.offHeapHandles())));
        assertThrows(GraphBuildException.class, () -> preallocatedToSink("preallocated-edge-batch",
            EdgeSpec.mpscRing(8).batch(BatchPolicy.maxItems(4))));

        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("preallocated-batch-stage")
            .preallocatedSource("ingress", ReusableMessage.class,
                PreallocationSpec.pool(ignored -> new ReusableMessage()).poolSize(256))
            .batchStage("batch", ReusableMessage.class, ReusableMessage.class, (batch, out, ctx) -> {
                for (int i = 0; i < batch.size(); i++) {
                    out.push(batch.get(i));
                }
            }, StageSpec.singleThreaded().batch(BatchPolicy.maxItems(4)))
            .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "batch", EdgeSpec.mpscRing(8))
            .edge("batch", "egress", EdgeSpec.spscRing(8))
            .build());
    }

    @Test
    void preallocatedReuseBoundUsesFusedPhysicalPlan() {
        final StaticGraph graph = StaticGraph.builder("preallocated-fused-bound")
                .fusion(FusionSpec.defaults())
                .preallocatedSource(
                    "ingress",
                    ReusableMessage.class,
                    PreallocationSpec.pool(ignored -> new ReusableMessage()).poolSize(128)
                )
                .stage("a", ReusableMessage.class, ReusableMessage.class, (message, out, ctx) -> out.push(message),
                    StageSpec.singleThreaded())
                .stage("b", ReusableMessage.class, ReusableMessage.class, (message, out, ctx) -> out.push(message),
                    StageSpec.singleThreaded())
                .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
                .edge("ingress", "a", EdgeSpec.mpscRing(8))
                .edge("a", "b", EdgeSpec.spscRing(8))
                .edge("b", "egress", EdgeSpec.spscRing(8))
                .build();

            final PreallocatedEmitter<ReusableMessage> ingress =
                graph.preallocatedEmitter("ingress", ReusableMessage.class);
            assertEquals(73, ingress.reuseBound());
            ingress.close();
        graph.stop(Duration.ofSeconds(1));
    }

    static final class ReusableMessage {
        long value;
    }

    private static StaticGraph preallocatedToSink(final String name, final EdgeSpec ingressSpec) {
        return StaticGraph.builder(name)
            .preallocatedSource(
                "ingress",
                ReusableMessage.class,
                PreallocationSpec.pool(ignored -> new ReusableMessage()).poolSize(128)
            )
            .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", ingressSpec)
            .build();
    }
}
