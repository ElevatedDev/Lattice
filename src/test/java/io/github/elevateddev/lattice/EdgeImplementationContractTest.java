package io.github.elevateddev.lattice;

import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.internal.edge.MessageEdge;
import io.github.elevateddev.lattice.internal.edge.MpscRingEdge;
import io.github.elevateddev.lattice.internal.edge.SpscRingEdge;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import io.github.elevateddev.lattice.metrics.StageMetrics;
import io.github.elevateddev.lattice.placement.MemoryMode;
import io.github.elevateddev.lattice.routing.Stamped;
import io.github.elevateddev.lattice.slab.SlabPool;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeImplementationContractTest {

    @Test
    void spscCloseRejectsNewOffersButPreservesAcceptedQueueForDrain() {
        final SpscRingEdge edge = new SpscRingEdge("source", "sink", 4, edgeMetrics(), graphMetrics());
        assertTrue(edge.offer(1));
        assertTrue(edge.offer(2));

        edge.close();

        assertTrue(edge.isClosed());
        assertFalse(edge.offer(3));
        assertEquals(2, edge.inFlight());
        assertEquals(1, edge.poll());
        assertEquals(2, edge.poll());
        assertNull(edge.poll());
        assertTrue(edge.isEmpty());
    }

    @Test
    void mpscOffHeapMetadataSupportsDropCoalesceDrainAndReleaseRemaining() throws Exception {
        final MpscRingEdge edge = new MpscRingEdge(
            "source",
            "sink",
            4,
            MemoryMode.offHeapHandles(),
            edgeMetrics(),
            graphMetrics(),
            false
        );
        edge.firstTouch("sink");
        assertTrue(edge.offer(new Event(1, 1)));
        assertTrue(edge.offer(new Event(2, 1)));
        assertTrue(edge.offer(new Event(1, 2)));

        assertEquals(new Event(1, 1), edge.dropOldest());
        assertTrue(edge.tryCoalesce(new Event(2, 99), item -> ((Event) item).key()));

        final Object[] drained = new Object[4];
        assertEquals(2, edge.drainTo(drained, 1, 3));
        assertNull(drained[0]);
        assertEquals(new Event(2, 99), drained[1]);
        assertEquals(new Event(1, 2), drained[2]);
        assertTrue(edge.isEmpty());

        final SlabPool<String> pool = new SlabPool<>("offheap-release", 1);
        assertTrue(edge.offer(pool.acquire("queued")));
        edge.releaseRemainingAfterQuiescence();
        assertEquals(0, pool.leakedCount());
        assertTrue(edge.isClosed());
    }

    @Test
    void coalescingRefusesHandleBearingItemsWithoutLeaking() {
        final MpscRingEdge edge = new MpscRingEdge(
            "source",
            "sink",
            4,
            MemoryMode.onHeapSlots(),
            edgeMetrics(),
            graphMetrics(),
            false
        );
        edge.firstTouch("sink");
        final SlabPool<String> pool = new SlabPool<>("coalesce-handles", 2);
        final var existing = pool.acquire("existing");
        final var replacement = pool.acquire("replacement");
        assertTrue(edge.offer(Stamped.of(1, existing)));

        assertFalse(edge.tryCoalesce(Stamped.of(1, replacement), item -> ((Stamped<?>) item).stamp()));
        assertEquals(2, pool.leakedCount());

        edge.releaseRemainingAfterQuiescence();
        replacement.release();
        assertEquals(0, pool.leakedCount());
    }

    @Test
    void processorDrainAdvancesPastDroppedSentinelsWithoutCountingThem() throws Exception {
        final MessageEdge edge = new MpscRingEdge(
            "source",
            "sink",
            4,
            MemoryMode.onHeapSlots(),
            edgeMetrics(),
            graphMetrics(),
            false
        );
        edge.firstTouch("sink");
        assertTrue(edge.offer(1));
        assertTrue(edge.offer(2));
        assertTrue(edge.offer(3));
        assertEquals(1, edge.dropOldest());

        final java.util.List<Integer> processed = new java.util.ArrayList<>();
        assertEquals(2, edge.drainToProcessor(item -> processed.add((Integer) item), 4));

        assertEquals(java.util.List.of(2, 3), processed);
        assertTrue(edge.isEmpty());
        assertEquals(2, edge.metrics().consumedCount());
    }

    private static EdgeMetrics edgeMetrics() {
        return new EdgeMetrics("source", "sink", "", MemoryMode.MemoryKind.ON_HEAP_SLOTS,
            MetricsSpec.off().hotCounters(true).residenceTiming(true));
    }

    private static GraphMetrics graphMetrics() {
        final MetricsSpec metricsSpec = MetricsSpec.off().hotCounters(true).residenceTiming(true);
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put("source", new StageMetrics("source", metricsSpec));
        stages.put("sink", new StageMetrics("sink", metricsSpec));
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put("source->sink", new EdgeMetrics("source", "sink", "", MemoryMode.MemoryKind.ON_HEAP_SLOTS,
            metricsSpec));
        return new GraphMetrics("edge-contract", stages, edges, metricsSpec);
    }

    private record Event(int key, int value) {
    }
}
