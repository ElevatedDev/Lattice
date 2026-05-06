package io.github.elevateddev.lattice.internal.edge;

import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import io.github.elevateddev.lattice.placement.MemoryMode;
import io.github.elevateddev.lattice.routing.Stamped;
import io.github.elevateddev.lattice.slab.SlabPool;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpscRingEdgeTest {

    @Test
    void constructorRequiresPositivePowerOfTwoCapacityButAllowsOneSlotRing() {
        assertThrows(IllegalArgumentException.class, () -> new SpscRingEdge(
            "source",
            "sink",
            0,
            EdgeTestSupport.edgeMetrics(),
            EdgeTestSupport.graphMetrics()
        ));
        assertThrows(IllegalArgumentException.class, () -> new SpscRingEdge(
            "source",
            "sink",
            3,
            EdgeTestSupport.edgeMetrics(),
            EdgeTestSupport.graphMetrics()
        ));

        final SpscRingEdge edge = new SpscRingEdge(
            "source",
            "sink",
            1,
            EdgeTestSupport.edgeMetrics(),
            EdgeTestSupport.graphMetrics()
        );

        assertTrue(edge.offer("only"));
        assertFalse(edge.offer("full"));
        assertEquals("only", edge.poll());
    }

    @Test
    void constructorRejectsNullCollaborators() {
        assertThrows(NullPointerException.class, () -> new SpscRingEdge(
            null,
            "sink",
            2,
            EdgeTestSupport.edgeMetrics(),
            EdgeTestSupport.graphMetrics()
        ));
        assertThrows(NullPointerException.class, () -> new SpscRingEdge(
            "source",
            null,
            2,
            EdgeTestSupport.edgeMetrics(),
            EdgeTestSupport.graphMetrics()
        ));
        assertThrows(NullPointerException.class, () -> new SpscRingEdge(
            "source",
            "sink",
            2,
            null,
            EdgeTestSupport.edgeMetrics(),
            EdgeTestSupport.graphMetrics()
        ));
        assertThrows(NullPointerException.class, () -> new SpscRingEdge(
            "source",
            "sink",
            2,
            MemoryMode.onHeapSlots(),
            null,
            EdgeTestSupport.graphMetrics()
        ));
        assertThrows(NullPointerException.class, () -> new SpscRingEdge(
            "source",
            "sink",
            2,
            MemoryMode.onHeapSlots(),
            EdgeTestSupport.edgeMetrics(),
            null
        ));
    }

    @Test
    void publishesInFifoOrderReusesSlotsAndMaintainsHotCounters() {
        final SpscRingEdge edge = EdgeTestSupport.spsc(2);

        assertTrue(edge.offer(1));
        assertTrue(edge.offer(2));
        assertFalse(edge.offer(3));
        assertEquals(2, edge.inFlight());
        assertEquals(1, edge.poll());
        assertTrue(edge.offer(3));

        assertEquals(2, edge.poll());
        assertEquals(3, edge.poll());
        assertNull(edge.poll());
        assertEquals(3, edge.metrics().emittedCount());
        assertEquals(3, edge.metrics().consumedCount());
    }

    @Test
    void firstTouchIsIdempotentAndOnlyAllocatesOnce() {
        final EdgeMetrics metrics = EdgeTestSupport.edgeMetrics();
        final SpscRingEdge edge = new SpscRingEdge(
            "source",
            "sink",
            2,
            metrics,
            EdgeTestSupport.graphMetrics()
        );

        assertEquals(1, metrics.firstTouchCount());
        edge.firstTouch("another-owner");
        assertEquals(1, metrics.firstTouchCount());
        assertTrue(edge.offer("ready"));
        assertEquals("ready", edge.poll());
    }

    @Test
    void offHeapHandleMetadataSupportsResidenceTimingStorage() {
        final MetricsSpec metricsSpec = EdgeTestSupport.metricsSpec();
        final EdgeMetrics metrics = EdgeTestSupport.edgeMetrics(
            "source",
            "sink",
            MemoryMode.MemoryKind.OFF_HEAP_HANDLES,
            metricsSpec
        );
        final GraphMetrics graphMetrics = EdgeTestSupport.graphMetrics("source", "sink", metricsSpec);
        final SpscRingEdge edge = new SpscRingEdge(
            "source",
            "sink",
            2,
            MemoryMode.offHeapHandles(),
            metrics,
            graphMetrics
        );
        edge.firstTouch("source");

        assertTrue(edge.offer("offheap"));
        assertEquals("offheap", edge.poll());
        assertTrue(edge.isEmpty());
    }

    @Test
    void drainToReturnsZeroWhenLimitOrTargetSpaceCannotAcceptItems() {
        final SpscRingEdge edge = EdgeTestSupport.spsc(2);
        assertTrue(edge.offer(1));
        final Object[] target = new Object[1];

        assertEquals(0, edge.drainTo(target, 0, 0));
        assertEquals(0, edge.drainTo(target, 1, 1));
        assertEquals(1, edge.poll());
    }

    @Test
    void dropOldestDropsHeadItemAndFreesCapacityForLaterOffer() {
        final SpscRingEdge edge = EdgeTestSupport.spsc(2);
        assertTrue(edge.offer("oldest"));
        assertTrue(edge.offer("survivor"));
        assertFalse(edge.offer("new"));

        assertEquals("oldest", edge.dropOldest());
        assertTrue(edge.offer("new"));

        assertEquals("survivor", edge.poll());
        assertEquals("new", edge.poll());
        assertNull(edge.dropOldest());
        assertTrue(edge.isEmpty());
    }

    @Test
    void tryCoalesceReplacesFirstPendingItemWithMatchingKey() {
        final SpscRingEdge edge = EdgeTestSupport.spsc(4);
        assertTrue(edge.offer(new Event(1, "old")));
        assertTrue(edge.offer(new Event(2, "stay")));

        assertFalse(edge.tryCoalesce(null, item -> ((Event) item).key()));
        assertFalse(edge.tryCoalesce(new Event(3, "missing"), item -> ((Event) item).key()));
        assertTrue(edge.tryCoalesce(new Event(1, "new"), item -> ((Event) item).key()));

        assertEquals(new Event(1, "new"), edge.poll());
        assertEquals(new Event(2, "stay"), edge.poll());
        assertTrue(edge.isEmpty());
    }

    @Test
    void tryCoalesceRefusesHandleBearingItemsWithoutTakingOwnership() {
        final SpscRingEdge edge = EdgeTestSupport.spsc(2);
        final SlabPool<String> pool = new SlabPool<>("spsc-coalesce", 2);
        final var existing = pool.acquire("existing");
        final var replacement = pool.acquire("replacement");
        assertTrue(edge.offer(Stamped.of(1, existing)));

        assertFalse(edge.tryCoalesce(Stamped.of(1, replacement), item -> ((Stamped<?>) item).stamp()));

        final Stamped<?> retained = (Stamped<?>) edge.poll();
        assertSame(existing, retained.value());
        existing.release();
        replacement.release();
        assertEquals(0, pool.leakedCount());
    }

    @Test
    void releaseRemainingAfterQuiescenceClosesAndReleasesQueuedHandles() {
        final SpscRingEdge edge = EdgeTestSupport.spsc(2);
        final SlabPool<String> pool = new SlabPool<>("spsc-release", 2);
        assertTrue(edge.offer(pool.acquire("direct")));
        assertTrue(edge.offer(Stamped.of(2, pool.acquire("stamped"))));

        edge.releaseRemainingAfterQuiescence();

        assertTrue(edge.isClosed());
        assertTrue(edge.isEmpty());
        assertEquals(0, pool.leakedCount());
    }

    private record Event(int key, String value) {
    }
}
