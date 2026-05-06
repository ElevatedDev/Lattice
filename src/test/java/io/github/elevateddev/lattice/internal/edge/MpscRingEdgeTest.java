package io.github.elevateddev.lattice.internal.edge;

import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import io.github.elevateddev.lattice.placement.MemoryMode;
import io.github.elevateddev.lattice.routing.Stamped;
import io.github.elevateddev.lattice.slab.SlabPool;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MpscRingEdgeTest {

    @Test
    void constructorRequiresPowerOfTwoCapacityOfAtLeastTwo() {
        assertThrows(IllegalArgumentException.class, () -> newMpsc(0));
        assertThrows(IllegalArgumentException.class, () -> newMpsc(1));
        assertThrows(IllegalArgumentException.class, () -> newMpsc(3));

        final MpscRingEdge edge = newMpsc(2);

        assertEquals(2, edge.capacity());
        assertTrue(edge.isEmpty());
    }

    @Test
    void defaultConstructorFirstTouchesConsumerOwnedMetadata() {
        final EdgeMetrics metrics = EdgeTestSupport.edgeMetrics();
        final MpscRingEdge edge = new MpscRingEdge(
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
    void offersPollsInReservationOrderReusesSlotsAndMaintainsHotCounters() {
        final MpscRingEdge edge = newMpsc(2);

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
    void offHeapHandleMetadataSupportsPublicationAndDrain() {
        final MetricsSpec metricsSpec = EdgeTestSupport.metricsSpec();
        final EdgeMetrics metrics = EdgeTestSupport.edgeMetrics(
            "source",
            "sink",
            MemoryMode.MemoryKind.OFF_HEAP_HANDLES,
            metricsSpec
        );
        final GraphMetrics graphMetrics = EdgeTestSupport.graphMetrics("source", "sink", metricsSpec);
        final MpscRingEdge edge = new MpscRingEdge(
            "source",
            "sink",
            4,
            MemoryMode.offHeapHandles(),
            metrics,
            graphMetrics
        );
        edge.firstTouch("sink");
        assertTrue(edge.offer("one"));
        assertTrue(edge.offer("two"));
        final Object[] target = new Object[4];

        assertEquals(2, edge.drainTo(target, 1, 2));

        assertNull(target[0]);
        assertEquals("one", target[1]);
        assertEquals("two", target[2]);
        assertNull(target[3]);
        assertTrue(edge.isEmpty());
    }

    @Test
    void closePoisonsTailAndRejectsLaterOffersWithoutDiscardingAcceptedItems() {
        final MpscRingEdge edge = newMpsc(4);
        assertTrue(edge.offer("accepted"));

        edge.close();

        assertTrue(edge.isClosed());
        assertFalse(edge.offer("rejected"));
        assertEquals("accepted", edge.poll());
        assertNull(edge.poll());
        assertTrue(edge.isEmpty());
    }

    @Test
    void dropOldestSkipsDroppedSentinelAndFreesCapacity() {
        final MpscRingEdge edge = newMpsc(2);
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
    void drainToSkipsDroppedSentinelsWithoutCountingThem() {
        final MpscRingEdge edge = newMpsc(4);
        assertTrue(edge.offer(1));
        assertTrue(edge.offer(2));
        assertTrue(edge.offer(3));
        assertEquals(1, edge.dropOldest());
        final Object[] target = new Object[3];

        assertEquals(2, edge.drainTo(target, 0, 3));

        assertEquals(2, target[0]);
        assertEquals(3, target[1]);
        assertNull(target[2]);
        assertTrue(edge.isEmpty());
    }

    @Test
    void drainToProcessorReleasesSlotBeforeCallbackReturns() throws Exception {
        final MpscRingEdge edge = newMpsc(2);
        assertTrue(edge.offer(1));
        assertTrue(edge.offer(2));
        final List<Integer> processed = new ArrayList<>();

        assertEquals(2, edge.drainToProcessor(item -> {
            processed.add((Integer) item);
            if ((Integer) item == 1) {
                assertTrue(edge.offer(3));
            }
        }, 2));

        assertEquals(List.of(1, 2), processed);
        assertEquals(3, edge.poll());
        assertTrue(edge.isEmpty());
    }

    @Test
    void tryCoalesceReplacesFirstPublishedItemWithMatchingKey() {
        final MpscRingEdge edge = newMpsc(4);
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
        final MpscRingEdge edge = newMpsc(2);
        final SlabPool<String> pool = new SlabPool<>("mpsc-coalesce", 2);
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
        final MpscRingEdge edge = newMpsc(2);
        final SlabPool<String> pool = new SlabPool<>("mpsc-release", 2);
        assertTrue(edge.offer(pool.acquire("direct")));
        assertTrue(edge.offer(Stamped.of(2, pool.acquire("stamped"))));

        edge.releaseRemainingAfterQuiescence();

        assertTrue(edge.isClosed());
        assertTrue(edge.isEmpty());
        assertEquals(0, pool.leakedCount());
    }

    private static MpscRingEdge newMpsc(final int capacity) {
        return EdgeTestSupport.mpsc(capacity);
    }

    private record Event(int key, String value) {
    }
}
