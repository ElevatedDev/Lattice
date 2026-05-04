package com.lattice.internal.edge;

import com.lattice.routing.Stamped;
import com.lattice.slab.SlabPool;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageEdgeTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("edges")
    void exposesIdentityCapacityMetricsAndInitialState(final String name, final Supplier<MessageEdge> edgeFactory) {
        final MessageEdge edge = edgeFactory.get();

        assertEquals(EdgeTestSupport.FROM, edge.from());
        assertEquals(EdgeTestSupport.TO, edge.to());
        assertEquals(4, edge.capacity());
        assertSame(edge.metrics(), edge.metrics());
        assertNotNull(edge.metrics());
        assertTrue(edge.isEmpty());
        assertEquals(0, edge.inFlight());
        assertFalse(edge.isClosed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("edges")
    void offersPollsAndReportsInFlightInFifoOrder(final String name, final Supplier<MessageEdge> edgeFactory) {
        final MessageEdge edge = edgeFactory.get();

        assertTrue(edge.offer("one"));
        assertTrue(edge.offer("two"));
        assertEquals(2, edge.inFlight());
        assertFalse(edge.isEmpty());

        assertEquals("one", edge.poll());
        assertEquals(1, edge.inFlight());
        assertEquals("two", edge.poll());
        assertNull(edge.poll());
        assertTrue(edge.isEmpty());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("edges")
    void defaultDrainToStartsAtZeroAndHonorsLimit(final String name, final Supplier<MessageEdge> edgeFactory) {
        final MessageEdge edge = edgeFactory.get();
        assertTrue(edge.offer("one"));
        assertTrue(edge.offer("two"));
        assertTrue(edge.offer("three"));
        final Object[] target = {"keep", "keep", "keep"};

        assertEquals(2, edge.drainTo(target, 2));

        assertArrayEquals(new Object[] {"one", "two", "keep"}, target);
        assertEquals("three", edge.poll());
        assertTrue(edge.isEmpty());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("edges")
    void drainToUsesOffsetLimitAndTargetCapacity(final String name, final Supplier<MessageEdge> edgeFactory) {
        final MessageEdge edge = edgeFactory.get();
        assertTrue(edge.offer(1));
        assertTrue(edge.offer(2));
        assertTrue(edge.offer(3));
        assertTrue(edge.offer(4));
        final Object[] target = {"before", null, null, null};

        assertEquals(3, edge.drainTo(target, 1, 10));

        assertArrayEquals(new Object[] {"before", 1, 2, 3}, target);
        assertEquals(4, edge.poll());
        assertTrue(edge.isEmpty());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("edges")
    void drainToProcessorStopsAtLimitAndLeavesRemainder(final String name, final Supplier<MessageEdge> edgeFactory)
        throws Exception {
        final MessageEdge edge = edgeFactory.get();
        assertTrue(edge.offer(1));
        assertTrue(edge.offer(2));
        assertTrue(edge.offer(3));
        final List<Integer> processed = new ArrayList<>();

        assertEquals(2, edge.drainToProcessor(item -> processed.add((Integer) item), 2));

        assertEquals(List.of(1, 2), processed);
        assertEquals(3, edge.poll());
        assertTrue(edge.isEmpty());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("edges")
    void processorExceptionRemovesOnlyClaimedItem(final String name, final Supplier<MessageEdge> edgeFactory) {
        final MessageEdge edge = edgeFactory.get();
        assertTrue(edge.offer(1));
        assertTrue(edge.offer(2));

        assertThrows(IllegalStateException.class, () -> edge.drainToProcessor(item -> {
            throw new IllegalStateException("boom " + item);
        }, 2));

        assertEquals(2, edge.poll());
        assertTrue(edge.isEmpty());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("edges")
    void closeRejectsNewOffersButPreservesAcceptedItems(final String name, final Supplier<MessageEdge> edgeFactory) {
        final MessageEdge edge = edgeFactory.get();
        assertTrue(edge.offer("accepted"));

        edge.close();

        assertTrue(edge.isClosed());
        assertFalse(edge.offer("rejected"));
        assertEquals("accepted", edge.poll());
        assertNull(edge.poll());
        assertTrue(edge.isEmpty());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("edges")
    void releaseRemainingAfterQuiescenceClosesAndReleasesQueuedHandles(
        final String name,
        final Supplier<MessageEdge> edgeFactory
    ) {
        final MessageEdge edge = edgeFactory.get();
        final SlabPool<String> pool = new SlabPool<>(name + "-contract-release", 2);
        assertTrue(edge.offer(pool.acquire("direct")));
        assertTrue(edge.offer(Stamped.of(1, pool.acquire("stamped"))));

        edge.releaseRemainingAfterQuiescence();

        assertTrue(edge.isClosed());
        assertTrue(edge.isEmpty());
        assertEquals(0, edge.inFlight());
        assertEquals(0, pool.leakedCount());
    }

    @Test
    void edgeFactoriesAreDistinctForEveryParameterizedCase() {
        final Object[] names = edges().map(Arguments::get).map(values -> values[0]).toArray();

        assertEquals(Arrays.asList("SPSC", "MPSC"), Arrays.asList(names));
    }

    private static Stream<Arguments> edges() {
        return Stream.of(
            Arguments.of("SPSC", (Supplier<MessageEdge>) () -> EdgeTestSupport.spsc(4)),
            Arguments.of("MPSC", (Supplier<MessageEdge>) () -> EdgeTestSupport.mpsc(4))
        );
    }
}
