package com.lattice;

import com.lattice.internal.edge.MpscRingEdge;
import com.lattice.internal.edge.SpscRingEdge;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.placement.MemoryMode;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RingEdgeCorrectnessTest {

    @Test
    void spscPublishesInFifoOrderAndReusesSlots() {
        final SpscRingEdge edge = new SpscRingEdge("a", "b", 4, edgeMetrics(), graphMetrics());

        for (int i = 0; i < 4; i++) {
            assertTrue(edge.offer(i));
        }
        assertNull(edge.offer(4) ? "unexpected" : null);
        for (int i = 0; i < 4; i++) {
            assertEquals(i, edge.poll());
        }

        for (int i = 4; i < 8; i++) {
            assertTrue(edge.offer(i));
        }
        for (int i = 4; i < 8; i++) {
            assertEquals(i, edge.poll());
        }
        assertTrue(edge.isEmpty());
        assertEquals(4, edge.metrics().highWaterMark());
    }

    @Test
    void mpscOnHeapSlotsAcceptConcurrentProducersWithSingleConsumer() throws Exception {
        assertMpscAcceptsConcurrentProducers(MemoryMode.onHeapSlots());
    }

    @Test
    void mpscOffHeapHandlesAcceptConcurrentProducersWithSingleConsumer() throws Exception {
        assertMpscAcceptsConcurrentProducers(MemoryMode.offHeapHandles());
    }

    @Test
    void mpscOfferAfterCloseFailsWithoutPublishing() {
        final MpscRingEdge edge = new MpscRingEdge("source", "sink", 8, MemoryMode.onHeapSlots(), edgeMetrics(), graphMetrics());
        edge.firstTouch("sink");

        edge.close();

        assertFalse(edge.offer(1));
        assertNull(edge.poll());
        assertTrue(edge.isEmpty());
        assertEquals(0, edge.metrics().emittedCount());
        assertEquals(0, edge.metrics().consumedCount());
    }

    private static void assertMpscAcceptsConcurrentProducers(final MemoryMode memoryMode) throws Exception {
        final int producers = 4;
        final int perProducer = 1_000;
        final int total = producers * perProducer;
        final MpscRingEdge edge = new MpscRingEdge("source", "sink", 1024, memoryMode, edgeMetrics(), graphMetrics());
        edge.firstTouch("sink");
        final Set<Integer> seen = ConcurrentHashMap.newKeySet(total);
        final CountDownLatch started = new CountDownLatch(producers);
        final ExecutorService pool = Executors.newFixedThreadPool(producers + 1);

        for (int p = 0; p < producers; p++) {
            final int base = p * perProducer;
            pool.submit(() -> {
                started.countDown();
                try {
                    assertTrue(started.await(2, TimeUnit.SECONDS));
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
                for (int i = 0; i < perProducer; i++) {
                    while (!edge.offer(base + i)) {
                        Thread.onSpinWait();
                    }
                }
            });
        }

        pool.submit(() -> {
            while (seen.size() < total) {
                final Object item = edge.poll();
                if (item == null) {
                    Thread.onSpinWait();
                } else {
                    seen.add((Integer) item);
                }
            }
        });

        pool.shutdown();
        assertTrue(pool.awaitTermination(Duration.ofSeconds(10).toMillis(), TimeUnit.MILLISECONDS));
        assertEquals(total, seen.size());
        assertEquals(total, edge.metrics().emittedCount());
        assertEquals(total, edge.metrics().consumedCount());
    }

    private static EdgeMetrics edgeMetrics() {
        return new EdgeMetrics("a", "b");
    }

    private static GraphMetrics graphMetrics() {
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put("a", new StageMetrics("a"));
        stages.put("b", new StageMetrics("b"));
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put("a->b", new EdgeMetrics("a", "b"));
        return new GraphMetrics("test", stages, edges);
    }
}
