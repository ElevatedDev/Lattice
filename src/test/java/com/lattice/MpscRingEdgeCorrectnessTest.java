package com.lattice;

import com.lattice.internal.edge.MpscRingEdge;
import com.lattice.graph.MetricsSpec;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.placement.MemoryMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MpscRingEdgeCorrectnessTest {

    @Test
    void constructorRejectsInvalidDirectCapacities() {
        assertThrows(IllegalArgumentException.class, () -> newEdge(0));
        assertThrows(IllegalArgumentException.class, () -> newEdge(1));
        assertThrows(IllegalArgumentException.class, () -> newEdge(3));

        final MpscRingEdge edge = newEdge(2);
        edge.firstTouch("sink");

        assertEquals(2, edge.capacity());
    }

    @Test
    void processorDrainReleasesClaimedSlotWhenProcessorThrows() {
        final MpscRingEdge edge = newEdge(2);
        edge.firstTouch("sink");
        assertTrue(edge.offer(1));
        assertTrue(edge.offer(2));

        assertThrows(IllegalStateException.class, () -> edge.drainToProcessor(item -> {
            throw new IllegalStateException("boom " + item);
        }, 2));

        assertEquals(2, edge.poll());
        assertTrue(edge.isEmpty());
        assertTrue(edge.offer(3));
        assertEquals(3, edge.poll());
    }

    @Test
    void processorDrainSkipsDroppedPrefixAndPreservesSurvivorOrder() throws Exception {
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
        assertTrue(edge.offer(1));
        assertTrue(edge.offer(2));
        assertTrue(edge.offer(3));

        assertEquals(1, edge.dropOldest());

        final List<Integer> processed = new ArrayList<>();
        assertEquals(2, edge.drainToProcessor(item -> processed.add((Integer) item), 4));

        assertEquals(List.of(2, 3), processed);
        assertTrue(edge.isEmpty());
        assertEquals(2, edge.metrics().consumedCount());
    }

    @Test
    void processorDrainReleasesSlotBeforeProcessorReturns() throws Exception {
        final MpscRingEdge edge = newEdge(2);
        edge.firstTouch("sink");
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

    private static MpscRingEdge newEdge(final int capacity) {
        return new MpscRingEdge(
            "source",
            "sink",
            capacity,
            MemoryMode.onHeapSlots(),
            edgeMetrics(),
            graphMetrics()
        );
    }

    private static EdgeMetrics edgeMetrics() {
        return new EdgeMetrics("source", "sink", "", MemoryMode.MemoryKind.ON_HEAP_SLOTS,
            MetricsSpec.off().hotCounters(true));
    }

    private static GraphMetrics graphMetrics() {
        final MetricsSpec metricsSpec = MetricsSpec.off().hotCounters(true);
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put("source", new StageMetrics("source", metricsSpec));
        stages.put("sink", new StageMetrics("sink", metricsSpec));
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put("source->sink", new EdgeMetrics("source", "sink", "", MemoryMode.MemoryKind.ON_HEAP_SLOTS,
            metricsSpec));
        return new GraphMetrics("mpsc-test", stages, edges, metricsSpec);
    }
}
