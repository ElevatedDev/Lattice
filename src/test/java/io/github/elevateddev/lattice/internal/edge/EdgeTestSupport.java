package io.github.elevateddev.lattice.internal.edge;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.internal.graph.EdgeDefinition;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import io.github.elevateddev.lattice.metrics.StageMetrics;
import io.github.elevateddev.lattice.placement.MemoryMode;
import java.util.LinkedHashMap;
import java.util.Map;

final class EdgeTestSupport {
    static final String FROM = "source";
    static final String TO = "sink";

    private EdgeTestSupport() {
    }

    static MetricsSpec metricsSpec() {
        return MetricsSpec.off().hotCounters(true).residenceTiming(true);
    }

    static EdgeMetrics edgeMetrics() {
        return edgeMetrics(FROM, TO, MemoryMode.MemoryKind.ON_HEAP_SLOTS, metricsSpec());
    }

    static EdgeMetrics edgeMetrics(
        final String from,
        final String to,
        final MemoryMode.MemoryKind memoryKind,
        final MetricsSpec metricsSpec
    ) {
        return new EdgeMetrics(from, to, "", memoryKind, metricsSpec);
    }

    static GraphMetrics graphMetrics() {
        return graphMetrics(FROM, TO, metricsSpec());
    }

    static GraphMetrics graphMetrics(final String from, final String to, final MetricsSpec metricsSpec) {
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put(from, new StageMetrics(from, metricsSpec));
        stages.put(to, new StageMetrics(to, metricsSpec));
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put(from + "->" + to, edgeMetrics(from, to, MemoryMode.MemoryKind.ON_HEAP_SLOTS, metricsSpec));
        return new GraphMetrics("edge-test", stages, edges, metricsSpec);
    }

    static SpscRingEdge spsc(final int capacity) {
        return new SpscRingEdge(FROM, TO, capacity, edgeMetrics(), graphMetrics());
    }

    static MpscRingEdge mpsc(final int capacity) {
        final MpscRingEdge edge = new MpscRingEdge(
            FROM,
            TO,
            capacity,
            MemoryMode.onHeapSlots(),
            edgeMetrics(),
            graphMetrics()
        );
        edge.firstTouch(TO);
        return edge;
    }

    static EdgeDefinition definition(final EdgeSpec spec) {
        return new EdgeDefinition(FROM, TO, Object.class, spec, 0, 0, false, false);
    }
}
