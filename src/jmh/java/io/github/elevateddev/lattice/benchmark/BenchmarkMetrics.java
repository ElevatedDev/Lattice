package io.github.elevateddev.lattice.benchmark;

import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import io.github.elevateddev.lattice.metrics.StageMetrics;
import java.util.LinkedHashMap;
import java.util.Map;

final class BenchmarkMetrics {

    private BenchmarkMetrics() {
    }

    static EdgeMetrics edgeMetrics(final String from, final String to) {
        return new EdgeMetrics(from, to);
    }

    static GraphMetrics graphMetrics(final String name, final String from, final String to) {
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put(from, new StageMetrics(from));
        stages.put(to, new StageMetrics(to));
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put(from + "->" + to, new EdgeMetrics(from, to));
        return new GraphMetrics(name, stages, edges);
    }
}
