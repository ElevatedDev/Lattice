package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphState;
import com.lattice.graph.MetricsSpec;
import com.lattice.internal.edge.EdgeFactory;
import com.lattice.internal.edge.MessageEdge;
import com.lattice.internal.graph.EdgeDefinition;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.placement.MemoryMode;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

final class RuntimeTestSupport {
    static final MetricsSpec METRICS = MetricsSpec.off().hotCounters(true).residenceTiming(true);

    private RuntimeTestSupport() {
    }

    static RuntimeCoordinator coordinator(final String graphName, final GraphState state, final GraphMetrics metrics) {
        return new RuntimeCoordinator(
            graphName,
            new AtomicReference<>(state),
            new AtomicReference<>(),
            metrics,
            0,
            false,
            false,
            false,
            false,
            false,
            0L
        );
    }

    static GraphMetrics graphMetrics(final String graphName, final String from, final String to) {
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put(from, new StageMetrics(from, METRICS));
        stages.put(to, new StageMetrics(to, METRICS));
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put(from + "->" + to, edgeMetrics(from, to));
        return new GraphMetrics(graphName, stages, edges, METRICS);
    }

    static EdgeMetrics edgeMetrics(final String from, final String to) {
        return new EdgeMetrics(from, to, "", MemoryMode.MemoryKind.ON_HEAP_SLOTS, METRICS);
    }

    static EdgeSender sender(
        final String owner,
        final Class<?> messageType,
        final EdgeSpec spec,
        final MessageEdge edge,
        final RuntimeCoordinator coordinator
    ) {
        return new EdgeSender(owner, messageType, edge, spec, new StageMetrics(owner, METRICS), coordinator);
    }

    static MessageEdge edge(final String from, final String to, final Class<?> messageType, final EdgeSpec spec) {
        final EdgeDefinition definition = new EdgeDefinition(from, to, messageType, spec, 0, 0, false, false);
        return EdgeFactory.create(definition, edgeMetrics(from, to), graphMetrics("runtime-test", from, to), false);
    }
}
