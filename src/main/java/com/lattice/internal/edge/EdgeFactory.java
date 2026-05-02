package com.lattice.internal.edge;

import com.lattice.edge.EdgeSpec;
import com.lattice.internal.graph.EdgeDefinition;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;

public final class EdgeFactory {

    private EdgeFactory() {
    }

    public static MessageEdge create(
        final EdgeDefinition definition,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics
    ) {
        return create(definition, metrics, graphMetrics, definition.sourceIngress());
    }

    public static MessageEdge create(
        final EdgeDefinition definition,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics,
        final boolean sourceIngressCloseGuard
    ) {
        final MessageEdge edge;
        if (definition.spec().kind() == EdgeSpec.EdgeKind.SPSC_RING) {
            edge = new SpscRingEdge(
                definition.from(),
                definition.to(),
                definition.spec().capacity(),
                definition.spec().memoryMode(),
                metrics,
                graphMetrics,
                plainClaim(definition.spec()),
                sourceIngressCloseGuard
            );
        } else {
            edge = new MpscRingEdge(
                definition.from(),
                definition.to(),
                definition.spec().capacity(),
                definition.spec().memoryMode(),
                metrics,
                graphMetrics,
                plainClaim(definition.spec())
            );
        }
        if (!firstTouchEnabled()) {
            edge.firstTouch("graph-builder");
        }
        return edge;
    }

    private static boolean firstTouchEnabled() {
        return Boolean.parseBoolean(System.getProperty("lattice.firstTouch.enabled", "true"));
    }

    private static boolean plainClaim(final EdgeSpec spec) {
        return switch (spec.overflowPolicy().kind()) {
            case DROP_OLDEST, COALESCE -> false;
            default -> true;
        };
    }
}
