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
        final MessageEdge edge;
        if (definition.spec().kind() == EdgeSpec.EdgeKind.SPSC_RING) {
            edge = new SpscRingEdge(
                definition.from(),
                definition.to(),
                definition.spec().capacity(),
                definition.spec().memoryMode(),
                metrics,
                graphMetrics,
                plainClaim(definition.spec())
            );
        } else {
            edge = new MpscRingEdge(
                definition.from(),
                definition.to(),
                definition.spec().capacity(),
                definition.spec().memoryMode(),
                metrics,
                graphMetrics
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
            case DROP_LATEST, DROP_OLDEST, COALESCE, REDIRECT -> false;
            default -> true;
        };
    }
}
