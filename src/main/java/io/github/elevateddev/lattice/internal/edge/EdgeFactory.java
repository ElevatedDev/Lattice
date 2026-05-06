package io.github.elevateddev.lattice.internal.edge;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.internal.graph.EdgeDefinition;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;

public final class EdgeFactory {

    private EdgeFactory() {
    }

    public static MessageEdge create(
        final EdgeDefinition definition,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics
    ) {
        return create(definition, metrics, graphMetrics, definition.sourceIngress(), false);
    }

    public static MessageEdge create(
        final EdgeDefinition definition,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics,
        final boolean sourceIngressCloseGuard
    ) {
        return create(definition, metrics, graphMetrics, sourceIngressCloseGuard, false);
    }

    public static MessageEdge create(
        final EdgeDefinition definition,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics,
        final boolean sourceIngressCloseGuard,
        final boolean workerFirstTouch
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
        if (!workerFirstTouch) {
            edge.firstTouch("graph-builder");
        }
        return edge;
    }

    private static boolean plainClaim(final EdgeSpec spec) {
        return switch (spec.overflowPolicy().kind()) {
            case DROP_OLDEST, COALESCE -> false;
            default -> true;
        };
    }
}
