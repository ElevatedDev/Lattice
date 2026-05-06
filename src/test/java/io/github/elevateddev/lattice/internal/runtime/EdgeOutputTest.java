package io.github.elevateddev.lattice.internal.runtime;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.internal.edge.MessageEdge;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeOutputTest {

    @Test
    void pushVariantsDelegateToEdgeSender() {
        final String from = "owner";
        final String to = "target";
        final EdgeSpec spec = EdgeSpec.spscRing(8);
        final MessageEdge edge = RuntimeTestSupport.edge(from, to, String.class, spec);
        final GraphMetrics metrics = RuntimeTestSupport.graphMetrics("edge-output", from, to);
        final RuntimeCoordinator coordinator = RuntimeTestSupport.coordinator("edge-output", GraphState.RUNNING, metrics);
        final EdgeOutput<String> output = new EdgeOutput<>(
            RuntimeTestSupport.sender(from, String.class, spec, edge, coordinator)
        );

        output.push("a");
        assertTrue(output.push("b", Duration.ofMillis(1)));
        assertTrue(output.tryPush("c"));

        assertEquals("a", edge.poll());
        assertEquals("b", edge.poll());
        assertEquals("c", edge.poll());
    }
}
