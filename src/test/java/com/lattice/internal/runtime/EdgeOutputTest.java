package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphState;
import com.lattice.internal.edge.MessageEdge;
import com.lattice.metrics.GraphMetrics;
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
