package io.github.elevateddev.lattice.internal.runtime;

import io.github.elevateddev.lattice.edge.BackpressureException;
import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.edge.OverflowPolicy;
import io.github.elevateddev.lattice.graph.GraphRuntimeException;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.internal.edge.MessageEdge;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EdgeSenderTest {

    @Test
    void validatesNullsTypesAndClosedGraphState() {
        final EdgeSpec spec = EdgeSpec.spscRing(2);
        final MessageEdge edge = RuntimeTestSupport.edge("owner", "target", String.class, spec);
        final GraphMetrics metrics = RuntimeTestSupport.graphMetrics("sender", "owner", "target");
        final EdgeSender sender = RuntimeTestSupport.sender(
            "owner",
            String.class,
            spec,
            edge,
            RuntimeTestSupport.coordinator("sender", GraphState.RUNNING, metrics)
        );

        assertThrows(NullPointerException.class, () -> sender.emit(null));
        assertThrows(ClassCastException.class, () -> sender.emit(1));

        final GraphMetrics stoppedMetrics = RuntimeTestSupport.graphMetrics("sender-stopped", "owner", "target");
        final RuntimeCoordinator stopping = RuntimeTestSupport.coordinator(
            "sender-stopped",
            GraphState.RUNNING,
            stoppedMetrics
        );
        stopping.requestStop();
        final EdgeSender stopped = RuntimeTestSupport.sender(
            "owner",
            String.class,
            spec,
            RuntimeTestSupport.edge("owner", "target", String.class, spec),
            stopping
        );
        assertThrows(GraphRuntimeException.class, () -> stopped.emit("x"));
        assertFalse(stopped.tryEmit("x"));
    }

    @Test
    void failFastOverflowThrowsWhenEdgeIsFull() {
        final EdgeSpec spec = EdgeSpec.spscRing(1).overflow(OverflowPolicy.failFast());
        final MessageEdge edge = RuntimeTestSupport.edge("owner", "target", String.class, spec);
        final GraphMetrics metrics = RuntimeTestSupport.graphMetrics("sender-full", "owner", "target");
        final EdgeSender sender = RuntimeTestSupport.sender(
            "owner",
            String.class,
            spec,
            edge,
            RuntimeTestSupport.coordinator("sender-full", GraphState.RUNNING, metrics)
        );

        sender.emit("first");

        assertThrows(BackpressureException.class, () -> sender.emit("second"));
        assertEquals("first", edge.poll());
    }
}
