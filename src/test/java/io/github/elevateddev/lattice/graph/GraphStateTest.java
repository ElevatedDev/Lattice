package io.github.elevateddev.lattice.graph;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphStateTest {

    @Test
    void exposesDocumentedLifecycleStatesInProgressionOrder() {
        assertEquals(List.of(
            GraphState.NEW,
            GraphState.STARTING,
            GraphState.RUNNING,
            GraphState.QUIESCING,
            GraphState.DRAINING,
            GraphState.STOPPING,
            GraphState.STOPPED,
            GraphState.FAILED
        ), List.of(GraphState.values()));
    }

    @Test
    void terminalStatesAreExplicit() {
        assertTrue(List.of(GraphState.STOPPED, GraphState.FAILED).contains(GraphState.STOPPED));
        assertTrue(List.of(GraphState.STOPPED, GraphState.FAILED).contains(GraphState.FAILED));
    }
}
