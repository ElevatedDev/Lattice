package io.github.elevateddev.lattice.internal.jfr;

import jdk.jfr.EventType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class JfrEventsTest {

    @Test
    void publicEmissionMethodsAreNoOpsWhenEventsAreNotEnabled() {
        assertFalse(JfrEvents.enabled());

        JfrEvents.graphStarted("graph");
        JfrEvents.graphStopped("graph", "STOPPED");
        JfrEvents.stageException("graph", "stage", new IllegalStateException("boom"));
        JfrEvents.edgeBackpressure("graph", "from", "to");
        JfrEvents.edgeStall("graph", "from", "to", 10L);
        JfrEvents.batchProcessed("graph", "stage", 2, 10L);
        JfrEvents.workerBlocked("graph", "stage");
        JfrEvents.workerParked("graph", "stage");
        JfrEvents.workerPlacement("graph", "stage", 1, 1, 0, 0, "PLACED");
        JfrEvents.affinityMismatch("graph", "stage", 1, 2);
        JfrEvents.numaMismatch("graph", "stage", 0, 1);
    }

    @Test
    void eventTypesUseStableLatticeNames() {
        assertEquals("io.github.elevateddev.lattice.GraphStarted", EventType.getEventType(JfrEvents.GraphStarted.class).getName());
        assertEquals("io.github.elevateddev.lattice.GraphStopped", EventType.getEventType(JfrEvents.GraphStopped.class).getName());
        assertEquals("io.github.elevateddev.lattice.StageException", EventType.getEventType(JfrEvents.StageException.class).getName());
        assertEquals("io.github.elevateddev.lattice.EdgeBackpressure", EventType.getEventType(JfrEvents.EdgeBackpressure.class).getName());
        assertEquals("io.github.elevateddev.lattice.WorkerPlacement", EventType.getEventType(JfrEvents.WorkerPlacement.class).getName());
    }
}
