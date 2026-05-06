package io.github.elevateddev.lattice.internal.graph;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeDefinitionTest {

    @Test
    void keyUsesDeclaredSourceAndTargetNames() {
        final EdgeDefinition edge = new EdgeDefinition(
            "source",
            "sink",
            String.class,
            EdgeSpec.mpscRing(8),
            4,
            2,
            true,
            true
        );

        assertEquals("source->sink", edge.key());
        assertEquals(4, edge.declarationOrder());
        assertEquals(2, edge.branchIndex());
        assertEquals(EdgeSpec.EdgeKind.MPSC_RING, edge.declaredKind());
        assertTrue(edge.redirectOnly());
        assertTrue(edge.sourceIngress());
    }
}
