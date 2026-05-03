package com.lattice.internal.graph;

import com.lattice.edge.EdgeSpec;
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
        assertTrue(edge.redirectOnly());
        assertTrue(edge.sourceIngress());
    }
}
