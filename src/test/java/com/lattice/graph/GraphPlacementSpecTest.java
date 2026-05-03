package com.lattice.graph;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphPlacementSpecTest {

    @Test
    void offDisablesRuntimeDerivedPlacement() {
        final GraphPlacementSpec off = GraphPlacementSpec.off();

        assertFalse(off.topologyAware());
        assertFalse(off.strict());
        assertFalse(off.firstTouch());
    }

    @Test
    void copyMethodsPreserveIndependentPlacementControls() {
        final GraphPlacementSpec base = GraphPlacementSpec.off()
            .topologyAware(true)
            .strict(true);
        final GraphPlacementSpec firstTouch = base.firstTouch(true);

        assertTrue(base.topologyAware());
        assertTrue(base.strict());
        assertFalse(base.firstTouch());
        assertTrue(firstTouch.topologyAware());
        assertTrue(firstTouch.strict());
        assertTrue(firstTouch.firstTouch());
        assertNotSame(base, firstTouch);
    }
}
