package com.lattice.graph;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FusionSpecTest {

    @Test
    void defaultsEnableDownstreamFusionOnly() {
        final FusionSpec defaults = FusionSpec.defaults();

        assertTrue(defaults.enabled());
        assertFalse(defaults.inlineSources());
        assertFalse(defaults.elideInlineSourcePhysicalPath());
        assertFalse(defaults.validateTypes());
    }

    @Test
    void disabledKeepsPhysicalGraphShape() {
        assertFalse(FusionSpec.disabled().enabled());
    }

    @Test
    void copyMethodsPreserveUnchangedControls() {
        final FusionSpec base = FusionSpec.defaults()
            .inlineSources(true)
            .elideInlineSourcePhysicalPath(true)
            .validateTypes(true);
        final FusionSpec disabled = base.enabled(false);

        assertTrue(base.enabled());
        assertFalse(disabled.enabled());
        assertTrue(disabled.inlineSources());
        assertTrue(disabled.elideInlineSourcePhysicalPath());
        assertTrue(disabled.validateTypes());
        assertNotSame(base, disabled);
    }
}
