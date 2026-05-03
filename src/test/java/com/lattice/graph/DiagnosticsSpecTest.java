package com.lattice.graph;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DiagnosticsSpecTest {

    @Test
    void offDisablesJfrByDefault() {
        assertFalse(DiagnosticsSpec.off().jfr());
    }

    @Test
    void jfrReturnsConfiguredCopyWithoutMutatingBaseSpec() {
        final DiagnosticsSpec base = DiagnosticsSpec.off();
        final DiagnosticsSpec enabled = base.jfr(true);
        final DiagnosticsSpec disabled = enabled.jfr(false);

        assertFalse(base.jfr());
        assertTrue(enabled.jfr());
        assertFalse(disabled.jfr());
        assertNotSame(base, enabled);
    }
}
