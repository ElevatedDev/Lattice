package com.lattice.nativeaccess;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeTopologyExceptionTest {

    @Test
    void messageConstructorPreservesDiagnosticText() {
        final NativeTopologyException exception = new NativeTopologyException("operation failed");

        assertEquals("operation failed", exception.getMessage());
        assertTrue(exception instanceof RuntimeException);
    }

    @Test
    void messageAndCauseConstructorPreservesFailureCause() {
        final IllegalStateException cause = new IllegalStateException("native errno");

        final NativeTopologyException exception = new NativeTopologyException("operation failed", cause);

        assertEquals("operation failed", exception.getMessage());
        assertSame(cause, exception.getCause());
    }
}
