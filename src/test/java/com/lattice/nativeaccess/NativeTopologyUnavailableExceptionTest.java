package com.lattice.nativeaccess;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeTopologyUnavailableExceptionTest {

    @Test
    void unavailableExceptionIsSpecificNativeTopologyFailure() {
        final NativeTopologyUnavailableException exception =
            new NativeTopologyUnavailableException("native backend unavailable");

        assertEquals("native backend unavailable", exception.getMessage());
        assertTrue(exception instanceof NativeTopologyException);
        assertTrue(Modifier.isFinal(NativeTopologyUnavailableException.class.getModifiers()));
    }

    @Test
    void messageAndCauseConstructorPreservesLoadFailureCause() {
        final UnsatisfiedLinkError cause = new UnsatisfiedLinkError("missing library");

        final NativeTopologyUnavailableException exception =
            new NativeTopologyUnavailableException("native backend unavailable", cause);

        assertEquals("native backend unavailable", exception.getMessage());
        assertSame(cause, exception.getCause());
    }
}
