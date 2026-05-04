package com.lattice.graph;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class GraphRuntimeExceptionTest {

    @Test
    void carriesRuntimeOperationMessage() {
        final GraphRuntimeException exception = new GraphRuntimeException("not running");

        assertEquals("not running", exception.getMessage());
        assertInstanceOf(RuntimeException.class, exception);
    }

    @Test
    void carriesRuntimeFailureCause() {
        final IllegalStateException cause = new IllegalStateException("stage");
        final GraphRuntimeException exception = new GraphRuntimeException("failed", cause);

        assertEquals("failed", exception.getMessage());
        assertSame(cause, exception.getCause());
    }
}
