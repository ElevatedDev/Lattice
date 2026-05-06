package io.github.elevateddev.lattice.edge;

import java.io.Serializable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class BackpressureExceptionTest {

    @Test
    void isRuntimeExceptionForFullEdgeRejections() {
        final BackpressureException exception = new BackpressureException("edge is full");

        assertInstanceOf(RuntimeException.class, exception);
        assertInstanceOf(Serializable.class, exception);
        assertEquals("edge is full", exception.getMessage());
        assertNull(exception.getCause());
    }
}
