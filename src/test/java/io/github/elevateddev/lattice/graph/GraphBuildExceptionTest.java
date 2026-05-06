package io.github.elevateddev.lattice.graph;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class GraphBuildExceptionTest {

    @Test
    void carriesGraphValidationMessage() {
        final GraphBuildException exception = new GraphBuildException("bad topology");

        assertEquals("bad topology", exception.getMessage());
        assertInstanceOf(RuntimeException.class, exception);
    }
}
