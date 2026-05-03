package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.StaticGraph;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SourceEmitterTest {

    @Test
    void graphEmitterUsesRuntimeImplementationAndEnforcesRunningState() throws Exception {
        final StaticGraph graph = StaticGraph.builder("source-emitter")
            .source("source", Integer.class)
            .sink("sink", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("source", "sink", EdgeSpec.mpscRing(8))
            .build();
        final Emitter<Integer> emitter = graph.emitter("source", Integer.class);

        assertInstanceOf(SourceEmitter.class, emitter);
        assertEquals("source", emitter.name());
        assertFalse(emitter.tryEmit(1));
        assertThrows(GraphRuntimeException.class, () -> emitter.emit(1));

        graph.start();
        assertTrue(emitter.tryEmit(1));
        emitter.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertTrue(emitter.isClosed());
    }
}
