package com.lattice.stage;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmitterTest {

    @Test
    void normalEmitterPublishesUntilClosed() throws Exception {
        final List<Integer> consumed = new CopyOnWriteArrayList<>();
        final StaticGraph graph = StaticGraph.builder("emitter-contract")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, consumed::add, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);

        assertEquals("ingress", ingress.name());
        assertFalse(ingress.isClosed());
        ingress.emit(1);
        assertTrue(ingress.emit(2, Duration.ofMillis(50)));
        assertTrue(ingress.tryEmit(3));
        ingress.close();

        assertTrue(ingress.isClosed());
        assertFalse(ingress.tryEmit(4));
        assertThrows(GraphRuntimeException.class, () -> ingress.emit(4));
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(List.of(1, 2, 3), List.copyOf(consumed));
    }

    @Test
    void emitterRejectsBlockingEmitBeforeGraphStarts() {
        final StaticGraph graph = StaticGraph.builder("emitter-before-start")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);

        assertFalse(ingress.tryEmit(1));
        assertThrows(GraphRuntimeException.class, () -> ingress.emit(1));
        assertTrue(graph.stop(Duration.ofSeconds(1)));
    }
}
