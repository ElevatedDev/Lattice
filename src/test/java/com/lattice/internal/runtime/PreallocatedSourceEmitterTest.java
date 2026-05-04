package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.stage.PreallocatedEmitter;
import com.lattice.stage.StageSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PreallocatedSourceEmitterTest {

    @Test
    void graphPreallocatedEmitterUsesRuntimeImplementationAndClaimOwnership() {
        final StaticGraph graph = StaticGraph.builder("preallocated-runtime")
            .preallocatedSource("source", Payload.class, Payload::new, 128)
            .sink("sink", Payload.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("source", "sink", EdgeSpec.spscRing(2))
            .build();
        final PreallocatedEmitter<Payload> emitter = graph.preallocatedEmitter("source", Payload.class);

        assertInstanceOf(PreallocatedSourceEmitter.class, emitter);
        assertEquals("source", emitter.name());
        assertEquals(128, emitter.poolSize());
        final Payload claimed = emitter.claim();

        assertThrows(com.lattice.graph.GraphRuntimeException.class, emitter::claim);
        emitter.discard(claimed);
    }

    private record Payload(int index) {
    }
}
