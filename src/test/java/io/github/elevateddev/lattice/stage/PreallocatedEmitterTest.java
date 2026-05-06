package io.github.elevateddev.lattice.stage;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.GraphRuntimeException;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.graph.PreallocationSpec;
import io.github.elevateddev.lattice.graph.StaticGraph;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreallocatedEmitterTest {

    @Test
    void preallocatedEmitterOwnsOneClaimUntilEmitOrDiscard() throws Exception {
        final List<Integer> consumed = new CopyOnWriteArrayList<>();
        final StaticGraph graph = StaticGraph.builder("preallocated-emitter-contract")
            .preallocatedSource(
                "ingress",
                ReusableMessage.class,
                PreallocationSpec.pool(ReusableMessage::new).poolSize(128)
            )
            .sink("egress", ReusableMessage.class, message -> consumed.add(message.value), StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();
        final PreallocatedEmitter<ReusableMessage> ingress =
            graph.preallocatedEmitter("ingress", ReusableMessage.class);

        assertEquals("ingress", ingress.name());
        assertEquals(128, ingress.poolSize());
        assertTrue(ingress.reuseBound() < ingress.poolSize());

        final ReusableMessage discarded = ingress.claim();
        assertThrows(GraphRuntimeException.class, ingress::claim);
        assertThrows(GraphRuntimeException.class, () -> ingress.emit(new ReusableMessage(999)));
        ingress.discard(discarded);

        graph.start();
        final ReusableMessage first = ingress.claim();
        first.value = 1;
        ingress.emit(first);
        final ReusableMessage second = ingress.claim();
        second.value = 2;
        assertTrue(ingress.emit(second, Duration.ofMillis(50)));
        final ReusableMessage third = ingress.claim();
        third.value = 3;
        assertTrue(ingress.tryEmit(third));
        ingress.close();

        assertTrue(ingress.isClosed());
        assertThrows(GraphRuntimeException.class, ingress::claim);
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(List.of(1, 2, 3), List.copyOf(consumed));
    }

    static final class ReusableMessage {
        final int slot;
        int value;

        ReusableMessage(final int slot) {
            this.slot = slot;
        }
    }
}
