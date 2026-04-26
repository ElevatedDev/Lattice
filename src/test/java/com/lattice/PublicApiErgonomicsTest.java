package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.PreallocationSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.placement.MemoryMode;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.StageSpec;
import com.lattice.wait.WaitSpec;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PublicApiErgonomicsTest {

    @Test
    void edgeCapacityBuilderPreservesOtherOptions() {
        final EdgeSpec base = EdgeSpec.mpscRing(8)
            .wait(WaitSpec.blocking())
            .overflow(OverflowPolicy.blockFor(Duration.ofMillis(1)))
            .memory(MemoryMode.offHeapHandles(64))
            .batch(BatchPolicy.maxItems(4));

        final EdgeSpec resized = base.capacity(16);

        assertEquals(8, base.capacity());
        assertEquals(16, resized.capacity());
        assertEquals(base.kind(), resized.kind());
        assertEquals(base.waitSpec().kind(), resized.waitSpec().kind());
        assertEquals(base.overflowPolicy().kind(), resized.overflowPolicy().kind());
        assertEquals(base.memoryMode().kind(), resized.memoryMode().kind());
        assertEquals(base.batchPolicy().kind(), resized.batchPolicy().kind());
    }

    @Test
    void builderFixedPoolOverloadDelegatesToPreallocationSpec() {
        final Reusable[] pool = new Reusable[128];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new Reusable(i);
        }

        final StaticGraph graph = StaticGraph.builder("fixed-pool-overload")
            .preallocatedSource("ingress", Reusable.class, pool)
            .sink("egress", Reusable.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();

        assertTrue(graph.plan().node("ingress").orElseThrow().preallocatedSource());
        assertEquals(PreallocationSpec.fixedPool(pool).requestedPoolSize(), pool.length);
    }

    private record Reusable(int id) {
    }
}
