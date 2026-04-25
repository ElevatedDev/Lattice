package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphBuildException;
import com.lattice.graph.StaticGraph;
import com.lattice.placement.MemoryMode;
import com.lattice.placement.PinPolicy;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.StageSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GraphValidationTest {

    @Test
    void emitsInspectableDeterministicPlan() {
        final StaticGraph graph = phaseOneBuilder().build();

        assertEquals("phase1-orders", graph.plan().name());
        assertEquals(3, graph.plan().nodes().size());
        assertEquals(2, graph.plan().edges().size());
        assertEquals(java.util.List.of("validate", "egress"), graph.plan().workerOrder());
    }

    @Test
    void rejectsDuplicateNodeNames() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad")
            .source("ingress", Order.class)
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build());
    }

    @Test
    void rejectsTypeMismatch() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad")
            .source("ingress", Order.class)
            .sink("egress", ValidOrder.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build());
    }

    @Test
    void rejectsUnsupportedPhaseOneOptions() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("batch")
            .source("ingress", Order.class)
            .stage("validate", Order.class, ValidOrder.class, (order, out, ctx) -> out.push(new ValidOrder(order.id())),
                StageSpec.singleThreaded().batch(BatchPolicy.maxItems(16)))
            .sink("egress", ValidOrder.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "validate", EdgeSpec.mpscRing(8))
            .edge("validate", "egress", EdgeSpec.spscRing(8))
            .build());

        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("memory")
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8).memory(MemoryMode.offHeapSlots()))
            .build());

    }

    @Test
    void validatesPhaseThreePlacementAndMemoryBudgets() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-cpu")
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded().pin(PinPolicy.cpu(1024)))
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build());

        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-cpuset")
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded().pin(PinPolicy.cpuSet(1024)))
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build());

        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-budget")
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8).memory(MemoryMode.offHeapHandles(8)))
            .build());

        StaticGraph.builder("valid-budget")
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8).memory(MemoryMode.offHeapHandles(64)))
            .build();
    }

    @Test
    void rejectsNonPowerOfTwoCapacity() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-capacity")
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(10))
            .build());
    }

    static StaticGraph.Builder phaseOneBuilder() {
        return StaticGraph.builder("phase1-orders")
            .source("ingress", Order.class)
            .stage(
                "validate",
                Order.class,
                ValidOrder.class,
                (order, out, ctx) -> {
                    if (order.valid()) {
                        out.push(new ValidOrder(order.id()));
                    }
                },
                StageSpec.singleThreaded()
            )
            .sink("egress", ValidOrder.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "validate", EdgeSpec.mpscRing(1024))
            .edge("validate", "egress", EdgeSpec.spscRing(1024));
    }

    record Order(int id, boolean valid) {
    }

    record ValidOrder(int id) {
    }
}
