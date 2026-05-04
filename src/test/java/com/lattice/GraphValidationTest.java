package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphBuildException;
import com.lattice.graph.SourceMode;
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
    void rejectsMissingEdgesUnknownEndpointsAndIllegalEndpointDirections() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("no-edges")
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .build());

        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("unknown-source")
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("missing", "egress", EdgeSpec.mpscRing(8))
            .build());

        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("self-edge")
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "ingress", EdgeSpec.mpscRing(8))
            .build());

        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("source-incoming")
            .source("left", Order.class)
            .source("right", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("left", "right", EdgeSpec.mpscRing(8))
            .edge("right", "egress", EdgeSpec.mpscRing(8))
            .build());

        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("sink-outgoing")
            .source("ingress", Order.class)
            .stage("stage", Order.class, Order.class, (order, out, ctx) -> out.push(order), StageSpec.singleThreaded())
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .edge("egress", "stage", EdgeSpec.spscRing(8))
            .build());
    }

    @Test
    void rejectsDanglingStagesCyclesAndIllegalWorkerOwnedMpscEdges() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("dangling-stage")
            .source("ingress", Order.class)
            .stage("validate", Order.class, Order.class, (order, out, ctx) -> out.push(order), StageSpec.singleThreaded())
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "validate", EdgeSpec.mpscRing(8))
            .build());

        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("cycle")
            .source("ingress", Order.class)
            .stage("a", Order.class, Order.class, (order, out, ctx) -> out.push(order), StageSpec.singleThreaded())
            .stage("b", Order.class, Order.class, (order, out, ctx) -> out.push(order), StageSpec.singleThreaded())
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .edge("a", "b", EdgeSpec.spscRing(8))
            .edge("b", "a", EdgeSpec.spscRing(8))
            .build());

        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("worker-mpsc")
            .source("ingress", Order.class)
            .stage("validate", Order.class, Order.class, (order, out, ctx) -> out.push(order), StageSpec.singleThreaded())
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "validate", EdgeSpec.mpscRing(8))
            .edge("validate", "egress", EdgeSpec.mpscRing(8))
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
    void specializesSingleProducerSourceIngressToSpsc() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("default-source")
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.spscRing(8))
            .build());

        final StaticGraph explicitSpsc = StaticGraph.builder("single-source-spsc")
            .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.spscRing(8))
            .build();
        assertEquals(SourceMode.SINGLE_PRODUCER, explicitSpsc.plan().node("ingress").orElseThrow().sourceMode());
        assertEquals(EdgeSpec.EdgeKind.SPSC_RING, explicitSpsc.plan().edge("ingress", "egress").orElseThrow().spec().kind());

        final StaticGraph rewrittenMpsc = StaticGraph.builder("single-source-mpsc")
            .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();
        assertEquals(EdgeSpec.EdgeKind.SPSC_RING, rewrittenMpsc.plan().edge("ingress", "egress").orElseThrow().spec().kind());
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

    @Test
    void rejectsMpscCapacityOneBecauseProducerSequenceCannotDistinguishFullFromFree() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-mpsc-capacity-one")
            .source("ingress", Order.class)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(1))
            .build());

        StaticGraph.builder("valid-spsc-capacity-one")
            .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
            .sink("egress", Order.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.spscRing(1))
            .build();
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
