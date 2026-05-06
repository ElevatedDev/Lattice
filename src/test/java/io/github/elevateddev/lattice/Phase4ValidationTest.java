package io.github.elevateddev.lattice;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.edge.OverflowPolicy;
import io.github.elevateddev.lattice.graph.GraphBuildException;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.routing.BroadcastSpec;
import io.github.elevateddev.lattice.routing.DispatchSpec;
import io.github.elevateddev.lattice.routing.JoinSpec;
import io.github.elevateddev.lattice.routing.PartitionSpec;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Phase4ValidationTest {

    @Test
    void planExposesRoutingNodesAndBranchIdentity() {
        final StaticGraph graph = StaticGraph.builder("plan")
            .source("ingress", Integer.class)
            .dispatch("dispatch", Integer.class, DispatchSpec.roundRobin(), StageSpec.singleThreaded())
            .sink("a", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .sink("b", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "dispatch", EdgeSpec.mpscRing(8))
            .edge("dispatch", "a", EdgeSpec.spscRing(8))
            .edge("dispatch", "b", EdgeSpec.spscRing(8))
            .build();

        assertEquals(4, graph.plan().nodes().size());
        assertEquals(0, graph.plan().edge("dispatch", "a").orElseThrow().branchIndex());
        assertEquals(1, graph.plan().edge("dispatch", "b").orElseThrow().branchIndex());
        assertEquals("dispatch", graph.plan().edge("dispatch", "a").orElseThrow().allocationOwner());
    }

    @Test
    void rejectsPartitionLaneCountMismatch() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-partition")
            .source("ingress", Integer.class)
            .partition("partition", Integer.class, PartitionSpec.byKey(value -> value, 2), StageSpec.singleThreaded())
            .sink("only", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "partition", EdgeSpec.mpscRing(8))
            .edge("partition", "only", EdgeSpec.spscRing(8))
            .build());
    }

    @Test
    void rejectsJoinWithoutEnoughInputs() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-join")
            .stampedSource("ingress", Integer.class)
            .join("join", Integer.class, JoinSpec.allOf(group -> 1), StageSpec.singleThreaded())
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "join", EdgeSpec.mpscRing(8))
            .edge("join", "egress", EdgeSpec.spscRing(8))
            .build());
    }

    @Test
    void redirectOverflowRequiresADeclaredRedirectEdge() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-redirect")
            .source("ingress", Integer.class)
            .sink("main", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .sink("dlq", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "main", EdgeSpec.mpscRing(8).overflow(OverflowPolicy.redirectTo("dlq")))
            .build());
    }

    @Test
    void rejectsOversizedWeightedDispatchSchedule() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-weighted")
            .source("ingress", Integer.class)
            .dispatch("route", Integer.class, DispatchSpec.weighted(1_000_000, 1), StageSpec.singleThreaded())
            .sink("left", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .sink("right", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "route", EdgeSpec.mpscRing(8))
            .edge("route", "left", EdgeSpec.spscRing(8))
            .edge("route", "right", EdgeSpec.spscRing(8))
            .build());
    }

    @Test
    void rejectsDropOldestOnEffectiveSpscEdge() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-spsc-drop-oldest")
            .source("ingress", Integer.class, SourceMode.SINGLE_PRODUCER)
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8).overflow(OverflowPolicy.dropOldest()))
            .build());
    }

    @Test
    void broadcastCopyRejectsUnsafeReferencesWithoutCopier() {
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-object-broadcast")
            .source("ingress", Object.class)
            .broadcast("fanout", Object.class, BroadcastSpec.copy(), StageSpec.singleThreaded())
            .sink("left", Object.class, ignored -> { }, StageSpec.singleThreaded())
            .sink("right", Object.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "fanout", EdgeSpec.mpscRing(8))
            .edge("fanout", "left", EdgeSpec.spscRing(8))
            .edge("fanout", "right", EdgeSpec.spscRing(8))
            .build());

        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("bad-mutable-record-broadcast")
            .source("ingress", MutableBroadcast.class)
            .broadcast("fanout", MutableBroadcast.class, BroadcastSpec.copy(), StageSpec.singleThreaded())
            .sink("left", MutableBroadcast.class, ignored -> { }, StageSpec.singleThreaded())
            .sink("right", MutableBroadcast.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "fanout", EdgeSpec.mpscRing(8))
            .edge("fanout", "left", EdgeSpec.spscRing(8))
            .edge("fanout", "right", EdgeSpec.spscRing(8))
            .build());

        final StaticGraph graph = StaticGraph.builder("scalar-record-broadcast")
            .source("ingress", ScalarBroadcast.class)
            .broadcast("fanout", ScalarBroadcast.class, BroadcastSpec.copy(), StageSpec.singleThreaded())
            .sink("left", ScalarBroadcast.class, ignored -> { }, StageSpec.singleThreaded())
            .sink("right", ScalarBroadcast.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "fanout", EdgeSpec.mpscRing(8))
            .edge("fanout", "left", EdgeSpec.spscRing(8))
            .edge("fanout", "right", EdgeSpec.spscRing(8))
            .build();

        assertEquals(4, graph.plan().nodes().size());
    }

    private record ScalarBroadcast(int id, String label) {
    }

    private record MutableBroadcast(List<String> values) {
    }
}
