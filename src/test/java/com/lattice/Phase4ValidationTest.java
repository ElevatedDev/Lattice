package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.GraphBuildException;
import com.lattice.graph.StaticGraph;
import com.lattice.routing.DispatchSpec;
import com.lattice.routing.JoinSpec;
import com.lattice.routing.PartitionSpec;
import com.lattice.stage.StageSpec;
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
}
