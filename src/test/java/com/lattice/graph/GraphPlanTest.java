package com.lattice.graph;

import com.lattice.edge.EdgeSpec;
import com.lattice.placement.PinPolicy;
import com.lattice.stage.StageSpec;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphPlanTest {

    @Test
    void planSnapshotsTopologyCollectionsAndFindsNodesEdgesAndPlacements() {
        final StageSpec stageSpec = StageSpec.singleThreaded().pin(PinPolicy.cpu(3));
        final GraphPlan.Node source = new GraphPlan.Node(
            " ingress ",
            GraphPlan.NodeKind.SOURCE,
            null,
            String.class,
            null,
            SourceMode.SINGLE_PRODUCER,
            true
        );
        final GraphPlan.Node sink = new GraphPlan.Node(
            "sink",
            GraphPlan.NodeKind.SINK,
            String.class,
            null,
            stageSpec
        );
        final GraphPlan.Edge edge = new GraphPlan.Edge("ingress", "sink", String.class, EdgeSpec.spscRing(8));
        final List<GraphPlan.Node> nodes = new ArrayList<>(List.of(source, sink));
        final List<GraphPlan.Edge> edges = new ArrayList<>(List.of(edge));

        final GraphPlan plan = new GraphPlan(" graph ", nodes, edges, List.of("sink"));
        nodes.clear();
        edges.clear();

        assertEquals("graph", plan.name());
        assertEquals(List.of(source, sink), plan.nodes());
        assertEquals(List.of(edge), plan.edges());
        assertEquals(List.of("sink"), plan.workerOrder());
        assertSame(source, plan.node("ingress").orElseThrow());
        assertSame(edge, plan.edge("ingress", "sink").orElseThrow());
        assertEquals(PinPolicy.PinKind.CPU, plan.placement("sink").orElseThrow().pinPolicy().kind());
        assertEquals(3, plan.placement("sink").orElseThrow().expectedCpu());
        assertTrue(plan.node("missing").isEmpty());
        assertTrue(plan.edge("ingress", "missing").isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> plan.nodes().add(source));
    }

    @Test
    void nodeDefaultsNullSourceModeToMultiProducerAndTracksPreallocation() {
        final GraphPlan.Node node = new GraphPlan.Node(
            "source",
            GraphPlan.NodeKind.SOURCE,
            null,
            Integer.class,
            null,
            null,
            true
        );

        assertEquals(SourceMode.MULTI_PRODUCER, node.sourceMode());
        assertTrue(node.preallocatedSource());
    }

    @Test
    void edgeTrimsNamesAndDefaultsAllocationOwner() {
        final GraphPlan.Edge edge = new GraphPlan.Edge(
            " from ",
            " to ",
            Integer.class,
            EdgeSpec.mpscRing(8),
            null,
            2,
            true
        );

        assertEquals("from", edge.from());
        assertEquals("to", edge.to());
        assertEquals(Integer.class, edge.messageType());
        assertEquals("", edge.allocationOwner());
        assertEquals(2, edge.branchIndex());
        assertTrue(edge.redirectOnly());
    }

    @Test
    void placementDefensivelyCopiesCpuSetsAndDerivesExpectedPlacementFields() {
        final BitSet cpus = new BitSet();
        cpus.set(2);
        final GraphPlan.Placement placement = GraphPlan.Placement.from("stage", PinPolicy.cpuSet(cpus));
        cpus.set(7);

        final BitSet returned = placement.expectedCpuSet();
        returned.set(9);

        assertEquals(PinPolicy.PinKind.CPU_SET, placement.pinPolicy().kind());
        assertTrue(placement.expectedCpuSet().get(2));
        assertFalse(placement.expectedCpuSet().get(7));
        assertFalse(placement.expectedCpuSet().get(9));
        assertEquals(-1, placement.expectedCpu());
        assertEquals(-1, placement.expectedNumaNode());
        assertFalse(placement.inheritsCpuset());
        assertNotSame(returned, placement.expectedCpuSet());
    }

    @Test
    void validatesRequiredPlanMetadata() {
        final GraphPlan.Node source = new GraphPlan.Node(
            "source",
            GraphPlan.NodeKind.SOURCE,
            null,
            String.class,
            null
        );

        assertThrows(IllegalArgumentException.class, () -> new GraphPlan(" ", List.of(source), List.of(), List.of()));
        assertThrows(IllegalArgumentException.class,
            () -> new GraphPlan.Node(" ", GraphPlan.NodeKind.SOURCE, null, String.class, null));
        assertThrows(NullPointerException.class,
            () -> new GraphPlan.Node("source", null, null, String.class, null));
        assertThrows(IllegalArgumentException.class,
            () -> new GraphPlan.Edge(" ", "sink", String.class, EdgeSpec.spscRing(1)));
        assertThrows(NullPointerException.class,
            () -> new GraphPlan.Edge("source", "sink", null, EdgeSpec.spscRing(1)));
        assertThrows(NullPointerException.class,
            () -> new GraphPlan.Edge("source", "sink", String.class, null));
        assertThrows(IllegalArgumentException.class,
            () -> new GraphPlan.Placement(" ", PinPolicy.none(), -1, new BitSet(), -1));
        assertThrows(NullPointerException.class,
            () -> new GraphPlan.Placement("stage", null, -1, new BitSet(), -1));
    }
}
