package io.github.elevateddev.lattice.internal.edge;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.edge.OverflowPolicy;
import io.github.elevateddev.lattice.internal.graph.EdgeDefinition;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeFactoryTest {

    @Test
    void createsSpscRingAndFirstTouchesByDefault() {
        final EdgeMetrics metrics = EdgeTestSupport.edgeMetrics();
        final GraphMetrics graphMetrics = EdgeTestSupport.graphMetrics();
        final EdgeDefinition definition = EdgeTestSupport.definition(EdgeSpec.spscRing(4));

        final MessageEdge edge = EdgeFactory.create(definition, metrics, graphMetrics);

        assertInstanceOf(SpscRingEdge.class, edge);
        assertEquals(EdgeTestSupport.FROM, edge.from());
        assertEquals(EdgeTestSupport.TO, edge.to());
        assertEquals(4, edge.capacity());
        assertSame(metrics, edge.metrics());
        assertEquals(1, metrics.firstTouchCount());
        assertTrue(edge.offer("ready"));
        assertEquals("ready", edge.poll());
    }

    @Test
    void createsMpscRingAndCanDeferFirstTouchToWorker() {
        final EdgeMetrics metrics = EdgeTestSupport.edgeMetrics();
        final GraphMetrics graphMetrics = EdgeTestSupport.graphMetrics();
        final EdgeDefinition definition = EdgeTestSupport.definition(EdgeSpec.mpscRing(4));

        final MessageEdge edge = EdgeFactory.create(definition, metrics, graphMetrics, false, true);

        assertInstanceOf(MpscRingEdge.class, edge);
        assertEquals(0, metrics.firstTouchCount());
        edge.firstTouch(EdgeTestSupport.TO);
        assertEquals(1, metrics.firstTouchCount());
        assertTrue(edge.offer("ready"));
        assertEquals("ready", edge.poll());
    }

    @Test
    void sourceIngressDefinitionEnablesCloseGuardWithoutChangingAcceptedItemSemantics() {
        final EdgeMetrics metrics = EdgeTestSupport.edgeMetrics();
        final GraphMetrics graphMetrics = EdgeTestSupport.graphMetrics();
        final EdgeDefinition sourceIngress = new EdgeDefinition(
            EdgeTestSupport.FROM,
            EdgeTestSupport.TO,
            Object.class,
            EdgeSpec.spscRing(4),
            0,
            0,
            false,
            true
        );

        final MessageEdge edge = EdgeFactory.create(sourceIngress, metrics, graphMetrics);

        assertTrue(edge.offer("accepted"));
        edge.close();
        assertFalse(edge.offer("rejected"));
        assertEquals("accepted", edge.poll());
    }

    @Test
    void lossyOverflowPoliciesStillCreateUsableEdges() {
        final MessageEdge dropOldestEdge = EdgeFactory.create(
            EdgeTestSupport.definition(EdgeSpec.spscRing(2).overflow(OverflowPolicy.dropOldest())),
            EdgeTestSupport.edgeMetrics(),
            EdgeTestSupport.graphMetrics()
        );
        final MessageEdge coalescingEdge = EdgeFactory.create(
            EdgeTestSupport.definition(EdgeSpec.mpscRing(2).overflow(OverflowPolicy.coalesceBy(Object::toString))),
            EdgeTestSupport.edgeMetrics(),
            EdgeTestSupport.graphMetrics()
        );

        assertTrue(dropOldestEdge.offer("a"));
        assertEquals("a", dropOldestEdge.dropOldest());
        assertTrue(dropOldestEdge.offer("b"));
        assertEquals("b", dropOldestEdge.poll());

        assertTrue(coalescingEdge.offer("x"));
        assertTrue(coalescingEdge.tryCoalesce("x2", item -> item.toString().substring(0, 1)));
        assertEquals("x2", coalescingEdge.poll());
    }
}
