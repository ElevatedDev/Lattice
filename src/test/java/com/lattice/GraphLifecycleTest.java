package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphLifecycleTest {

    @Test
    void runsSourceStageSinkEndToEndAndStopsAfterSourceClose() throws Exception {
        final List<GraphValidationTest.ValidOrder> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = StaticGraph.builder("orders")
            .source("ingress", GraphValidationTest.Order.class)
            .stage(
                "validate",
                GraphValidationTest.Order.class,
                GraphValidationTest.ValidOrder.class,
                (order, out, ctx) -> {
                    if (order.valid()) {
                        out.push(new GraphValidationTest.ValidOrder(order.id()));
                    }
                },
                StageSpec.singleThreaded()
            )
            .sink("egress", GraphValidationTest.ValidOrder.class, consumed::add, StageSpec.singleThreaded())
            .edge("ingress", "validate", EdgeSpec.mpscRing(64))
            .edge("validate", "egress", EdgeSpec.spscRing(64))
            .build();

        graph.start();
        final Emitter<GraphValidationTest.Order> ingress = graph.emitter("ingress", GraphValidationTest.Order.class);
        for (int i = 0; i < 100; i++) {
            ingress.emit(new GraphValidationTest.Order(i, i % 2 == 0));
        }
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(50, consumed.size());
        assertEquals(100, graph.metrics().stage("validate").consumedCount());
        assertEquals(50, graph.metrics().stage("validate").emittedCount());
        assertEquals(50, graph.metrics().stage("egress").consumedCount());
    }

    @Test
    void stopClosesSourcesAndDrainsQueuedMessages() {
        final List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = StaticGraph.builder("drain")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, consumed::add, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(128))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 128; i++) {
            ingress.emit(i);
        }

        assertTrue(graph.stop(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(128, consumed.size());
    }

    @Test
    void stopBeforeStartIsDeterministic() {
        final StaticGraph graph = GraphValidationTest.phaseOneBuilder().build();

        graph.stop();

        assertEquals(GraphState.STOPPED, graph.state());
    }
}
