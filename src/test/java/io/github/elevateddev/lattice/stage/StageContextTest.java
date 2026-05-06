package io.github.elevateddev.lattice.stage;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.metrics.StageMetrics;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageContextTest {

    @Test
    void runtimeContextExposesGraphStageStateAndOwnedMetrics() throws Exception {
        final AtomicReference<String> graphName = new AtomicReference<>();
        final AtomicReference<String> stageName = new AtomicReference<>();
        final AtomicReference<GraphState> graphState = new AtomicReference<>();
        final AtomicReference<StageMetrics> metrics = new AtomicReference<>();
        final AtomicReference<Boolean> stopping = new AtomicReference<>();
        final StaticGraph graph = StaticGraph.builder("stage-context-contract")
            .source("ingress", Integer.class)
            .stage("observe", Integer.class, Integer.class, (value, out, context) -> {
                graphName.set(context.graphName());
                stageName.set(context.stageName());
                graphState.set(context.graphState());
                metrics.set(context.metrics());
                stopping.set(context.isStopping());
                out.push(value);
            }, StageSpec.singleThreaded())
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "observe", EdgeSpec.mpscRing(8))
            .edge("observe", "egress", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals("stage-context-contract", graphName.get());
        assertEquals("observe", stageName.get());
        assertEquals(GraphState.RUNNING, graphState.get());
        assertSame(graph.metrics().stage("observe"), metrics.get());
        assertFalse(stopping.get());
    }

    @Test
    void fakesCanRepresentStoppingStatesForInterfaceConsumers() {
        final StageContext context = new FixedStageContext(
            "graph",
            "stage",
            GraphState.STOPPING,
            new StageMetrics("stage")
        );

        assertEquals("graph", context.graphName());
        assertEquals("stage", context.stageName());
        assertEquals(GraphState.STOPPING, context.graphState());
        assertTrue(context.isStopping());
    }
}
