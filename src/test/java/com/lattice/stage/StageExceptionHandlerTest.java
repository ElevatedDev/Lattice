package com.lattice.stage;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageExceptionHandlerTest {

    @Test
    void failGraphHandlerChoosesGraphFailureForEveryException() {
        final Throwable failure = new IllegalStateException("boom");
        final StageContext context = new FixedStageContext("graph", "stage");
        final StageExceptionHandler handler = StageExceptionHandler.failGraph();

        assertEquals(
            StageExceptionAction.FAIL_GRAPH,
            handler.onException("graph", "stage", failure, context)
        );
    }

    @Test
    void runtimePassesFailureIdentityAndStageContextToCustomHandler() throws Exception {
        final AtomicReference<String> graphName = new AtomicReference<>();
        final AtomicReference<String> stageName = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<StageContext> context = new AtomicReference<>();
        final RuntimeException thrown = new RuntimeException("boom");
        final StaticGraph graph = StaticGraph.builder("exception-handler-contract")
            .exceptionHandler((receivedGraph, receivedStage, receivedFailure, receivedContext) -> {
                graphName.set(receivedGraph);
                stageName.set(receivedStage);
                failure.set(receivedFailure);
                context.set(receivedContext);
                return StageExceptionAction.POISON_STAGE;
            })
            .source("ingress", Integer.class)
            .stage("explode", Integer.class, Integer.class, (value, out, ctx) -> {
                throw thrown;
            }, StageSpec.singleThreaded())
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "explode", EdgeSpec.mpscRing(8))
            .edge("explode", "egress", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        graph.emitter("ingress", Integer.class).emit(1);

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertFalse(graph.failure().isPresent());
        assertEquals("exception-handler-contract", graphName.get());
        assertEquals("explode", stageName.get());
        assertSame(thrown, failure.get());
        assertEquals("exception-handler-contract", context.get().graphName());
        assertEquals("explode", context.get().stageName());
    }
}
