package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import com.lattice.metrics.WorkerState;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageWorkerTest {

    @Test
    void exposesDefaultSingleMessageBatchSizeUsedForWorkerReceiveLoops() {
        assertEquals(64, StageWorker.defaultSingleMessageBatchSize());
    }

    @Test
    void workerRunsStageLogicWithRuntimeContextAndStopsAfterDrain() throws Exception {
        final AtomicInteger consumed = new AtomicInteger();
        final StaticGraph graph = StaticGraph.builder("stage-worker")
            .source("source", Integer.class)
            .stage("stage", Integer.class, Integer.class, (item, out, context) -> {
                assertEquals("stage-worker", context.graphName());
                assertEquals("stage", context.stageName());
                assertEquals(GraphState.RUNNING, context.graphState());
                out.push(item + 1);
            }, StageSpec.singleThreaded())
            .sink("sink", Integer.class, consumed::addAndGet, StageSpec.singleThreaded())
            .edge("source", "stage", EdgeSpec.mpscRing(8))
            .edge("stage", "sink", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        final Emitter<Integer> emitter = graph.emitter("source", Integer.class);
        emitter.emit(1);
        emitter.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(2, consumed.get());
        assertEquals(WorkerState.STOPPED, graph.metrics().stage("stage").workerState());
    }

    @Test
    void fusedStageExceptionCarriesStageMetricsContextAndCause() {
        final RuntimeStageContext context = new RuntimeStageContext(
            RuntimeTestSupport.coordinator(
                "fused",
                GraphState.RUNNING,
                RuntimeTestSupport.graphMetrics("fused", "source", "sink")
            ),
            "stage",
            new com.lattice.metrics.StageMetrics("stage")
        );
        final IllegalStateException cause = new IllegalStateException("boom");
        final StageWorker.FusedStageException failure = new StageWorker.FusedStageException(
            "stage",
            context.metrics(),
            context,
            cause
        );

        assertEquals("stage", failure.stageName());
        assertSame(context.metrics(), failure.metrics());
        assertSame(context, failure.context());
        assertSame(cause, failure.getCause());
    }
}
