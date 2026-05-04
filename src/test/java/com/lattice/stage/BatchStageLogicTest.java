package com.lattice.stage;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchStageLogicTest {

    @Test
    void runtimeInvokesBatchLogicWithNonEmptyBatchesBoundedByPolicy() throws Exception {
        final List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        final List<Integer> consumed = new CopyOnWriteArrayList<>();
        final StaticGraph graph = StaticGraph.builder("batch-stage-contract")
            .source("ingress", Integer.class)
            .batchStage("batch", Integer.class, Integer.class, (batch, out, context) -> {
                assertFalse(batch.isEmpty());
                assertTrue(batch.size() <= 4);
                assertEquals("batch-stage-contract", context.graphName());
                assertEquals("batch", context.stageName());
                batchSizes.add(batch.size());
                for (int i = 0; i < batch.size(); i++) {
                    out.push(batch.get(i) * 10);
                }
            }, StageSpec.singleThreaded().batch(BatchPolicy.maxItems(4)))
            .sink("egress", Integer.class, consumed::add, StageSpec.singleThreaded())
            .edge("ingress", "batch", EdgeSpec.mpscRing(32).batch(BatchPolicy.maxItems(4)))
            .edge("batch", "egress", EdgeSpec.spscRing(32))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 10; i++) {
            ingress.emit(i);
        }
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(List.of(0, 10, 20, 30, 40, 50, 60, 70, 80, 90), List.copyOf(consumed));
        assertFalse(batchSizes.isEmpty());
        assertEquals(10, batchSizes.stream().mapToInt(Integer::intValue).sum());
    }

    @Test
    void logicCanBeInvokedDirectlyByAdaptersWithFakes() throws Exception {
        final Batch<String> batch = new Batch<>() {
            @Override
            public int size() {
                return 2;
            }

            @Override
            public String get(final int index) {
                return List.of("a", "b").get(index);
            }
        };
        final RecordingOutput<String> output = new RecordingOutput<>();
        final BatchStageLogic<String, String> logic = (items, out, context) -> {
            for (int i = 0; i < items.size(); i++) {
                out.push(context.stageName() + ":" + items.get(i));
            }
        };

        logic.onBatch(batch, output, new FixedStageContext("graph", "letters"));

        assertEquals(List.of("letters:a", "letters:b"), output.items());
    }
}
