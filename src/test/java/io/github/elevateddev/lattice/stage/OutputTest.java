package io.github.elevateddev.lattice.stage;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.graph.StaticGraph;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutputTest {

    @Test
    void stageOutputPushVariantsPublishToTheOutgoingEdge() throws Exception {
        final List<String> consumed = new CopyOnWriteArrayList<>();
        final StaticGraph graph = StaticGraph.builder("output-contract")
            .source("ingress", Integer.class)
            .stage("format", Integer.class, String.class, (value, out, context) -> {
                if (value == 1) {
                    out.push("push");
                } else if (value == 2) {
                    assertTrue(out.push("timed", Duration.ofMillis(50)));
                } else {
                    assertTrue(out.tryPush("try"));
                }
            }, StageSpec.singleThreaded())
            .sink("egress", String.class, consumed::add, StageSpec.singleThreaded())
            .edge("ingress", "format", EdgeSpec.mpscRing(8))
            .edge("format", "egress", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);
        ingress.emit(2);
        ingress.emit(3);
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(List.of("push", "timed", "try"), List.copyOf(consumed));
    }

    @Test
    void outputCanBeImplementedByAdapters() {
        final RecordingOutput<String> output = new RecordingOutput<>();

        output.push("a");
        assertTrue(output.push("b", Duration.ofMillis(1)));
        assertTrue(output.tryPush("c"));

        assertEquals(List.of("a", "b", "c"), output.items());
    }
}
