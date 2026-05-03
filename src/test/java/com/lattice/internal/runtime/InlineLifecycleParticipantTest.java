package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.FusionSpec;
import com.lattice.graph.GraphState;
import com.lattice.graph.SourceMode;
import com.lattice.graph.StaticGraph;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InlineLifecycleParticipantTest {

    @Test
    void inlineSourceLifecycleStopsAfterSourceClosesAndInlineWorkExits() throws Exception {
        final AtomicInteger consumed = new AtomicInteger();
        final StaticGraph graph = StaticGraph.builder("inline-lifecycle")
            .fusion(FusionSpec.defaults().inlineSources(true).elideInlineSourcePhysicalPath(true))
            .source("source", Integer.class, SourceMode.SINGLE_PRODUCER)
            .stage("stage", Integer.class, Integer.class, (item, out, context) -> out.push(item + 1),
                StageSpec.singleThreaded())
            .sink("sink", Integer.class, consumed::addAndGet, StageSpec.singleThreaded())
            .edge("source", "stage", EdgeSpec.spscRing(8))
            .edge("stage", "sink", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        final Emitter<Integer> emitter = graph.emitter("source", Integer.class);
        emitter.emit(1);
        emitter.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(2, consumed.get());
    }
}
