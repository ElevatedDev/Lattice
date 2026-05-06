package io.github.elevateddev.lattice.graph;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.PreallocatedEmitter;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StaticGraphTest {

    @Test
    void builderCompilesStaticTopologyAndDefaultSourceMode() {
        final StaticGraph graph = sourceToSink("builder-plan", ignored -> { }).build();

        assertEquals("builder-plan", graph.plan().name());
        assertEquals(SourceMode.MULTI_PRODUCER, graph.plan().node("source").orElseThrow().sourceMode());
        assertEquals(List.of("sink"), graph.plan().workerOrder());
        assertEquals("builder-plan", graph.compilationReport().graphName());
        assertEquals(GraphCompilationReport.WorkerDecisionKind.RUNNABLE,
            graph.compilationReport().worker("sink").orElseThrow().decision());
        assertEquals(GraphState.NEW, graph.state());
        assertTrue(graph.failure().isEmpty());
    }

    @Test
    void compilationReportShowsSourceSpecialization() {
        final StaticGraph graph = StaticGraph.builder("specialized")
            .source("source", Integer.class, SourceMode.SINGLE_PRODUCER)
            .sink("sink", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("source", "sink", EdgeSpec.mpscRing(8))
            .build();

        final GraphCompilationReport.Edge edge = graph.compilationReport()
            .edge("source", "sink")
            .orElseThrow();

        assertEquals(EdgeSpec.EdgeKind.MPSC_RING, edge.declaredKind());
        assertEquals(EdgeSpec.EdgeKind.SPSC_RING, edge.effectiveKind());
        assertEquals(GraphCompilationReport.Reason.SOURCE_SPECIALIZED_TO_SPSC, edge.reason().orElseThrow());
        assertFalse(graph.compilationReport().hasMerges());
    }

    @Test
    void emitterPublishesItemsAndCloseDrainsAcceptedWork() throws Exception {
        final List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = sourceToSink("emit-close", consumed::add).build();

        graph.start();
        final Emitter<Integer> emitter = graph.emitter("source", Integer.class);

        emitter.emit(1);
        assertTrue(emitter.emit(2, Duration.ofMillis(100)));
        assertTrue(emitter.tryEmit(3));
        emitter.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(List.of(1, 2, 3), consumed);
        assertTrue(emitter.isClosed());
    }

    @Test
    void rejectsEmitterLookupForUnknownWrongTypeAndPreallocatedSources() {
        final StaticGraph graph = StaticGraph.builder("lookup")
            .preallocatedSource("source", Payload.class, Payload::new, 128)
            .sink("sink", Payload.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("source", "sink", EdgeSpec.spscRing(2))
            .build();

        assertThrows(GraphRuntimeException.class, () -> graph.emitter("missing", Payload.class));
        assertThrows(GraphRuntimeException.class, () -> graph.emitter("source", Payload.class));
        assertThrows(GraphRuntimeException.class, () -> graph.preallocatedEmitter("source", Object.class));
        assertThrows(GraphRuntimeException.class, () -> graph.preallocatedEmitter("missing", Payload.class));

        final PreallocatedEmitter<Payload> preallocated = graph.preallocatedEmitter("source", Payload.class);
        assertEquals("source", preallocated.name());
        assertFalse(preallocated.isClosed());
    }

    @Test
    void lifecycleOperationsValidateDocumentedStateTransitions() {
        final StaticGraph graph = sourceToSink("lifecycle", ignored -> { }).build();

        assertTrue(graph.quiesce(Duration.ofMillis(10)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertTrue(graph.stop(Duration.ofMillis(10)));
        graph.abort();
        assertEquals(GraphState.STOPPED, graph.state());

        final StaticGraph running = sourceToSink("resume-state", ignored -> { }).build();
        assertThrows(GraphRuntimeException.class, running::resume);
    }

    private static StaticGraph.Builder sourceToSink(
        final String name,
        final java.util.function.Consumer<? super Integer> sink
    ) {
        return StaticGraph.builder(name)
            .source("source", Integer.class)
            .sink("sink", Integer.class, sink, StageSpec.singleThreaded())
            .edge("source", "sink", EdgeSpec.mpscRing(8));
    }

    private record Payload(int id) {
        private Payload() {
            this(0);
        }
    }
}
