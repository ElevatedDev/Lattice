package io.github.elevateddev.lattice.internal.graph;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.DiagnosticsSpec;
import io.github.elevateddev.lattice.graph.FusionSpec;
import io.github.elevateddev.lattice.graph.GraphPlacementSpec;
import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.stage.BatchPolicy;
import io.github.elevateddev.lattice.stage.StageExceptionHandler;
import io.github.elevateddev.lattice.stage.StageSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StaticGraphBuilderTest {

    @Test
    void builderMethodsReturnSameBuilderAndCompileTrimmedGraphName() {
        final StaticGraphBuilder builder = new StaticGraphBuilder(" graph ");

        assertSame(builder, builder.fusion(FusionSpec.disabled()));
        assertSame(builder, builder.metrics(MetricsSpec.off().hotCounters(true)));
        assertSame(builder, builder.placement(GraphPlacementSpec.off().strict(true)));
        assertSame(builder, builder.diagnostics(DiagnosticsSpec.off().jfr(true)));
        assertSame(builder, builder.exceptionHandler(StageExceptionHandler.failGraph()));

        final StaticGraph graph = builder
            .source("source", String.class, SourceMode.MULTI_PRODUCER)
            .sink("sink", String.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("source", "sink", EdgeSpec.mpscRing(8))
            .build();

        assertEquals("graph", graph.plan().name());
        assertEquals(SourceMode.MULTI_PRODUCER, graph.plan().node("source").orElseThrow().sourceMode());
    }

    @Test
    void rejectsBlankNamesAndNullGraphControlsAtDeclarationTime() {
        assertThrows(IllegalArgumentException.class, () -> new StaticGraphBuilder(" "));

        final StaticGraphBuilder builder = new StaticGraphBuilder("controls");
        assertThrows(NullPointerException.class, () -> builder.fusion(null));
        assertThrows(NullPointerException.class, () -> builder.metrics(null));
        assertThrows(NullPointerException.class, () -> builder.placement(null));
        assertThrows(NullPointerException.class, () -> builder.diagnostics(null));
        assertThrows(NullPointerException.class, () -> builder.exceptionHandler(null));
        assertThrows(IllegalArgumentException.class, () -> builder.source(" ", String.class));
    }

    @Test
    void batchStageRequiresABatchPolicy() {
        final StaticGraphBuilder builder = new StaticGraphBuilder("batch");

        assertThrows(IllegalArgumentException.class,
            () -> builder.batchStage(
                "batch",
                String.class,
                String.class,
                (batch, output, context) -> { },
                StageSpec.singleThreaded()
            ));

        builder.batchStage(
            "batch",
            String.class,
            String.class,
            (batch, output, context) -> { },
            StageSpec.singleThreaded().batch(BatchPolicy.maxItems(2))
        );
    }
}
