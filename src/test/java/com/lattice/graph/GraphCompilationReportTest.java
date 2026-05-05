package com.lattice.graph;

import com.lattice.edge.EdgeSpec;
import com.lattice.placement.PinPolicy;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphCompilationReportTest {

    @Test
    void reportSnapshotsRowsAndSupportsLookups() {
        final GraphCompilationReport.Worker worker = new GraphCompilationReport.Worker(
            "stage",
            GraphPlan.NodeKind.STAGE,
            GraphCompilationReport.OperatorKind.STAGE,
            GraphCompilationReport.WorkerDecisionKind.RUNNABLE,
            GraphCompilationReport.OutputKind.EDGE,
            "",
            PinPolicy.none(),
            null,
            ""
        );
        final GraphCompilationReport.Edge edge = new GraphCompilationReport.Edge(
            "source",
            "stage",
            EdgeSpec.EdgeKind.MPSC_RING,
            EdgeSpec.EdgeKind.SPSC_RING,
            GraphCompilationReport.EdgeUseKind.NORMAL,
            "stage",
            true,
            false,
            GraphCompilationReport.Reason.SOURCE_SPECIALIZED_TO_SPSC,
            "specialized"
        );
        final GraphCompilationReport.Sender sender = new GraphCompilationReport.Sender(
            "source",
            "source->stage",
            GraphCompilationReport.SenderKind.EDGE,
            "",
            GraphCompilationReport.OutputKind.EDGE,
            null,
            ""
        );
        final GraphCompilationReport.Merge merge = new GraphCompilationReport.Merge(
            GraphCompilationReport.MergeKind.STAGE_CHAIN,
            "stage",
            List.of("stage"),
            List.of(),
            "stage",
            null,
            "stage chain"
        );
        final GraphCompilationReport.Fallback fallback = new GraphCompilationReport.Fallback(
            GraphCompilationReport.SubjectKind.EDGE,
            "source->stage",
            GraphCompilationReport.Reason.NON_FUSIBLE_EDGE,
            null
        );
        final List<GraphCompilationReport.Worker> workers = new ArrayList<>(List.of(worker));
        final List<GraphCompilationReport.Edge> edges = new ArrayList<>(List.of(edge));
        final List<GraphCompilationReport.Sender> senders = new ArrayList<>(List.of(sender));
        final List<GraphCompilationReport.Merge> merges = new ArrayList<>(List.of(merge));
        final List<GraphCompilationReport.Fallback> fallbacks = new ArrayList<>(List.of(fallback));

        final GraphCompilationReport report = new GraphCompilationReport(
            " report ",
            new GraphCompilationReport.Controls(true, true, false, false, false),
            workers,
            edges,
            senders,
            merges,
            fallbacks
        );
        workers.clear();
        edges.clear();
        senders.clear();
        merges.clear();
        fallbacks.clear();

        assertEquals("report", report.graphName());
        assertTrue(report.controls().fusionEnabled());
        assertEquals(worker, report.worker("stage").orElseThrow());
        assertEquals(edge, report.edge("source", "stage").orElseThrow());
        assertEquals(List.of(merge), report.mergesForOwner("stage"));
        assertEquals(List.of(fallback), report.fallbacksFor("source->stage"));
        assertTrue(report.hasMerges());
        assertTrue(report.hasFallbacks());
        assertThrows(UnsupportedOperationException.class, () -> report.workers().add(worker));
        assertThrows(UnsupportedOperationException.class, () -> merge.mergedNodes().add("other"));
    }

    @Test
    void reasonCodesAreStableAndDiscoverable() {
        assertEquals("fusion.non_fusible_edge.overflow",
            GraphCompilationReport.Reason.NON_FUSIBLE_EDGE_OVERFLOW.code());
        assertEquals(
            GraphCompilationReport.Reason.NON_FUSIBLE_EDGE_OVERFLOW,
            GraphCompilationReport.Reason.fromCode("fusion.non_fusible_edge.overflow").orElseThrow()
        );
        assertTrue(GraphCompilationReport.Reason.fromCode("missing").isEmpty());
    }

    @Test
    void logicalOnlyReportUsesPlanShapeWithoutPhysicalDecisions() {
        final GraphPlan.Node source = new GraphPlan.Node(
            "source",
            GraphPlan.NodeKind.SOURCE,
            null,
            String.class,
            null,
            SourceMode.SINGLE_PRODUCER
        );
        final GraphPlan.Node sink = new GraphPlan.Node(
            "sink",
            GraphPlan.NodeKind.SINK,
            String.class,
            null,
            null
        );
        final GraphPlan.Edge edge = new GraphPlan.Edge(
            "source",
            "sink",
            String.class,
            EdgeSpec.spscRing(8),
            "sink",
            0,
            false
        );

        final GraphCompilationReport report = GraphCompilationReport.logicalOnly(
            new GraphPlan("logical", List.of(source, sink), List.of(edge), List.of("sink"))
        );

        assertEquals("logical", report.graphName());
        assertFalse(report.controls().fusionEnabled());
        assertEquals(GraphCompilationReport.WorkerDecisionKind.RUNNABLE,
            report.worker("sink").orElseThrow().decision());
        assertEquals(GraphCompilationReport.EdgeUseKind.NORMAL,
            report.edge("source", "sink").orElseThrow().use());
        assertFalse(report.hasMerges());
        assertFalse(report.hasFallbacks());
    }

    @Test
    void summaryRendersHumanReadableRows() {
        final GraphCompilationReport report = new GraphCompilationReport(
            "summary",
            new GraphCompilationReport.Controls(true, false, false, false, false),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(new GraphCompilationReport.Fallback(
                GraphCompilationReport.SubjectKind.GRAPH,
                "summary",
                GraphCompilationReport.Reason.FUSION_DISABLED,
                null
            ))
        );

        final String summary = report.toSummaryString();

        assertTrue(summary.contains("GraphCompilationReport{name=summary}"));
        assertTrue(summary.contains("controls: fusion=enabled"));
        assertTrue(summary.contains("fallbacks:"));
        assertTrue(summary.contains("fusion.disabled graph=summary"));
    }

    @Test
    void validatesRequiredFields() {
        assertThrows(IllegalArgumentException.class, () -> new GraphCompilationReport(
            " ",
            new GraphCompilationReport.Controls(false, false, false, false, false),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of()
        ));
        assertThrows(NullPointerException.class, () -> new GraphCompilationReport.Worker(
            "stage",
            null,
            GraphCompilationReport.OperatorKind.STAGE,
            GraphCompilationReport.WorkerDecisionKind.RUNNABLE,
            GraphCompilationReport.OutputKind.NONE,
            "",
            PinPolicy.none(),
            null,
            ""
        ));
        assertThrows(NullPointerException.class, () -> new GraphCompilationReport.Fallback(
            GraphCompilationReport.SubjectKind.GRAPH,
            "graph",
            null,
            ""
        ));
    }
}
