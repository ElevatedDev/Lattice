package io.github.elevateddev.lattice.examples;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.edge.OverflowPolicy;
import io.github.elevateddev.lattice.graph.FusionSpec;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.placement.PinPolicy;
import io.github.elevateddev.lattice.stage.StageSpec;

public final class CompilationReportExample {

    private CompilationReportExample() {
    }

    public static void main(final String[] args) {
        try (StaticGraph fused = fusedPipeline(); StaticGraph notFused = notFusedPipeline()) {
            System.out.println("== fused ==");
            System.out.print(fused.compilationReport().toSummaryString());
            System.out.println("== not-fused ==");
            System.out.print(notFused.compilationReport().toSummaryString());
        }
    }

    private static StaticGraph fusedPipeline() {
        return StaticGraph.builder("report-fused")
            .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
            .stage("parse", Order.class, ParsedOrder.class,
                (order, out, ctx) -> out.push(new ParsedOrder(order.id())),
                StageSpec.singleThreaded())
            .stage("risk", ParsedOrder.class, RiskOrder.class,
                (order, out, ctx) -> out.push(new RiskOrder(order.id())),
                StageSpec.singleThreaded())
            .sink("egress", RiskOrder.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "parse", EdgeSpec.mpscRing(1024))
            .edge("parse", "risk", EdgeSpec.spscRing(1024))
            .edge("risk", "egress", EdgeSpec.spscRing(1024))
            .build();
    }

    private static StaticGraph notFusedPipeline() {
        return StaticGraph.builder("report-not-fused")
            .fusion(FusionSpec.defaults().inlineSources(true))
            .source("ingress", Order.class)
            .stage("parse", Order.class, ParsedOrder.class,
                (order, out, ctx) -> out.push(new ParsedOrder(order.id())),
                StageSpec.singleThreaded())
            .stage("risk", ParsedOrder.class, RiskOrder.class,
                (order, out, ctx) -> out.push(new RiskOrder(order.id())),
                StageSpec.singleThreaded().pin(PinPolicy.cpu(2)))
            .sink("egress", RiskOrder.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "parse", EdgeSpec.mpscRing(1024))
            .edge("parse", "risk", EdgeSpec.spscRing(1024).overflow(OverflowPolicy.failFast()))
            .edge("risk", "egress", EdgeSpec.spscRing(1024))
            .build();
    }

    private record Order(int id) {
    }

    private record ParsedOrder(int id) {
    }

    private record RiskOrder(int id) {
    }
}
