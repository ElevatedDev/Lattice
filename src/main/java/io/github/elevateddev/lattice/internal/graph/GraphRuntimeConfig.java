package io.github.elevateddev.lattice.internal.graph;

import io.github.elevateddev.lattice.graph.DiagnosticsSpec;
import io.github.elevateddev.lattice.graph.FusionSpec;
import io.github.elevateddev.lattice.graph.GraphPlacementSpec;
import io.github.elevateddev.lattice.graph.MetricsSpec;
import java.util.Objects;

public record GraphRuntimeConfig(
    FusionSpec fusion,
    MetricsSpec metrics,
    GraphPlacementSpec placement,
    DiagnosticsSpec diagnostics
) {
    public GraphRuntimeConfig {
        Objects.requireNonNull(fusion, "fusion");
        Objects.requireNonNull(metrics, "metrics");
        Objects.requireNonNull(placement, "placement");
        Objects.requireNonNull(diagnostics, "diagnostics");
    }

    public static GraphRuntimeConfig defaults() {
        return new GraphRuntimeConfig(
            FusionSpec.defaults(),
            MetricsSpec.off(),
            GraphPlacementSpec.off(),
            DiagnosticsSpec.off()
        );
    }

    public boolean jfrEnabled() {
        return diagnostics.jfr();
    }

    public boolean fusedLogicalEdgeCountersEnabled() {
        return metrics.hotCounters() && metrics.fusedLogicalEdgeCounters();
    }
}
