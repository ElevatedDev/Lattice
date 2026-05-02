package com.lattice.internal.graph;

import com.lattice.graph.DiagnosticsSpec;
import com.lattice.graph.FusionSpec;
import com.lattice.graph.GraphPlacementSpec;
import com.lattice.graph.MetricsSpec;
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
