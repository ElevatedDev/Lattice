package io.github.elevateddev.lattice.internal.graph;

import io.github.elevateddev.lattice.graph.DiagnosticsSpec;
import io.github.elevateddev.lattice.graph.FusionSpec;
import io.github.elevateddev.lattice.graph.GraphPlacementSpec;
import io.github.elevateddev.lattice.graph.MetricsSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphRuntimeConfigTest {

    @Test
    void defaultsMatchDocumentedGraphRuntimeDefaults() {
        final GraphRuntimeConfig config = GraphRuntimeConfig.defaults();

        assertTrue(config.fusion().enabled());
        assertFalse(config.metrics().hotCounters());
        assertFalse(config.placement().topologyAware());
        assertFalse(config.diagnostics().jfr());
        assertFalse(config.jfrEnabled());
        assertFalse(config.fusedLogicalEdgeCountersEnabled());
    }

    @Test
    void derivedFlagsRequireTheirOwningFeatureToBeEnabled() {
        final GraphRuntimeConfig dormantFusedCounters = new GraphRuntimeConfig(
            FusionSpec.defaults(),
            MetricsSpec.off().fusedLogicalEdgeCounters(true),
            GraphPlacementSpec.off(),
            DiagnosticsSpec.off().jfr(true)
        );
        final GraphRuntimeConfig activeFusedCounters = new GraphRuntimeConfig(
            FusionSpec.defaults(),
            MetricsSpec.off().hotCounters(true).fusedLogicalEdgeCounters(true),
            GraphPlacementSpec.off(),
            DiagnosticsSpec.off()
        );

        assertTrue(dormantFusedCounters.jfrEnabled());
        assertFalse(dormantFusedCounters.fusedLogicalEdgeCountersEnabled());
        assertTrue(activeFusedCounters.fusedLogicalEdgeCountersEnabled());
    }

    @Test
    void validatesRequiredRuntimeSpecs() {
        assertThrows(NullPointerException.class,
            () -> new GraphRuntimeConfig(null, MetricsSpec.off(), GraphPlacementSpec.off(), DiagnosticsSpec.off()));
        assertThrows(NullPointerException.class,
            () -> new GraphRuntimeConfig(FusionSpec.defaults(), null, GraphPlacementSpec.off(), DiagnosticsSpec.off()));
        assertThrows(NullPointerException.class,
            () -> new GraphRuntimeConfig(FusionSpec.defaults(), MetricsSpec.off(), null, DiagnosticsSpec.off()));
        assertThrows(NullPointerException.class,
            () -> new GraphRuntimeConfig(FusionSpec.defaults(), MetricsSpec.off(), GraphPlacementSpec.off(), null));
    }
}
