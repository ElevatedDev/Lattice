package io.github.elevateddev.lattice.graph;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsSpecTest {

    @Test
    void offDisablesAllMetricWorkByDefault() {
        final MetricsSpec off = MetricsSpec.off();

        assertFalse(off.hotCounters());
        assertFalse(off.residenceTiming());
        assertFalse(off.stageHistograms());
        assertFalse(off.fusedLogicalEdgeCounters());
    }

    @Test
    void copyMethodsPreserveUnchangedMetricControls() {
        final MetricsSpec base = MetricsSpec.off()
            .hotCounters(true)
            .residenceTiming(true)
            .stageHistograms(true);
        final MetricsSpec fusedCounters = base.fusedLogicalEdgeCounters(true);

        assertTrue(base.hotCounters());
        assertTrue(base.residenceTiming());
        assertTrue(base.stageHistograms());
        assertFalse(base.fusedLogicalEdgeCounters());
        assertTrue(fusedCounters.hotCounters());
        assertTrue(fusedCounters.residenceTiming());
        assertTrue(fusedCounters.stageHistograms());
        assertTrue(fusedCounters.fusedLogicalEdgeCounters());
        assertNotSame(base, fusedCounters);
    }
}
