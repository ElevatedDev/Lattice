package com.lattice.graph;

/**
 * Graph-level metrics controls.
 * <p>
 * Metrics are off by default. Enabling hot counters adds per-message counter
 * updates on graph, stage, edge, wait, and routing paths.
 */
public final class MetricsSpec {

    private static final MetricsSpec OFF = new MetricsSpec(false, false, false, false);

    private final boolean hotCounters;
    private final boolean residenceTiming;
    private final boolean stageHistograms;
    private final boolean fusedLogicalEdgeCounters;

    private MetricsSpec(
        final boolean hotCounters,
        final boolean residenceTiming,
        final boolean stageHistograms,
        final boolean fusedLogicalEdgeCounters
    ) {
        this.hotCounters = hotCounters;
        this.residenceTiming = residenceTiming;
        this.stageHistograms = stageHistograms;
        this.fusedLogicalEdgeCounters = fusedLogicalEdgeCounters;
    }

    /**
     * Creates a metrics spec with all metrics disabled.
     */
    public static MetricsSpec off() {
        return OFF;
    }

    /**
     * Returns a copy with hot-path counters enabled or disabled.
     */
    public MetricsSpec hotCounters(final boolean hotCounters) {
        return new MetricsSpec(hotCounters, residenceTiming, stageHistograms, fusedLogicalEdgeCounters);
    }

    /**
     * Returns a copy with edge residence-time tracking enabled or disabled.
     */
    public MetricsSpec residenceTiming(final boolean residenceTiming) {
        return new MetricsSpec(hotCounters, residenceTiming, stageHistograms, fusedLogicalEdgeCounters);
    }

    /**
     * Returns a copy with stage batch/service histograms enabled or disabled.
     */
    public MetricsSpec stageHistograms(final boolean stageHistograms) {
        return new MetricsSpec(hotCounters, residenceTiming, stageHistograms, fusedLogicalEdgeCounters);
    }

    /**
     * Returns a copy with logical counters for fused/elided edges enabled or
     * disabled. This option is dormant unless hot counters are also enabled.
     */
    public MetricsSpec fusedLogicalEdgeCounters(final boolean fusedLogicalEdgeCounters) {
        return new MetricsSpec(hotCounters, residenceTiming, stageHistograms, fusedLogicalEdgeCounters);
    }

    /**
     * Returns whether hot-path counters are enabled.
     */
    public boolean hotCounters() {
        return hotCounters;
    }

    /**
     * Returns whether edge residence-time tracking is enabled.
     */
    public boolean residenceTiming() {
        return residenceTiming;
    }

    /**
     * Returns whether stage histograms are enabled.
     */
    public boolean stageHistograms() {
        return stageHistograms;
    }

    /**
     * Returns whether logical counters for fused/elided edges are enabled.
     */
    public boolean fusedLogicalEdgeCounters() {
        return fusedLogicalEdgeCounters;
    }
}
