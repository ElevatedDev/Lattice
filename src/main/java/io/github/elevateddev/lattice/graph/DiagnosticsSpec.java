package io.github.elevateddev.lattice.graph;

/**
 * Graph-level diagnostic event controls.
 */
public final class DiagnosticsSpec {

    private static final DiagnosticsSpec OFF = new DiagnosticsSpec(false);

    private final boolean jfr;

    private DiagnosticsSpec(final boolean jfr) {
        this.jfr = jfr;
    }

    /**
     * Creates a diagnostics spec with diagnostics disabled.
     */
    public static DiagnosticsSpec off() {
        return OFF;
    }

    /**
     * Returns a copy with JFR event emission enabled or disabled.
     */
    public DiagnosticsSpec jfr(final boolean jfr) {
        return new DiagnosticsSpec(jfr);
    }

    /**
     * Returns whether JFR event emission is enabled.
     */
    public boolean jfr() {
        return jfr;
    }
}
