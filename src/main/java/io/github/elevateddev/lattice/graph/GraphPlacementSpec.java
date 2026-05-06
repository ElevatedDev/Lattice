package io.github.elevateddev.lattice.graph;

/**
 * Graph-level controls for runtime-derived placement behavior.
 * <p>
 * Explicit per-stage pinning remains part of {@link io.github.elevateddev.lattice.stage.StageSpec}.
 */
public final class GraphPlacementSpec {

    private static final GraphPlacementSpec OFF = new GraphPlacementSpec(false, false, false);

    private final boolean topologyAware;
    private final boolean strict;
    private final boolean firstTouch;

    private GraphPlacementSpec(final boolean topologyAware, final boolean strict, final boolean firstTouch) {
        this.topologyAware = topologyAware;
        this.strict = strict;
        this.firstTouch = firstTouch;
    }

    /**
     * Creates a placement spec with runtime-derived placement disabled.
     */
    public static GraphPlacementSpec off() {
        return OFF;
    }

    /**
     * Returns a copy that enables or disables topology-derived stage placement.
     */
    public GraphPlacementSpec topologyAware(final boolean topologyAware) {
        return new GraphPlacementSpec(topologyAware, strict, firstTouch);
    }

    /**
     * Returns a copy that controls whether placement failures fail startup.
     */
    public GraphPlacementSpec strict(final boolean strict) {
        return new GraphPlacementSpec(topologyAware, strict, firstTouch);
    }

    /**
     * Returns a copy that controls whether workers first-touch owned edge
     * storage after placement.
     */
    public GraphPlacementSpec firstTouch(final boolean firstTouch) {
        return new GraphPlacementSpec(topologyAware, strict, firstTouch);
    }

    /**
     * Returns whether topology-aware placement is enabled.
     */
    public boolean topologyAware() {
        return topologyAware;
    }

    /**
     * Returns whether placement failures should fail graph startup.
     */
    public boolean strict() {
        return strict;
    }

    /**
     * Returns whether workers should first-touch owned edge storage.
     */
    public boolean firstTouch() {
        return firstTouch;
    }
}
