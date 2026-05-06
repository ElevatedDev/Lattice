/**
 * Lattice public API module.
 * <p>
 * The module exposes the graph DSL, bounded edge configuration, routing
 * primitives, lifecycle metrics, wait and placement configuration, slab-pool
 * helpers, and experimental native topology access used by placement-aware
 * deployments.
 */
module io.github.elevateddev.lattice {
    requires HdrHistogram;
    requires jdk.jfr;

    exports io.github.elevateddev.lattice.edge;
    exports io.github.elevateddev.lattice.graph;
    exports io.github.elevateddev.lattice.metrics;
    exports io.github.elevateddev.lattice.placement;
    exports io.github.elevateddev.lattice.routing;
    exports io.github.elevateddev.lattice.slab;
    exports io.github.elevateddev.lattice.stage;
    exports io.github.elevateddev.lattice.wait;
    exports io.github.elevateddev.lattice.nativeaccess;
}
