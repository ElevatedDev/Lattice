/**
 * Lattice public API module.
 * <p>
 * The module exposes the graph DSL, bounded edge configuration, routing
 * primitives, lifecycle metrics, wait and placement configuration, slab-pool
 * helpers, and experimental native topology access used by placement-aware
 * deployments.
 */
module com.lattice {
    requires HdrHistogram;
    requires jdk.jfr;

    exports com.lattice.edge;
    exports com.lattice.graph;
    exports com.lattice.metrics;
    exports com.lattice.placement;
    exports com.lattice.routing;
    exports com.lattice.slab;
    exports com.lattice.stage;
    exports com.lattice.wait;
    exports com.lattice.nativeaccess;
}
