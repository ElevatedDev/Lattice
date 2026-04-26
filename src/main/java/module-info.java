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
    exports com.staticgraph.runtime.nativeaccess;
}
