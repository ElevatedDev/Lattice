package com.lattice.internal.graph;

import com.lattice.edge.EdgeSpec;

public record EdgeDefinition(
    String from,
    String to,
    Class<?> messageType,
    EdgeSpec spec,
    int declarationOrder,
    int branchIndex,
    boolean redirectOnly
) {

    public String key() {
        return from + "->" + to;
    }
}
