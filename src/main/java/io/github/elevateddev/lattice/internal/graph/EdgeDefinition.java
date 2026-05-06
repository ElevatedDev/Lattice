package io.github.elevateddev.lattice.internal.graph;

import io.github.elevateddev.lattice.edge.EdgeSpec;

public record EdgeDefinition(
    String from,
    String to,
    Class<?> messageType,
    EdgeSpec spec,
    int declarationOrder,
    int branchIndex,
    boolean redirectOnly,
    boolean sourceIngress,
    EdgeSpec.EdgeKind declaredKind
) {

    public EdgeDefinition(
        final String from,
        final String to,
        final Class<?> messageType,
        final EdgeSpec spec,
        final int declarationOrder,
        final int branchIndex,
        final boolean redirectOnly,
        final boolean sourceIngress
    ) {
        this(from, to, messageType, spec, declarationOrder, branchIndex, redirectOnly, sourceIngress, spec.kind());
    }

    public String key() {
        return from + "->" + to;
    }
}
