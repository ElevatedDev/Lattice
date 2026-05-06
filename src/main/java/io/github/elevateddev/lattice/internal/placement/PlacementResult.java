package io.github.elevateddev.lattice.internal.placement;

import io.github.elevateddev.lattice.metrics.PlacementStatus;

public record PlacementResult(
    PlacementStatus status,
    String message,
    int expectedCpu,
    int observedCpu,
    int expectedNumaNode,
    int observedNumaNode,
    String allocationOwner,
    boolean affinityViolation,
    boolean numaViolation
) {
}
