package com.lattice.internal.placement;

import com.lattice.metrics.PlacementStatus;

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
