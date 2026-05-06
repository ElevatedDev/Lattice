package io.github.elevateddev.lattice.metrics;

/**
 * Outcome of applying a stage placement policy.
 */
public enum PlacementStatus {
    /**
     * No placement was requested.
     */
    NOT_REQUESTED,
    /**
     * Placement was applied as requested.
     */
    APPLIED,
    /**
     * Placement ran with a fallback or partial match.
     */
    DEGRADED,
    /**
     * Placement support is unavailable in this runtime.
     */
    UNAVAILABLE,
    /**
     * Placement was requested and failed.
     */
    FAILED
}
