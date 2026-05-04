package com.lattice.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class PlacementStatusTest {

    @Test
    void exposesDocumentedPlacementOutcomes() {
        assertArrayEquals(
            new PlacementStatus[] {
                PlacementStatus.NOT_REQUESTED,
                PlacementStatus.APPLIED,
                PlacementStatus.DEGRADED,
                PlacementStatus.UNAVAILABLE,
                PlacementStatus.FAILED
            },
            PlacementStatus.values()
        );
    }

    @Test
    void resolvesPlacementOutcomesByStableName() {
        for (final PlacementStatus status : PlacementStatus.values()) {
            assertSame(status, PlacementStatus.valueOf(status.name()));
        }
    }
}
