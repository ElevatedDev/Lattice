package com.lattice.internal.placement;

import com.lattice.metrics.PlacementStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlacementResultTest {

    @Test
    void recordCarriesPlacementObservationAndViolationFlags() {
        final PlacementResult result = new PlacementResult(
            PlacementStatus.DEGRADED,
            "expected cpu",
            2,
            3,
            0,
            1,
            "stage",
            true,
            true
        );

        assertEquals(PlacementStatus.DEGRADED, result.status());
        assertEquals("expected cpu", result.message());
        assertEquals(2, result.expectedCpu());
        assertEquals(3, result.observedCpu());
        assertEquals(0, result.expectedNumaNode());
        assertEquals(1, result.observedNumaNode());
        assertEquals("stage", result.allocationOwner());
        assertTrue(result.affinityViolation());
        assertTrue(result.numaViolation());
    }
}
