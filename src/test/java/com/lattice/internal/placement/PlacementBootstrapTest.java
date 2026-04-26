package com.lattice.internal.placement;

import com.lattice.internal.edge.MessageEdge;
import com.lattice.metrics.PlacementStatus;
import com.lattice.placement.PinPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class PlacementBootstrapTest {

    @Test
    void noPlacementRequestDoesNotRequireNativeSupport() {
        final PlacementResult result = PlacementBootstrap.bootstrap(
            "stage",
            PinPolicy.none(),
            new MessageEdge[0]
        );

        assertEquals(PlacementStatus.NOT_REQUESTED, result.status());
        assertEquals(-1, result.expectedCpu());
        assertEquals(-1, result.expectedNumaNode());
        assertFalse(result.affinityViolation());
        assertFalse(result.numaViolation());
        assertFalse(result.message().isBlank());
    }
}
