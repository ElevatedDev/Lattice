package io.github.elevateddev.lattice.wait;

import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WaitSpecTest {

    @Test
    void busySpinDescribesLatencyFirstWaitingWithoutParking() {
        final WaitSpec spec = WaitSpec.busySpin();

        assertEquals(WaitSpec.WaitKind.BUSY_SPIN, spec.kind());
        assertEquals(0, spec.spins());
        assertEquals(0, spec.yields());
        assertEquals(Duration.ZERO, spec.parkNanos());
    }

    @Test
    void phasedDefaultUsesDocumentedPhasedWaitingShape() {
        final WaitSpec spec = WaitSpec.phasedDefault();

        assertEquals(WaitSpec.WaitKind.PHASED, spec.kind());
        assertEquals(10_000, spec.spins());
        assertEquals(50, spec.yields());
        assertEquals(Duration.ofNanos(500), spec.parkNanos());
    }

    @Test
    void phasedWaitAllowsZeroParkAndExposesAllParameters() {
        final WaitSpec spec = WaitSpec.phased(7, 3, Duration.ZERO);

        assertEquals(WaitSpec.WaitKind.PHASED, spec.kind());
        assertEquals(7, spec.spins());
        assertEquals(3, spec.yields());
        assertEquals(Duration.ZERO, spec.parkNanos());
    }

    @Test
    void blockingWaitDescribesBlockingStrategy() {
        final WaitSpec spec = WaitSpec.blocking();

        assertEquals(WaitSpec.WaitKind.BLOCKING, spec.kind());
        assertEquals(0, spec.spins());
        assertEquals(0, spec.yields());
        assertEquals(Duration.ofMillis(1), spec.parkNanos());
    }

    @Test
    void phasedWaitRejectsNegativeCountsAndNegativeParkDuration() {
        assertThrows(IllegalArgumentException.class, () -> WaitSpec.phased(-1, 0, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> WaitSpec.phased(0, -1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> WaitSpec.phased(0, 0, Duration.ofNanos(-1)));
        assertThrows(NullPointerException.class, () -> WaitSpec.phased(0, 0, null));
    }
}
