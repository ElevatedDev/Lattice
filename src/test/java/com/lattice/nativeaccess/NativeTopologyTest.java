package com.lattice.nativeaccess;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.BitSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class NativeTopologyTest {

    @Test
    void snapshotReturnsProcessWideCachedObservation() {
        final NativeTopologySnapshot first = NativeTopology.snapshot();
        final NativeTopologySnapshot second = NativeTopology.snapshot();

        assertSame(first, second);
        assertEquals(NativeTopology.isLoaded(), first.loaded());
        assertNotNull(NativeTopology.loadFailureMessage());
        if (!NativeTopology.isLoaded()) {
            assertEquals(NativeTopology.loadFailureMessage(), first.failureMessage());
        }
    }

    @Test
    void nullCpuSetIsRejectedBeforeNativeAccess() {
        assertThrows(NullPointerException.class, () -> NativeTopology.pinCurrentThreadToCpuSet(null, 1));
    }

    @Test
    void nativeOperationsReportUnavailableWhenLibraryIsNotLoaded() {
        assumeFalse(NativeTopology.isLoaded(), "native topology library is available in this JVM");

        assertUnavailable(NativeTopology::capabilities);
        assertUnavailable(NativeTopology::maxCpuCount);
        assertUnavailable(NativeTopology::configuredCpuCount);
        assertUnavailable(NativeTopology::onlineCpuCount);
        assertUnavailable(NativeTopology::currentCpu);
        assertUnavailable(NativeTopology::currentNumaNode);
        assertUnavailable(() -> NativeTopology.numaNodeOfCpu(0));
        assertUnavailable(() -> NativeTopology.isCpuAllowed(0));
        assertUnavailable(() -> NativeTopology.pinCurrentThreadToCpu(0));
        assertUnavailable(() -> NativeTopology.pinCurrentThreadToNumaNode(0));
        assertUnavailable(() -> NativeTopology.pinCurrentThreadToCpuSet(singleCpuSet(), 1));
        assertUnavailable(NativeTopology::setLocalAllocationPolicy);
        assertUnavailable(() -> NativeTopology.firstTouchMemory(1L, 0L));
    }

    private static void assertUnavailable(final Executable executable) {
        final NativeTopologyUnavailableException exception =
            assertThrows(NativeTopologyUnavailableException.class, executable);

        assertNotNull(exception.getMessage());
    }

    private static BitSet singleCpuSet() {
        final BitSet cpus = new BitSet();
        cpus.set(0);
        return cpus;
    }
}
