package com.lattice.nativeaccess;

import java.util.BitSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeTopologySnapshotTest {

    @Test
    void unloadedSnapshotDoesNotQueryNativeTopology() {
        final RecordingProbe probe = RecordingProbe.unloaded("disabled");

        final NativeTopologySnapshot snapshot = NativeTopologySnapshot.capture(probe);

        assertFalse(snapshot.loaded());
        assertFalse(snapshot.hasCapabilities());
        assertEquals("disabled", snapshot.failureMessage());
        assertEquals(0, snapshot.cpuTopology().allowedCpuCount());
        assertEquals(0, probe.capabilityCalls);
        assertEquals(0, probe.maxCalls);
        assertEquals(0, probe.allowedCalls);
    }

    @Test
    void loadedSnapshotWithUnavailableCapabilitiesPreservesFailureContext() {
        final RecordingProbe nullCapabilities = RecordingProbe.loadedWithoutCapabilities();

        final NativeTopologySnapshot snapshot = NativeTopologySnapshot.capture(nullCapabilities);

        assertTrue(snapshot.loaded());
        assertFalse(snapshot.hasCapabilities());
        assertTrue(snapshot.failure() instanceof NativeTopologyException);
        assertTrue(snapshot.failureMessage().contains("capabilities are unavailable"));
        assertThrows(NativeTopologyUnavailableException.class, snapshot::capabilities);
        assertEquals(-1, snapshot.maxCpuCount());
        assertEquals(-1, snapshot.configuredCpuCount());
        assertEquals(-1, snapshot.onlineCpuCount());
        assertEquals(0, snapshot.cpuTopology().allowedCpuCount());
        assertEquals(1, nullCapabilities.capabilityCalls);

        final RecordingProbe throwingCapabilities = RecordingProbe.throwingCapabilities("boom");
        final NativeTopologySnapshot throwingSnapshot = NativeTopologySnapshot.capture(throwingCapabilities);
        assertTrue(throwingSnapshot.loaded());
        assertFalse(throwingSnapshot.hasCapabilities());
        assertEquals("boom", throwingSnapshot.failureMessage());
        assertThrows(NativeTopologyUnavailableException.class, throwingSnapshot::capabilities);
    }

    @Test
    void cpuCountsAreCachedWithoutCpuTopologyScan() {
        final RecordingProbe probe = RecordingProbe.loaded(
            NativeCapabilities.AFFINITY,
            8,
            4,
            6,
            new BitSet(),
            new int[8]
        );
        final NativeTopologySnapshot snapshot = NativeTopologySnapshot.capture(probe);

        assertEquals(8, snapshot.maxCpuCount());
        assertEquals(6, snapshot.onlineCpuCount());
        assertEquals(8, snapshot.maxCpuCount());

        assertEquals(1, probe.capabilityCalls);
        assertEquals(1, probe.maxCalls);
        assertEquals(1, probe.configuredCalls);
        assertEquals(1, probe.onlineCalls);
        assertEquals(0, probe.allowedCalls);
        assertEquals(0, probe.numaCalls);
    }

    @Test
    void cpuTopologyScansAllowedNumaMetadataOnce() {
        final BitSet allowed = new BitSet();
        allowed.set(0);
        allowed.set(2);
        allowed.set(5);
        final int[] numaByCpu = {0, -1, 0, -1, 1, 1, -1, -1};
        final RecordingProbe probe = RecordingProbe.loaded(
            NativeCapabilities.AFFINITY | NativeCapabilities.NUMA_QUERY,
            8,
            4,
            6,
            allowed,
            numaByCpu
        );
        final NativeTopologySnapshot snapshot = NativeTopologySnapshot.capture(probe);

        final NativeTopologySnapshot.CpuTopology first = snapshot.cpuTopology();
        final NativeTopologySnapshot.CpuTopology second = snapshot.cpuTopology();

        assertEquals(3, first.allowedCpuCount());
        assertTrue(first.isCpuAllowed(2));
        assertFalse(first.isCpuAllowed(4));
        assertEquals(1, first.numaNodeOfCpu(5));
        assertEquals(1, snapshot.cachedNumaNodeOfCpu(5));
        assertTrue(second.hasNumaMetadata());
        assertEquals(6, first.counts().scanLimit());
        assertFalse(first.isCpuAllowed(-1));
        assertFalse(first.isCpuAllowed(99));
        assertEquals(-1, first.numaNodeOfCpu(-1));
        assertEquals(-1, first.numaNodeOfCpu(99));
        final BitSet returned = first.allowedCpus();
        returned.set(7);
        assertFalse(first.allowedCpus().get(7));
        assertEquals(1, probe.maxCalls);
        assertEquals(1, probe.configuredCalls);
        assertEquals(1, probe.onlineCalls);
        assertEquals(6, probe.allowedCalls);
        assertEquals(3, probe.numaCalls);
    }

    @Test
    void cachedNumaLookupDoesNotTriggerCpuTopologyScan() {
        final BitSet allowed = new BitSet();
        allowed.set(0);
        final RecordingProbe probe = RecordingProbe.loaded(
            NativeCapabilities.AFFINITY | NativeCapabilities.NUMA_QUERY,
            8,
            4,
            4,
            allowed,
            new int[] {0, -1, -1, -1}
        );
        final NativeTopologySnapshot snapshot = NativeTopologySnapshot.capture(probe);

        assertEquals(-1, snapshot.cachedNumaNodeOfCpu(0));

        assertEquals(0, probe.maxCalls);
        assertEquals(0, probe.allowedCalls);
        assertEquals(0, probe.numaCalls);
    }

    private static final class RecordingProbe implements NativeTopologySnapshot.NativeProbe {
        private final boolean loaded;
        private final String loadFailureMessage;
        private final NativeCapabilities capabilities;
        private final int maxCpuCount;
        private final int configuredCpuCount;
        private final int onlineCpuCount;
        private final BitSet allowedCpus;
        private final int[] numaByCpu;
        private int capabilityCalls;
        private int maxCalls;
        private int configuredCalls;
        private int onlineCalls;
        private int allowedCalls;
        private int numaCalls;
        private RuntimeException capabilityFailure;

        private RecordingProbe(
            final boolean loaded,
            final String loadFailureMessage,
            final NativeCapabilities capabilities,
            final int maxCpuCount,
            final int configuredCpuCount,
            final int onlineCpuCount,
            final BitSet allowedCpus,
            final int[] numaByCpu
        ) {
            this.loaded = loaded;
            this.loadFailureMessage = loadFailureMessage;
            this.capabilities = capabilities;
            this.maxCpuCount = maxCpuCount;
            this.configuredCpuCount = configuredCpuCount;
            this.onlineCpuCount = onlineCpuCount;
            this.allowedCpus = (BitSet) allowedCpus.clone();
            this.numaByCpu = numaByCpu.clone();
        }

        static RecordingProbe unloaded(final String loadFailureMessage) {
            return new RecordingProbe(false, loadFailureMessage, null, -1, -1, -1, new BitSet(), new int[0]);
        }

        static RecordingProbe loadedWithoutCapabilities() {
            return new RecordingProbe(true, "", null, -1, -1, -1, new BitSet(), new int[0]);
        }

        static RecordingProbe throwingCapabilities(final String message) {
            final RecordingProbe probe = new RecordingProbe(true, "", null, -1, -1, -1, new BitSet(), new int[0]);
            probe.capabilityFailure = new NativeTopologyException(message);
            return probe;
        }

        static RecordingProbe loaded(
            final long capabilityBits,
            final int maxCpuCount,
            final int configuredCpuCount,
            final int onlineCpuCount,
            final BitSet allowedCpus,
            final int[] numaByCpu
        ) {
            return new RecordingProbe(
                true,
                "",
                NativeCapabilities.fromBits(capabilityBits),
                maxCpuCount,
                configuredCpuCount,
                onlineCpuCount,
                allowedCpus,
                numaByCpu
            );
        }

        @Override
        public boolean loaded() {
            return loaded;
        }

        @Override
        public String loadFailureMessage() {
            return loadFailureMessage;
        }

        @Override
        public NativeCapabilities capabilities() {
            capabilityCalls++;
            if (capabilityFailure != null) {
                throw capabilityFailure;
            }
            return capabilities;
        }

        @Override
        public int maxCpuCount() {
            maxCalls++;
            return maxCpuCount;
        }

        @Override
        public int configuredCpuCount() {
            configuredCalls++;
            return configuredCpuCount;
        }

        @Override
        public int onlineCpuCount() {
            onlineCalls++;
            return onlineCpuCount;
        }

        @Override
        public boolean isCpuAllowed(final int cpu) {
            allowedCalls++;
            return allowedCpus.get(cpu);
        }

        @Override
        public int numaNodeOfCpu(final int cpu) {
            numaCalls++;
            if (cpu < 0 || cpu >= numaByCpu.length) {
                throw new NativeTopologyException("missing NUMA metadata for CPU " + cpu);
            }
            return numaByCpu[cpu];
        }
    }
}
