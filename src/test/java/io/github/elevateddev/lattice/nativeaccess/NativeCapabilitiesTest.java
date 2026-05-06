package io.github.elevateddev.lattice.nativeaccess;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeCapabilitiesTest {

    @Test
    void recordComponentsExposeNativeFeatureAvailability() {
        final NativeCapabilities capabilities = new NativeCapabilities(
            true,
            false,
            true,
            false,
            true,
            false
        );

        assertTrue(capabilities.linux());
        assertFalse(capabilities.affinity());
        assertTrue(capabilities.currentCpu());
        assertFalse(capabilities.numaQuery());
        assertTrue(capabilities.localMemoryPolicy());
        assertFalse(capabilities.firstTouch());
    }

    @Test
    void nativeCapabilityBitsMapToRecordComponents() {
        final NativeCapabilities none = NativeCapabilities.fromBits(0L);

        assertFalse(none.linux());
        assertFalse(none.affinity());
        assertFalse(none.currentCpu());
        assertFalse(none.numaQuery());
        assertFalse(none.localMemoryPolicy());
        assertFalse(none.firstTouch());

        final NativeCapabilities all = NativeCapabilities.fromBits(
            NativeCapabilities.LINUX
                | NativeCapabilities.AFFINITY
                | NativeCapabilities.CURRENT_CPU
                | NativeCapabilities.NUMA_QUERY
                | NativeCapabilities.LOCAL_MEMORY_POLICY
                | NativeCapabilities.FIRST_TOUCH
        );

        assertTrue(all.linux());
        assertTrue(all.affinity());
        assertTrue(all.currentCpu());
        assertTrue(all.numaQuery());
        assertTrue(all.localMemoryPolicy());
        assertTrue(all.firstTouch());
    }

    @Test
    void unknownNativeCapabilityBitsAreIgnored() {
        final NativeCapabilities capabilities = NativeCapabilities.fromBits(Long.MIN_VALUE | NativeCapabilities.AFFINITY);

        assertFalse(capabilities.linux());
        assertTrue(capabilities.affinity());
        assertFalse(capabilities.currentCpu());
        assertFalse(capabilities.numaQuery());
        assertFalse(capabilities.localMemoryPolicy());
        assertFalse(capabilities.firstTouch());
    }

    @Test
    void equalityIsValueBasedLikeOtherJavaRecords() {
        final NativeCapabilities left = new NativeCapabilities(true, true, false, false, true, true);
        final NativeCapabilities right = new NativeCapabilities(true, true, false, false, true, true);

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());
    }
}
