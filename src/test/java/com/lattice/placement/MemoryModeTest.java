package com.lattice.placement;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MemoryModeTest {

    @Test
    void onHeapSlotsUsesHeapSlotStorageWithoutBudget() {
        final MemoryMode mode = MemoryMode.onHeapSlots();

        assertEquals(MemoryMode.MemoryKind.ON_HEAP_SLOTS, mode.kind());
        assertEquals(0L, mode.bytes());
    }

    @Test
    void offHeapSlotsRemainsExplicitUnsupportedCompatibilityMode() {
        final MemoryMode mode = MemoryMode.offHeapSlots();

        assertEquals(MemoryMode.MemoryKind.OFF_HEAP_SLOTS, mode.kind());
        assertEquals(0L, mode.bytes());
    }

    @Test
    void offHeapHandlesWithoutBudgetLetsCompilerChooseSizing() {
        final MemoryMode mode = MemoryMode.offHeapHandles();

        assertEquals(MemoryMode.MemoryKind.OFF_HEAP_HANDLES, mode.kind());
        assertEquals(0L, mode.bytes());
    }

    @Test
    void offHeapHandlesWithBudgetExposesMinimumMetadataBudget() {
        final MemoryMode mode = MemoryMode.offHeapHandles(4096L);

        assertEquals(MemoryMode.MemoryKind.OFF_HEAP_HANDLES, mode.kind());
        assertEquals(4096L, mode.bytes());
        assertEquals(0L, MemoryMode.offHeapHandles(0L).bytes());

        assertThrows(IllegalArgumentException.class, () -> MemoryMode.offHeapHandles(-1L));
    }
}
