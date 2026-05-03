package com.lattice.edge;

import com.lattice.placement.MemoryMode;
import com.lattice.stage.BatchPolicy;
import com.lattice.wait.WaitSpec;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EdgeSpecTest {

    @Test
    void spscRingFactoryUsesDocumentedDefaults() {
        final EdgeSpec spec = EdgeSpec.spscRing(1024);

        assertEquals(EdgeSpec.EdgeKind.SPSC_RING, spec.kind());
        assertEquals(1024, spec.capacity());
        assertEquals(WaitSpec.WaitKind.PHASED, spec.waitSpec().kind());
        assertEquals(OverflowPolicy.OverflowKind.BLOCK, spec.overflowPolicy().kind());
        assertEquals(MemoryMode.MemoryKind.ON_HEAP_SLOTS, spec.memoryMode().kind());
        assertEquals(BatchPolicy.BatchKind.DISABLED, spec.batchPolicy().kind());
    }

    @Test
    void mpscRingFactoryUsesDocumentedDefaults() {
        final EdgeSpec spec = EdgeSpec.mpscRing(2048);

        assertEquals(EdgeSpec.EdgeKind.MPSC_RING, spec.kind());
        assertEquals(2048, spec.capacity());
        assertEquals(WaitSpec.WaitKind.PHASED, spec.waitSpec().kind());
        assertEquals(OverflowPolicy.OverflowKind.BLOCK, spec.overflowPolicy().kind());
        assertEquals(MemoryMode.MemoryKind.ON_HEAP_SLOTS, spec.memoryMode().kind());
        assertEquals(BatchPolicy.BatchKind.DISABLED, spec.batchPolicy().kind());
    }

    @Test
    void builderMethodsReturnCopiesAndPreserveUnchangedOptions() {
        final EdgeSpec base = EdgeSpec.mpscRing(8);
        final WaitSpec wait = WaitSpec.blocking();
        final OverflowPolicy overflow = OverflowPolicy.blockFor(Duration.ofMillis(2));
        final MemoryMode memory = MemoryMode.offHeapHandles(128);
        final BatchPolicy batch = BatchPolicy.maxItems(16);

        final EdgeSpec configured = base
            .wait(wait)
            .overflow(overflow)
            .memory(memory)
            .batch(batch)
            .capacity(32)
            .withKind(EdgeSpec.EdgeKind.SPSC_RING);

        assertNotSame(base, configured);
        assertEquals(EdgeSpec.EdgeKind.MPSC_RING, base.kind());
        assertEquals(8, base.capacity());
        assertEquals(WaitSpec.WaitKind.PHASED, base.waitSpec().kind());
        assertEquals(OverflowPolicy.OverflowKind.BLOCK, base.overflowPolicy().kind());
        assertEquals(MemoryMode.MemoryKind.ON_HEAP_SLOTS, base.memoryMode().kind());
        assertEquals(BatchPolicy.BatchKind.DISABLED, base.batchPolicy().kind());

        assertEquals(EdgeSpec.EdgeKind.SPSC_RING, configured.kind());
        assertEquals(32, configured.capacity());
        assertSame(wait, configured.waitSpec());
        assertSame(overflow, configured.overflowPolicy());
        assertSame(memory, configured.memoryMode());
        assertSame(batch, configured.batchPolicy());
    }

    @Test
    void capacityMustBePositiveAtTheSpecBoundary() {
        assertThrows(IllegalArgumentException.class, () -> EdgeSpec.spscRing(0));
        assertThrows(IllegalArgumentException.class, () -> EdgeSpec.mpscRing(-1));
        assertThrows(IllegalArgumentException.class, () -> EdgeSpec.spscRing(8).capacity(0));
    }

    @Test
    void replacementOptionsMustBePresent() {
        final EdgeSpec spec = EdgeSpec.spscRing(8);

        assertThrows(NullPointerException.class, () -> spec.wait(null));
        assertThrows(NullPointerException.class, () -> spec.overflow(null));
        assertThrows(NullPointerException.class, () -> spec.memory(null));
        assertThrows(NullPointerException.class, () -> spec.batch(null));
        assertThrows(NullPointerException.class, () -> spec.withKind(null));
    }
}
