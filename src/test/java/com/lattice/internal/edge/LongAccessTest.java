package com.lattice.internal.edge;

import com.lattice.placement.MemoryMode;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LongAccessTest {

    @Test
    void createUsesDirectStorageOnlyForOffHeapHandleMetadata() {
        assertInstanceOf(LongAccess.Heap.class, LongAccess.create(2, MemoryMode.MemoryKind.ON_HEAP_SLOTS));
        assertInstanceOf(LongAccess.Heap.class, LongAccess.create(2, MemoryMode.MemoryKind.OFF_HEAP_SLOTS));
        assertInstanceOf(LongAccess.Direct.class, LongAccess.create(2, MemoryMode.MemoryKind.OFF_HEAP_HANDLES));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("accessFactories")
    void plainAccessRoundTripsIndependentIndexes(final String name, final Supplier<LongAccess> factory) {
        final LongAccess access = factory.get();

        access.setPlain(0, 11L);
        access.setPlain(1, 22L);

        assertEquals(11L, access.getPlain(0));
        assertEquals(22L, access.getPlain(1));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("accessFactories")
    void releaseAcquireAccessRoundTripsIndependentIndexes(final String name, final Supplier<LongAccess> factory) {
        final LongAccess access = factory.get();

        access.setRelease(0, 33L);
        access.setRelease(1, 44L);

        assertEquals(33L, access.getAcquire(0));
        assertEquals(44L, access.getAcquire(1));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("accessFactories")
    void accessorsUseNormalIndexBounds(final String name, final Supplier<LongAccess> factory) {
        final LongAccess access = factory.get();

        assertThrows(IndexOutOfBoundsException.class, () -> access.getPlain(2));
        assertThrows(IndexOutOfBoundsException.class, () -> access.setPlain(2, 1L));
        assertThrows(IndexOutOfBoundsException.class, () -> access.getAcquire(2));
        assertThrows(IndexOutOfBoundsException.class, () -> access.setRelease(2, 1L));
    }

    private static Stream<Arguments> accessFactories() {
        return Stream.of(
            Arguments.of("heap", (Supplier<LongAccess>) () -> LongAccess.create(2, MemoryMode.MemoryKind.ON_HEAP_SLOTS)),
            Arguments.of("direct", (Supplier<LongAccess>) () -> LongAccess.create(2, MemoryMode.MemoryKind.OFF_HEAP_HANDLES))
        );
    }
}
