package com.lattice.slab;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlabHandleTest {

    @Test
    void retainedHandlesSharePayloadAndReferenceCount() {
        final SlabPool<String> pool = new SlabPool<>("handles", 1);
        final SlabHandle<String> first = pool.acquire("payload");
        final SlabHandle<String> retained = first.retain();

        assertSame("payload", first.payload());
        assertSame(first.payload(), retained.payload());
        assertEquals(2, first.references());
        assertEquals(2, retained.references());
        assertFalse(first.released());
        assertFalse(retained.released());
        assertEquals(0, pool.availablePermits());

        first.release();

        assertTrue(first.released());
        assertFalse(retained.released());
        assertEquals(1, retained.references());
        assertEquals(0, pool.availablePermits());
        assertEquals(0, pool.releasedCount());

        retained.release();

        assertTrue(retained.released());
        assertEquals(0, first.references());
        assertEquals(1, pool.availablePermits());
        assertEquals(1, pool.releasedCount());
    }

    @Test
    void releaseIsIdempotentForEachHandleInstance() {
        final SlabPool<String> pool = new SlabPool<>("idempotent", 1);
        final SlabHandle<String> handle = pool.acquire("payload");

        handle.release();
        handle.release();
        handle.close();

        assertTrue(handle.released());
        assertEquals(0, handle.references());
        assertEquals(1, pool.availablePermits());
        assertEquals(1, pool.releasedCount());
        assertThrows(IllegalStateException.class, handle::retain);
    }

    @Test
    void tryWithResourcesReleasesTheHandle() {
        final SlabPool<String> pool = new SlabPool<>("try-with-resources", 1);

        try (SlabHandle<String> handle = pool.acquire("payload")) {
            assertEquals("payload", handle.payload());
            assertEquals(1, handle.references());
            assertFalse(handle.released());
            assertEquals(0, pool.availablePermits());
        }

        assertEquals(1, pool.availablePermits());
        assertEquals(1, pool.releasedCount());
        assertEquals(0, pool.leakedCount());
    }

    @Test
    void releasedHandlesCannotBeRetainedAgain() {
        final SlabPool<String> pool = new SlabPool<>("released-retain", 1);
        final SlabHandle<String> handle = pool.acquire("payload");

        handle.release();

        assertThrows(IllegalStateException.class, handle::retain);
    }
}
