package io.github.elevateddev.lattice;

import io.github.elevateddev.lattice.slab.SlabHandle;
import io.github.elevateddev.lattice.slab.SlabPool;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlabPoolHandleContractTest {

    @Test
    void poolRejectsInvalidCapacityAndNullPayloads() {
        assertThrows(IllegalArgumentException.class, () -> new SlabPool<>("bad", 0));
        assertThrows(NullPointerException.class, () -> new SlabPool<String>(null, 1));

        final SlabPool<String> pool = new SlabPool<>("payloads", 1);
        assertThrows(NullPointerException.class, () -> pool.acquire(null));
        assertEquals("payloads", pool.name());
        assertEquals(1, pool.availablePermits());
        assertEquals(0, pool.acquiredCount());
        assertEquals(0, pool.releasedCount());
        assertEquals(0, pool.leakedCount());
    }

    @Test
    void retainReleaseReturnsPermitOnlyAfterTheLastHandleIsReleased() {
        final SlabPool<String> pool = new SlabPool<>("handles", 1);
        final SlabHandle<String> first = pool.acquire("payload");
        final SlabHandle<String> retained = first.retain();

        assertSame("payload", first.payload());
        assertEquals(2, first.references());
        assertEquals(2, retained.references());
        assertEquals(0, pool.availablePermits());
        assertEquals(1, pool.acquiredCount());
        assertEquals(0, pool.releasedCount());
        assertEquals(1, pool.leakedCount());

        first.release();
        assertTrue(first.released());
        assertFalse(retained.released());
        assertEquals(1, retained.references());
        assertEquals(0, pool.availablePermits());
        assertEquals(0, pool.releasedCount());

        retained.close();
        assertTrue(retained.released());
        assertEquals(0, first.references());
        assertEquals(1, pool.availablePermits());
        assertEquals(1, pool.releasedCount());
        assertEquals(0, pool.leakedCount());
    }

    @Test
    void releaseIsIdempotentPerHandleButReleasedHandlesCannotBeRetainedAgain() {
        final SlabPool<String> pool = new SlabPool<>("idempotent", 1);
        final SlabHandle<String> handle = pool.acquire("payload");

        handle.release();
        handle.release();

        assertTrue(handle.released());
        assertEquals(0, handle.references());
        assertEquals(1, pool.availablePermits());
        assertEquals(1, pool.releasedCount());
        assertThrows(IllegalStateException.class, handle::retain);
    }
}
