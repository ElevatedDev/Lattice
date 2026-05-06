package io.github.elevateddev.lattice.internal.runtime;

import io.github.elevateddev.lattice.routing.Stamped;
import io.github.elevateddev.lattice.slab.SlabHandle;
import io.github.elevateddev.lattice.slab.SlabPool;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HandleOwnershipTest {

    @Test
    void retainsOnlyTheActiveInputHandleWithinScope() {
        final SlabPool<String> pool = new SlabPool<>("ownership", 2);
        final SlabHandle<String> active = pool.acquire("active");
        final SlabHandle<String> other = pool.acquire("other");

        try (HandleOwnership.Scope ignored = HandleOwnership.scope(active)) {
            final Object retained = HandleOwnership.prepareForEnqueue(active);

            assertTrue(HandleOwnership.active());
            assertEquals(2, active.references());
            assertSame(other, HandleOwnership.prepareForEnqueue(other));
            HandleOwnership.releaseIfHandle(retained);
        }

        assertFalse(HandleOwnership.active());
        active.release();
        other.release();
        assertEquals(0, pool.leakedCount());
    }

    @Test
    void stampedHandlesAreRetainedAndReleasedRecursively() {
        final SlabPool<String> pool = new SlabPool<>("stamped", 1);
        final SlabHandle<String> handle = pool.acquire("value");
        final Stamped<SlabHandle<String>> stamped = Stamped.of(1L, handle);

        try (HandleOwnership.Scope ignored = HandleOwnership.scope(stamped)) {
            final Object retained = HandleOwnership.prepareForEnqueue(stamped);

            assertEquals(2, handle.references());
            HandleOwnership.releaseIfHandle(retained);
        }

        handle.release();
        assertEquals(0, pool.leakedCount());
    }
}
