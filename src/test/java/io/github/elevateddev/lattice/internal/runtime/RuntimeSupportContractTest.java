package io.github.elevateddev.lattice.internal.runtime;

import io.github.elevateddev.lattice.routing.Stamped;
import io.github.elevateddev.lattice.slab.SlabHandle;
import io.github.elevateddev.lattice.slab.SlabPool;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeSupportContractTest {

    @Test
    void arrayBatchExposesOnlySizedItemsAndClearReleasesReferences() {
        final ArrayBatch batch = new ArrayBatch(3);
        batch.items()[0] = "a";
        batch.items()[1] = "b";
        batch.items()[2] = "hidden";
        batch.size(2);

        assertFalse(batch.isEmpty());
        assertEquals(2, batch.size());
        assertEquals("a", batch.get(0));
        assertEquals("b", batch.itemAt(1));
        assertThrows(IndexOutOfBoundsException.class, () -> batch.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> batch.get(2));

        batch.clear();

        assertTrue(batch.isEmpty());
        assertEquals(0, batch.size());
        assertEquals(null, batch.items()[0]);
        assertEquals(null, batch.items()[1]);
        assertEquals("hidden", batch.items()[2]);
    }

    @Test
    void handleOwnershipRetainsOnlyActiveInputHandlesAndReleasesStampedValues() {
        final SlabPool<String> pool = new SlabPool<>("ownership", 2);
        final SlabHandle<String> active = pool.acquire("active");
        final SlabHandle<String> other = pool.acquire("other");

        try (HandleOwnership.Scope ignored = HandleOwnership.scope(active)) {
            assertTrue(HandleOwnership.active());
            final Object retained = HandleOwnership.prepareForEnqueue(active);
            assertTrue(retained instanceof SlabHandle<?>);
            assertEquals(2, active.references());
            assertSame(other, HandleOwnership.prepareForEnqueue(other));
            ((SlabHandle<?>) retained).release();
        }

        assertFalse(HandleOwnership.active());
        assertEquals(1, active.references());
        active.release();
        other.release();
        assertEquals(0, pool.leakedCount());
    }

    @Test
    void handleOwnershipRetainsStampedHandlesAndNestedScopesRestoreOuterContext() {
        final SlabPool<String> pool = new SlabPool<>("nested", 2);
        final SlabHandle<String> outer = pool.acquire("outer");
        final SlabHandle<String> inner = pool.acquire("inner");
        final Stamped<SlabHandle<String>> stampedOuter = Stamped.of(7, outer);
        final Stamped<SlabHandle<String>> stampedInner = Stamped.of(8, inner);

        try (HandleOwnership.Scope outerScope = HandleOwnership.scope(stampedOuter)) {
            final Object retainedOuter = HandleOwnership.prepareForEnqueue(stampedOuter);
            assertTrue(retainedOuter instanceof Stamped<?>);
            assertEquals(2, outer.references());
            HandleOwnership.releaseIfHandle(retainedOuter);

            try (HandleOwnership.Scope innerScope = HandleOwnership.scope(stampedInner)) {
                final Object retainedInner = HandleOwnership.prepareForEnqueue(stampedInner);
                assertEquals(2, inner.references());
                assertSame(outer, HandleOwnership.prepareForEnqueue(outer));
                HandleOwnership.releaseIfHandle(retainedInner);
            }

            final Object retainedOuterAgain = HandleOwnership.prepareForEnqueue(outer);
            assertEquals(2, outer.references());
            HandleOwnership.releaseIfHandle(retainedOuterAgain);
        }

        outer.release();
        inner.release();
        assertEquals(0, pool.leakedCount());
    }
}
