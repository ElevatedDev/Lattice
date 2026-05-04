package com.lattice.graph;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreallocationSpecTest {

    @Test
    void factoryPoolDefersSizingToGraphCompilerUntilRequested() {
        final AtomicInteger calls = new AtomicInteger();
        final PreallocationSpec<Item> spec = PreallocationSpec.pool(index -> {
            calls.incrementAndGet();
            return new Item(index);
        });

        assertFalse(spec.fixed());
        assertEquals(0, spec.requestedPoolSize());
        assertNull(spec.fixedPool());
        assertEquals(0, calls.get());
        assertEquals(8, spec.poolSize(8).requestedPoolSize());
    }

    @Test
    void factoryPoolWithExplicitSizeRecordsRequestedSize() {
        final PreallocationSpec<Item> spec = PreallocationSpec.pool(Item::new, 16);

        assertFalse(spec.fixed());
        assertEquals(16, spec.requestedPoolSize());
        assertSame(spec.factory(), spec.factory());
    }

    @Test
    void fixedPoolSnapshotsArrayButReusesCallerItems() {
        final Item first = new Item(1);
        final Item second = new Item(2);
        final Item[] pool = { first, second };

        final PreallocationSpec<Item> spec = PreallocationSpec.fixedPool(pool);
        pool[0] = new Item(99);

        final Item[] returned = spec.fixedPool();
        returned[1] = new Item(100);

        assertTrue(spec.fixed());
        assertNull(spec.factory());
        assertEquals(2, spec.requestedPoolSize());
        assertSame(first, spec.fixedPool()[0]);
        assertSame(second, spec.fixedPool()[1]);
        assertNotSame(returned, spec.fixedPool());
    }

    @Test
    void validatesPoolInputsAndFixedPoolSizeChanges() {
        final PreallocationSpec<Item> factory = PreallocationSpec.pool(Item::new);
        final PreallocationSpec<Item> fixed = PreallocationSpec.fixedPool(new Item[] { new Item(1), new Item(2) });

        assertThrows(NullPointerException.class, () -> PreallocationSpec.pool(null));
        assertThrows(NullPointerException.class, () -> PreallocationSpec.fixedPool(null));
        assertThrows(IllegalArgumentException.class, () -> PreallocationSpec.fixedPool(new Item[0]));
        assertThrows(IllegalArgumentException.class,
            () -> PreallocationSpec.fixedPool(new Item[] { new Item(1), null }));
        assertThrows(IllegalArgumentException.class, () -> factory.poolSize(0));
        assertThrows(IllegalArgumentException.class, () -> fixed.poolSize(4));
        assertEquals(2, fixed.poolSize(2).requestedPoolSize());
    }

    private record Item(int id) {
    }
}
