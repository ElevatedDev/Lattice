package com.lattice.internal.runtime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrayBatchTest {

    @Test
    void exposesOnlySizedItemsAndClearsVisibleReferences() {
        final ArrayBatch batch = new ArrayBatch(3);
        batch.items()[0] = "a";
        batch.items()[1] = "b";
        batch.items()[2] = "hidden";
        batch.size(2);

        assertEquals(2, batch.size());
        assertEquals("a", batch.get(0));
        assertEquals("b", batch.itemAt(1));
        assertThrows(IndexOutOfBoundsException.class, () -> batch.get(2));

        batch.clear();

        assertTrue(batch.isEmpty());
        assertNull(batch.items()[0]);
        assertNull(batch.items()[1]);
        assertEquals("hidden", batch.items()[2]);
    }
}
