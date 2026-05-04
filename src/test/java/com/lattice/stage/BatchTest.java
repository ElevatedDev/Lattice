package com.lattice.stage;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchTest {

    @Test
    void defaultIsEmptyReflectsReportedSize() {
        assertTrue(new ListBatch<>(List.of()).isEmpty());
        assertFalse(new ListBatch<>(List.of("item")).isEmpty());
    }

    @Test
    void batchProvidesIndexedReadAccessToDeliveredItems() {
        final Batch<String> batch = new ListBatch<>(List.of("alpha", "beta"));

        assertEquals(2, batch.size());
        assertEquals("alpha", batch.get(0));
        assertEquals("beta", batch.get(1));
        assertThrows(IndexOutOfBoundsException.class, () -> batch.get(2));
    }

    private record ListBatch<T>(List<T> items) implements Batch<T> {
        @Override
        public int size() {
            return items.size();
        }

        @Override
        public T get(final int index) {
            return items.get(index);
        }
    }
}
