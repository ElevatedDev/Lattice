package io.github.elevateddev.lattice.internal.edge;

import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PaddedLongCacheTest {

    @Test
    void valueDefaultsToZeroAndIsPlainMutableScratchState() {
        final PaddedLongCache padded = new PaddedLongCache();

        assertEquals(0L, padded.value);
        padded.value = 99L;
        assertEquals(99L, padded.value);
    }

    @Test
    void valueIsSurroundedBySevenLongPaddingFieldsOnEachSide() throws Exception {
        assertPaddingFieldTypes();
        assertEquals(long.class, field("value").getType());
        assertEquals(15, PaddedLongCache.class.getDeclaredFields().length);
    }

    private static void assertPaddingFieldTypes() throws Exception {
        for (int i = 1; i <= 7; i++) {
            assertEquals(long.class, field("p0" + i).getType());
            assertEquals(long.class, field("p1" + i).getType());
        }
    }

    private static Field field(final String name) throws NoSuchFieldException {
        return PaddedLongCache.class.getDeclaredField(name);
    }
}
