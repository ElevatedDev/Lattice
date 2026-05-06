package io.github.elevateddev.lattice.internal.edge;

import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PaddedLongTest {

    @Test
    void valueDefaultsToZeroAndIsMutable() {
        final PaddedLong padded = new PaddedLong();

        assertEquals(0L, padded.value);
        padded.value = 42L;
        assertEquals(42L, padded.value);
    }

    @Test
    void valueIsSurroundedBySevenLongPaddingFieldsOnEachSide() throws Exception {
        assertPaddingFieldTypes();
        assertEquals(long.class, field("value").getType());
        assertEquals(15, PaddedLong.class.getDeclaredFields().length);
    }

    private static void assertPaddingFieldTypes() throws Exception {
        for (int i = 1; i <= 7; i++) {
            assertEquals(long.class, field("p0" + i).getType());
            assertEquals(long.class, field("p1" + i).getType());
        }
    }

    private static Field field(final String name) throws NoSuchFieldException {
        return PaddedLong.class.getDeclaredField(name);
    }
}
