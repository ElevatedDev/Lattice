package com.lattice.internal.edge;

import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaddedBooleanTest {

    @Test
    void valueDefaultsToFalseAndIsMutable() {
        final PaddedBoolean padded = new PaddedBoolean();

        assertFalse(padded.value);
        padded.value = true;
        assertTrue(padded.value);
    }

    @Test
    void valueIsSurroundedBySevenLongPaddingFieldsOnEachSide() throws Exception {
        assertPaddingFieldTypes();
        assertEquals(boolean.class, field("value").getType());
        assertEquals(15, PaddedBoolean.class.getDeclaredFields().length);
    }

    private static void assertPaddingFieldTypes() throws Exception {
        for (int i = 1; i <= 7; i++) {
            assertEquals(long.class, field("p0" + i).getType());
            assertEquals(long.class, field("p1" + i).getType());
        }
    }

    private static Field field(final String name) throws NoSuchFieldException {
        return PaddedBoolean.class.getDeclaredField(name);
    }
}
