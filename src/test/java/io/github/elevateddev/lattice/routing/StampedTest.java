package io.github.elevateddev.lattice.routing;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StampedTest {

    @Test
    void factoryCreatesStampedPayloadWithPrimitiveLongStamp() {
        final Payload payload = new Payload("order-7");
        final Stamped<Payload> stamped = Stamped.of(123L, payload);

        assertEquals(123L, stamped.stamp());
        assertSame(payload, stamped.value());
        assertEquals(new Stamped<>(123L, payload), stamped);
    }

    @Test
    void constructorPreservesAnyLongStampValue() {
        final Stamped<String> stamped = new Stamped<>(Long.MIN_VALUE, "payload");

        assertEquals(Long.MIN_VALUE, stamped.stamp());
        assertEquals("payload", stamped.value());
    }

    @Test
    void stampedValuesUseRecordValueSemantics() {
        final Stamped<String> left = Stamped.of(1L, "payload");
        final Stamped<String> same = Stamped.of(1L, "payload");
        final Stamped<String> differentStamp = Stamped.of(2L, "payload");
        final Stamped<String> differentValue = Stamped.of(1L, "other");

        assertEquals(left, same);
        assertEquals(left.hashCode(), same.hashCode());
        assertNotEquals(left, differentStamp);
        assertNotEquals(left, differentValue);
    }

    @Test
    void stampedPayloadRequiresNonNullValue() {
        assertThrows(NullPointerException.class, () -> Stamped.of(1L, null));
        assertThrows(NullPointerException.class, () -> new Stamped<String>(1L, null));
    }

    private record Payload(String id) {
    }
}
