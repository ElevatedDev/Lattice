package com.lattice.routing;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinGroupTest {

    @Test
    void constructorCreatesStableReadOnlySnapshotOfValues() {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("left", "L");
        values.put("right", 17);

        final JoinGroup group = new JoinGroup("stamp-1", values, true, false, "right");
        values.put("late", "ignored");

        assertEquals("stamp-1", group.stamp());
        assertFalse(group.usesLongStamp());
        assertTrue(group.complete());
        assertFalse(group.timedOut());
        assertEquals("right", group.triggeringSource());
        assertIterableEquals(List.of("left", "right"), group.valuesBySource().keySet());
        assertEquals("L", group.valuesBySource().get("left"));
        assertEquals(17, group.valuesBySource().get("right"));
        assertThrows(UnsupportedOperationException.class, () -> group.valuesBySource().put("late", "value"));
    }

    @Test
    void valueReturnsTypedOptionalForPresentValues() {
        final JoinGroup group = new JoinGroup("stamp", Map.of("left", "value"), true, false, "left");

        assertEquals("value", group.value("left", String.class).orElseThrow());
        assertTrue(group.value("missing", String.class).isEmpty());
        assertThrows(ClassCastException.class, () -> group.value("left", Integer.class));
    }

    @Test
    void longStampAdaptsPrimitiveAndNumericObjectStamps() {
        final Map<String, Object> values = new LinkedHashMap<>();
        final JoinGroup runtimeGroup = JoinGroup.reusableRuntimeGroup(values);

        runtimeGroup.resetRuntime(99L, true, false, "left");

        assertTrue(runtimeGroup.usesLongStamp());
        assertEquals(Long.valueOf(99L), runtimeGroup.stamp());
        assertEquals(99L, runtimeGroup.longStamp());
        assertTrue(runtimeGroup.complete());
        assertFalse(runtimeGroup.timedOut());
        assertEquals("left", runtimeGroup.triggeringSource());

        final JoinGroup numericObjectStamp = new JoinGroup(42, Map.of(), false, true, null);
        assertFalse(numericObjectStamp.usesLongStamp());
        assertEquals(42L, numericObjectStamp.longStamp());
        assertEquals("", numericObjectStamp.triggeringSource());
    }

    @Test
    void nonNumericObjectStampCannotBeReadAsLong() {
        final JoinGroup group = new JoinGroup("object-stamp", Map.of(), false, false, null);

        assertFalse(group.usesLongStamp());
        assertEquals("object-stamp", group.stamp());
        assertThrows(IllegalStateException.class, group::longStamp);
    }

    @Test
    void runtimeGroupExposesReusableValuesAndStableSnapshots() {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("left", "L1");
        final JoinGroup group = JoinGroup.reusableRuntimeGroup(values);
        group.resetRuntime(7L, false, true, null);

        final Map<String, Object> snapshot = group.snapshotValuesBySource();
        values.put("left", "L2");
        values.put("right", "R2");

        assertEquals(Map.of("left", "L1"), snapshot);
        assertThrows(UnsupportedOperationException.class, () -> snapshot.put("right", "R1"));
        assertEquals("L2", group.valuesBySource().get("left"));
        assertEquals("R2", group.valuesBySource().get("right"));
        assertFalse(group.complete());
        assertTrue(group.timedOut());
        assertEquals("", group.triggeringSource());

        group.resetRuntime("object-stamp", true, false, "right");
        assertFalse(group.usesLongStamp());
        assertEquals("object-stamp", group.stamp());
        assertTrue(group.complete());
        assertFalse(group.timedOut());
        assertEquals("right", group.triggeringSource());
    }

    @Test
    void requiredConstructorAndRuntimeInputsRejectNull() {
        assertThrows(NullPointerException.class, () -> new JoinGroup(null, Map.of(), true, false, "left"));
        assertThrows(NullPointerException.class, () -> new JoinGroup("stamp", null, true, false, "left"));
        assertThrows(NullPointerException.class, () -> JoinGroup.reusableRuntimeGroup(null));

        final JoinGroup group = JoinGroup.reusableRuntimeGroup(new LinkedHashMap<>());
        assertThrows(NullPointerException.class, () -> group.resetRuntime((Object) null, true, false, "left"));
    }
}
