package com.lattice.routing;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JoinSpecTest {

    @Test
    void allOfUsesStampedLongsAndDefaultPolicies() {
        final Function<JoinGroup, String> combiner = group -> group.value("left", String.class).orElse("");
        final JoinSpec<String> spec = JoinSpec.allOf(combiner);
        final Stamped<String> stamped = Stamped.of(42L, "payload");

        assertEquals(JoinSpec.JoinKind.ALL_OF, spec.kind());
        assertSame(combiner, spec.combiner());
        assertTrue(spec.longStamp());
        assertTrue(spec.capacity() > 0);
        assertEquals(Duration.ZERO, spec.timeout());
        assertEquals(JoinSpec.MissingBranchPolicy.DISCARD, spec.missingBranchPolicy());
        assertEquals(JoinSpec.DuplicatePolicy.COUNT, spec.duplicatePolicy());
        assertEquals(42L, spec.extractLongStamp(stamped));
        assertEquals(Long.valueOf(42L), spec.extractStamp(stamped));
        assertEquals(Long.valueOf(42L), spec.stampExtractor().apply(stamped));
        assertThrows(IllegalArgumentException.class, () -> spec.extractLongStamp("not stamped"));
    }

    @Test
    void anyOfUsesStampedLongsAndEmitsOnAnyBranchMode() {
        final JoinSpec<String> spec = JoinSpec.anyOf(JoinSpecTest::describeGroup);

        assertEquals(JoinSpec.JoinKind.ANY_OF, spec.kind());
        assertTrue(spec.longStamp());
        assertEquals(7L, spec.extractLongStamp(Stamped.of(7L, "payload")));
    }

    @Test
    void objectStampConfigurationPreservesJoinPolicies() {
        final JoinSpec<String> spec = JoinSpec.anyOf(JoinSpecTest::describeGroup)
            .capacity(16)
            .timeout(Duration.ofMillis(25))
            .missingBranches(JoinSpec.MissingBranchPolicy.EMIT_PARTIAL)
            .duplicates(JoinSpec.DuplicatePolicy.FAIL)
            .stamp(item -> ((Message) item).accountId());

        assertEquals(JoinSpec.JoinKind.ANY_OF, spec.kind());
        assertFalse(spec.longStamp());
        assertEquals(16, spec.capacity());
        assertEquals(Duration.ofMillis(25), spec.timeout());
        assertEquals(JoinSpec.MissingBranchPolicy.EMIT_PARTIAL, spec.missingBranchPolicy());
        assertEquals(JoinSpec.DuplicatePolicy.FAIL, spec.duplicatePolicy());
        assertEquals("account-7", spec.extractStamp(new Message("account-7", 101L)));
        assertEquals("account-7", spec.stampExtractor().apply(new Message("account-7", 101L)));
        assertThrows(IllegalStateException.class, () -> spec.extractLongStamp(new Message("account-7", 101L)));
    }

    @Test
    void customLongStampConfigurationAvoidsObjectStampMode() {
        final JoinSpec<String> spec = JoinSpec.allOf(JoinSpecTest::describeGroup)
            .stampLong(item -> ((Message) item).sequence());

        assertTrue(spec.longStamp());
        assertEquals(101L, spec.extractLongStamp(new Message("account-7", 101L)));
        assertEquals(Long.valueOf(101L), spec.extractStamp(new Message("account-7", 101L)));
        assertEquals(Long.valueOf(101L), spec.stampExtractor().apply(new Message("account-7", 101L)));
    }

    @Test
    void fluentConfigurationReturnsIndependentSpecs() {
        final JoinSpec<String> first = JoinSpec.allOf(JoinSpecTest::describeGroup)
            .capacity(3)
            .timeout(Duration.ofMillis(1))
            .duplicates(JoinSpec.DuplicatePolicy.IGNORE);
        final JoinSpec<String> second = first
            .capacity(4)
            .timeout(Duration.ofMillis(2))
            .duplicates(JoinSpec.DuplicatePolicy.FAIL);

        assertEquals(3, first.capacity());
        assertEquals(Duration.ofMillis(1), first.timeout());
        assertEquals(JoinSpec.DuplicatePolicy.IGNORE, first.duplicatePolicy());
        assertEquals(4, second.capacity());
        assertEquals(Duration.ofMillis(2), second.timeout());
        assertEquals(JoinSpec.DuplicatePolicy.FAIL, second.duplicatePolicy());
    }

    @Test
    void combinerReceivesJoinGroupAndProducesConfiguredOutput() {
        final JoinSpec<String> spec = JoinSpec.allOf(JoinSpecTest::describeGroup);
        final JoinGroup group = new JoinGroup("stamp", Map.of("left", "L", "right", "R"), true, false, "right");

        assertEquals("stamp:right:L", spec.combiner().apply(group));
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThrows(NullPointerException.class, () -> JoinSpec.allOf(null));
        assertThrows(NullPointerException.class, () -> JoinSpec.anyOf(null));

        final JoinSpec<String> spec = JoinSpec.allOf(JoinSpecTest::describeGroup);
        assertThrows(NullPointerException.class, () -> spec.stamp(null));
        assertThrows(NullPointerException.class, () -> spec.stampLong(null));
        assertThrows(IllegalArgumentException.class, () -> spec.capacity(0));
        assertThrows(IllegalArgumentException.class, () -> spec.capacity(-1));
        assertThrows(NullPointerException.class, () -> spec.timeout(null));
        assertThrows(IllegalArgumentException.class, () -> spec.timeout(Duration.ofNanos(-1)));
        assertThrows(NullPointerException.class, () -> spec.missingBranches(null));
        assertThrows(NullPointerException.class, () -> spec.duplicates(null));
    }

    private static String describeGroup(final JoinGroup group) {
        return group.stamp() + ":" + group.triggeringSource() + ":"
            + group.value("left", String.class).orElse("");
    }

    private record Message(String accountId, long sequence) {
    }
}
