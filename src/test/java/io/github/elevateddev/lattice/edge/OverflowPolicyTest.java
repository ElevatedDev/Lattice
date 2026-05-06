package io.github.elevateddev.lattice.edge;

import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OverflowPolicyTest {

    @Test
    void simplePoliciesExposeTheirFullEdgeBehaviorWithoutOptionalState() {
        assertSimplePolicy(OverflowPolicy.block(), OverflowPolicy.OverflowKind.BLOCK);
        assertSimplePolicy(OverflowPolicy.failFast(), OverflowPolicy.OverflowKind.FAIL_FAST);
        assertSimplePolicy(OverflowPolicy.dropLatest(), OverflowPolicy.OverflowKind.DROP_LATEST);
        assertSimplePolicy(OverflowPolicy.dropNewest(), OverflowPolicy.OverflowKind.DROP_LATEST);
        assertSimplePolicy(OverflowPolicy.dropOldest(), OverflowPolicy.OverflowKind.DROP_OLDEST);
    }

    @Test
    void blockForRequiresPositiveTimeoutAndExposesIt() {
        final Duration timeout = Duration.ofMillis(5);
        final OverflowPolicy policy = OverflowPolicy.blockFor(timeout);

        assertEquals(OverflowPolicy.OverflowKind.BLOCK_FOR, policy.kind());
        assertEquals(timeout, policy.timeout());
        assertNull(policy.coalescingKey());
        assertNull(policy.redirectTarget());

        assertThrows(NullPointerException.class, () -> OverflowPolicy.blockFor(null));
        assertThrows(IllegalArgumentException.class, () -> OverflowPolicy.blockFor(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> OverflowPolicy.blockFor(Duration.ofNanos(-1)));
    }

    @Test
    void coalesceByStoresCallerSuppliedKeyExtractor() {
        final OverflowPolicy policy = OverflowPolicy.coalesceBy((Message message) -> message.key());

        assertEquals(OverflowPolicy.OverflowKind.COALESCE, policy.kind());
        assertEquals("orders", policy.coalescingKey().apply(new Message("orders", 17)));
        assertNull(policy.timeout());
        assertNull(policy.redirectTarget());

        assertThrows(NullPointerException.class, () -> OverflowPolicy.coalesceBy(null));
    }

    @Test
    void redirectToRequiresANamedTargetAndExposesIt() {
        final OverflowPolicy policy = OverflowPolicy.redirectTo("overflow-handler");

        assertEquals(OverflowPolicy.OverflowKind.REDIRECT, policy.kind());
        assertEquals("overflow-handler", policy.redirectTarget());
        assertNull(policy.timeout());
        assertNull(policy.coalescingKey());

        assertThrows(NullPointerException.class, () -> OverflowPolicy.redirectTo(null));
        assertThrows(IllegalArgumentException.class, () -> OverflowPolicy.redirectTo(""));
        assertThrows(IllegalArgumentException.class, () -> OverflowPolicy.redirectTo("   "));
    }

    private static void assertSimplePolicy(final OverflowPolicy policy, final OverflowPolicy.OverflowKind kind) {
        assertEquals(kind, policy.kind());
        assertNull(policy.timeout());
        assertNull(policy.coalescingKey());
        assertNull(policy.redirectTarget());
    }

    private record Message(String key, int value) {
    }
}
