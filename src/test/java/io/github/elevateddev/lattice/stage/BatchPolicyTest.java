package io.github.elevateddev.lattice.stage;

import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchPolicyTest {

    @Test
    void disabledPolicyDeliversSingleMessages() {
        final BatchPolicy policy = BatchPolicy.disabled();

        assertEquals(BatchPolicy.BatchKind.DISABLED, policy.kind());
        assertEquals(0, policy.maxItems());
        assertEquals(Duration.ZERO, policy.linger());
    }

    @Test
    void maxItemsPolicyRequiresPositiveLimitAndDoesNotLinger() {
        final BatchPolicy policy = BatchPolicy.maxItems(32);

        assertEquals(BatchPolicy.BatchKind.MAX_ITEMS, policy.kind());
        assertEquals(32, policy.maxItems());
        assertEquals(Duration.ZERO, policy.linger());
        assertThrows(IllegalArgumentException.class, () -> BatchPolicy.maxItems(0));
        assertThrows(IllegalArgumentException.class, () -> BatchPolicy.maxItems(-1));
    }

    @Test
    void lingerPolicyCarriesLimitAndNonNegativeTimeout() {
        final BatchPolicy defaultLimit = BatchPolicy.linger(Duration.ofMillis(1));
        final BatchPolicy explicitLimit = BatchPolicy.linger(8, Duration.ZERO);

        assertEquals(BatchPolicy.BatchKind.LINGER, defaultLimit.kind());
        assertEquals(64, defaultLimit.maxItems());
        assertEquals(Duration.ofMillis(1), defaultLimit.linger());
        assertEquals(BatchPolicy.BatchKind.LINGER, explicitLimit.kind());
        assertEquals(8, explicitLimit.maxItems());
        assertEquals(Duration.ZERO, explicitLimit.linger());
        assertThrows(IllegalArgumentException.class, () -> BatchPolicy.linger(0, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> BatchPolicy.linger(1, Duration.ofNanos(-1)));
        assertThrows(NullPointerException.class, () -> BatchPolicy.linger(1, null));
        assertThrows(NullPointerException.class, () -> BatchPolicy.linger(null));
    }
}
