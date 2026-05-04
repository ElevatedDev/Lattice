package com.lattice.routing;

import java.util.function.Function;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartitionSpecTest {

    @Test
    void byKeyRecordsExtractorAndFixedLaneCount() {
        final PartitionSpec<Message, String> spec = PartitionSpec.byKey(Message::accountId, 4);

        assertEquals(4, spec.lanes());
        assertEquals(0L, spec.hotKeyThreshold());
        assertEquals("account-7", spec.keyExtractor().apply(new Message("account-7", 1)));
    }

    @Test
    void hotKeyThresholdReturnsIndependentSpecWithSamePartitioning() {
        final Function<Message, String> keyExtractor = Message::accountId;
        final PartitionSpec<Message, String> base = PartitionSpec.byKey(keyExtractor, 2);
        final PartitionSpec<Message, String> thresholded = base.hotKeyThreshold(10L);
        final PartitionSpec<Message, String> disabled = thresholded.hotKeyThreshold(0L);

        assertEquals(0L, base.hotKeyThreshold());
        assertEquals(10L, thresholded.hotKeyThreshold());
        assertEquals(0L, disabled.hotKeyThreshold());
        assertEquals(2, thresholded.lanes());
        assertEquals(2, disabled.lanes());
        assertSame(keyExtractor, thresholded.keyExtractor());
        assertEquals("account-8", thresholded.keyExtractor().apply(new Message("account-8", 2)));
    }

    @Test
    void byKeyRequiresExtractorAndPositiveLaneCount() {
        assertThrows(NullPointerException.class, () -> PartitionSpec.byKey(null, 1));
        assertThrows(IllegalArgumentException.class, () -> PartitionSpec.byKey(Message::accountId, 0));
        assertThrows(IllegalArgumentException.class, () -> PartitionSpec.byKey(Message::accountId, -1));
    }

    @Test
    void hotKeyThresholdRejectsNegativeValues() {
        final PartitionSpec<Message, String> spec = PartitionSpec.byKey(Message::accountId, 1);

        assertThrows(IllegalArgumentException.class, () -> spec.hotKeyThreshold(-1L));
    }

    private record Message(String accountId, int sequence) {
    }
}
