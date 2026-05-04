package com.lattice.routing;

import java.util.function.Function;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DispatchSpecTest {

    @Test
    void roundRobinDeclaresCyclicDispatchWithoutKeyOrWeights() {
        final DispatchSpec<Message> spec = DispatchSpec.roundRobin();

        assertEquals(DispatchSpec.DispatchKind.ROUND_ROBIN, spec.kind());
        assertNull(spec.keyExtractor());
        assertArrayEquals(new int[0], spec.weights());
        assertEquals(0, spec.weightSum());
    }

    @Test
    void keyedDispatchExposesConfiguredKeyExtractor() {
        final Function<Message, Integer> keyExtractor = Message::accountId;
        final DispatchSpec<Message> spec = DispatchSpec.keyed(keyExtractor);

        assertEquals(DispatchSpec.DispatchKind.KEYED, spec.kind());
        assertSame(keyExtractor, spec.keyExtractor());
        assertEquals(42, spec.keyExtractor().apply(new Message(42, "accepted")));
        assertArrayEquals(new int[0], spec.weights());
        assertEquals(0, spec.weightSum());
    }

    @Test
    void weightedDispatchStoresPositiveWeightsDefensively() {
        final int[] configuredWeights = { 1, 3, 2 };
        final DispatchSpec<Message> spec = DispatchSpec.weighted(configuredWeights);

        configuredWeights[0] = 99;

        assertEquals(DispatchSpec.DispatchKind.WEIGHTED, spec.kind());
        assertNull(spec.keyExtractor());
        assertArrayEquals(new int[] { 1, 3, 2 }, spec.weights());
        assertEquals(6, spec.weightSum());

        final int[] returnedWeights = spec.weights();
        returnedWeights[1] = 99;
        assertArrayEquals(new int[] { 1, 3, 2 }, spec.weights());
    }

    @Test
    void keyedDispatchRequiresExtractor() {
        assertThrows(NullPointerException.class, () -> DispatchSpec.keyed(null));
    }

    @Test
    void weightedDispatchRequiresAtLeastOnePositiveWeight() {
        assertThrows(NullPointerException.class, () -> DispatchSpec.weighted((int[]) null));
        assertThrows(IllegalArgumentException.class, DispatchSpec::weighted);
        assertThrows(IllegalArgumentException.class, () -> DispatchSpec.weighted(0));
        assertThrows(IllegalArgumentException.class, () -> DispatchSpec.weighted(1, -1));
    }

    private record Message(int accountId, String status) {
    }
}
