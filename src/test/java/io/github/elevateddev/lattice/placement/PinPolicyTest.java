package io.github.elevateddev.lattice.placement;

import java.util.BitSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PinPolicyTest {

    @Test
    void noneRequestsNoExplicitPlacement() {
        final PinPolicy policy = PinPolicy.none();

        assertEquals(PinPolicy.PinKind.NONE, policy.kind());
        assertEquals(-1, policy.cpuId());
        assertEquals(-1, policy.coreId());
        assertEquals(-1, policy.numaNode());
        assertTrue(policy.cpuSet().isEmpty());
        assertFalse(policy.requiresNativePlacement());
    }

    @Test
    void cpuRequestsOneLogicalCpu() {
        final PinPolicy policy = PinPolicy.cpu(3);

        assertEquals(PinPolicy.PinKind.CPU, policy.kind());
        assertEquals(3, policy.cpuId());
        assertEquals(3, policy.coreId());
        assertEquals(-1, policy.numaNode());
        assertTrue(policy.cpuSet().isEmpty());
        assertTrue(policy.requiresNativePlacement());

        assertThrows(IllegalArgumentException.class, () -> PinPolicy.cpu(-1));
    }

    @Test
    void coreIsSourceCompatibleCpuPlacementAlias() {
        final PinPolicy policy = PinPolicy.core(4);

        assertEquals(PinPolicy.PinKind.CORE, policy.kind());
        assertEquals(4, policy.cpuId());
        assertEquals(4, policy.coreId());
        assertEquals(-1, policy.numaNode());
        assertTrue(policy.requiresNativePlacement());

        assertThrows(IllegalArgumentException.class, () -> PinPolicy.core(-1));
    }

    @Test
    void cpuSetVarargsRequestsAnyCpuInSet() {
        final PinPolicy policy = PinPolicy.cpuSet(1, 5, 8, 5);

        assertEquals(PinPolicy.PinKind.CPU_SET, policy.kind());
        assertTrue(policy.cpuSet().get(1));
        assertTrue(policy.cpuSet().get(5));
        assertTrue(policy.cpuSet().get(8));
        assertEquals(-1, policy.cpuId());
        assertEquals(-1, policy.numaNode());
        assertTrue(policy.requiresNativePlacement());

        assertThrows(IllegalArgumentException.class, () -> PinPolicy.cpuSet(-1));
        assertThrows(IllegalArgumentException.class, () -> PinPolicy.cpuSet(1, -2));
        assertThrows(NullPointerException.class, () -> PinPolicy.cpuSet(1, (int[]) null));
    }

    @Test
    void cpuSetBitSetIsDefensivelyCopiedInAndOut() {
        final BitSet requested = new BitSet();
        requested.set(2);

        final PinPolicy policy = PinPolicy.cpuSet(requested);
        requested.set(9);

        assertTrue(policy.cpuSet().get(2));
        assertFalse(policy.cpuSet().get(9));

        final BitSet returned = policy.cpuSet();
        returned.set(11);
        assertFalse(policy.cpuSet().get(11));

        assertThrows(NullPointerException.class, () -> PinPolicy.cpuSet((BitSet) null));
        assertThrows(IllegalArgumentException.class, () -> PinPolicy.cpuSet(new BitSet()));
    }

    @Test
    void numaNodeRequestsNumaPlacement() {
        final PinPolicy policy = PinPolicy.numaNode(2);

        assertEquals(PinPolicy.PinKind.NUMA_NODE, policy.kind());
        assertEquals(2, policy.numaNode());
        assertEquals(-1, policy.cpuId());
        assertTrue(policy.cpuSet().isEmpty());
        assertTrue(policy.requiresNativePlacement());

        assertThrows(IllegalArgumentException.class, () -> PinPolicy.numaNode(-1));
    }

    @Test
    void inheritCpusetRequestsProcessCpuSetPlacement() {
        final PinPolicy policy = PinPolicy.inheritCpuset();

        assertEquals(PinPolicy.PinKind.INHERIT_CPUSET, policy.kind());
        assertEquals(-1, policy.cpuId());
        assertEquals(-1, policy.coreId());
        assertEquals(-1, policy.numaNode());
        assertTrue(policy.cpuSet().isEmpty());
        assertTrue(policy.requiresNativePlacement());
    }
}
