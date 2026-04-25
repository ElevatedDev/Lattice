package com.lattice.internal.runtime;

import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class HandleOwnershipAllocationTest {

    @Test
    void singleInputScopeDoesNotAllocateAfterWarmup() {
        final com.sun.management.ThreadMXBean bean = allocationBean();
        if (bean == null) {
            return;
        }

        final Object payload = new Object();
        for (int i = 0; i < 100_000; i++) {
            scopeAndPrepare(payload);
        }

        final long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
        for (int i = 0; i < 250_000; i++) {
            scopeAndPrepare(payload);
        }
        final long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;
        assertTrue(allocated <= 1024, "steady-state ownership scope allocated " + allocated + " bytes");
    }

    private static void scopeAndPrepare(final Object payload) {
        final HandleOwnership.Scope scope = HandleOwnership.scope(payload);
        try {
            if (HandleOwnership.prepareForEnqueue(payload) != payload) {
                throw new AssertionError("plain payload should not be retained");
            }
        } finally {
            scope.close();
        }
    }

    private static com.sun.management.ThreadMXBean allocationBean() {
        final java.lang.management.ThreadMXBean platformBean = ManagementFactory.getThreadMXBean();
        if (!(platformBean instanceof com.sun.management.ThreadMXBean bean) || !bean.isThreadAllocatedMemorySupported()) {
            return null;
        }
        bean.setThreadAllocatedMemoryEnabled(true);
        return bean;
    }
}
