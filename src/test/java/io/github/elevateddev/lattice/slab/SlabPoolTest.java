package io.github.elevateddev.lattice.slab;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlabPoolTest {

    @Test
    void poolRequiresNameAndPositiveCapacity() {
        assertThrows(NullPointerException.class, () -> new SlabPool<String>(null, 1));
        assertThrows(IllegalArgumentException.class, () -> new SlabPool<>("payloads", 0));
        assertThrows(IllegalArgumentException.class, () -> new SlabPool<>("payloads", -1));
    }

    @Test
    void poolTracksPermitsAndAcquireReleaseCounters() {
        final SlabPool<String> pool = new SlabPool<>("payloads", 2);

        assertEquals("payloads", pool.name());
        assertEquals(2, pool.availablePermits());
        assertEquals(0, pool.acquiredCount());
        assertEquals(0, pool.releasedCount());
        assertEquals(0, pool.leakedCount());

        final SlabHandle<String> first = pool.acquire("first");
        final SlabHandle<String> second = pool.acquire("second");

        assertEquals(0, pool.availablePermits());
        assertEquals(2, pool.acquiredCount());
        assertEquals(0, pool.releasedCount());
        assertEquals(2, pool.leakedCount());

        first.close();
        second.close();

        assertEquals(2, pool.availablePermits());
        assertEquals(2, pool.acquiredCount());
        assertEquals(2, pool.releasedCount());
        assertEquals(0, pool.leakedCount());
    }

    @Test
    void poolRejectsNullPayloadsWithoutConsumingPermits() {
        final SlabPool<String> pool = new SlabPool<>("payloads", 1);

        assertThrows(NullPointerException.class, () -> pool.acquire(null));
        assertEquals(1, pool.availablePermits());
        assertEquals(0, pool.acquiredCount());
        assertEquals(0, pool.releasedCount());
    }

    @Test
    void acquireBlocksUntilAPermitIsReleased() throws Exception {
        final SlabPool<String> pool = new SlabPool<>("bounded", 1);
        final SlabHandle<String> first = pool.acquire("first");
        final CountDownLatch attemptingAcquire = new CountDownLatch(1);
        final CountDownLatch secondAcquired = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        final Future<SlabHandle<String>> second = executor.submit(() -> {
            attemptingAcquire.countDown();
            final SlabHandle<String> handle = pool.acquire("second");
            secondAcquired.countDown();
            return handle;
        });

        assertTrue(attemptingAcquire.await(5, TimeUnit.SECONDS));
        assertFalse(secondAcquired.await(100, TimeUnit.MILLISECONDS));
        assertEquals(0, pool.availablePermits());

        first.release();

        assertTrue(secondAcquired.await(5, TimeUnit.SECONDS));
        final SlabHandle<String> secondHandle = second.get(5, TimeUnit.SECONDS);
        assertEquals("second", secondHandle.payload());
        secondHandle.release();

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(1, pool.availablePermits());
        assertEquals(2, pool.acquiredCount());
        assertEquals(2, pool.releasedCount());
        assertEquals(0, pool.leakedCount());
    }
}
