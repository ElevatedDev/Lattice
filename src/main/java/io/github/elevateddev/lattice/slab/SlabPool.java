package io.github.elevateddev.lattice.slab;

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Bounded pool for shared payload handles.
 * <p>
 * A slab pool limits concurrent outstanding payloads with permits. Acquired
 * handles should be released directly or through try-with-resources.
 *
 * @param <T> payload type
 */
public final class SlabPool<T> {

    private final String name;
    private final Semaphore permits;
    private final LongAdder acquired = new LongAdder();
    private final LongAdder released = new LongAdder();

    /**
     * Creates a named slab pool.
     *
     * @param name pool name used for diagnostics
     * @param capacity maximum number of outstanding payloads
     */
    public SlabPool(final String name, final int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.name = Objects.requireNonNull(name, "name");
        this.permits = new Semaphore(capacity);
    }

    /**
     * Acquires a handle for the payload, blocking until a permit is available.
     */
    public SlabHandle<T> acquire(final T payload) {
        Objects.requireNonNull(payload, "payload");
        permits.acquireUninterruptibly();
        acquired.increment();
        return new SlabHandle<>(this, payload, new AtomicInteger(1));
    }

    void release(final T payload) {
        released.increment();
        permits.release();
    }

    /**
     * Returns the pool name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns how many handles have been acquired.
     */
    public long acquiredCount() {
        return acquired.sum();
    }

    /**
     * Returns how many payloads have been fully released to the pool.
     */
    public long releasedCount() {
        return released.sum();
    }

    /**
     * Returns acquired minus released payload count.
     */
    public long leakedCount() {
        return acquired.sum() - released.sum();
    }

    /**
     * Returns currently available permits.
     */
    public int availablePermits() {
        return permits.availablePermits();
    }
}
