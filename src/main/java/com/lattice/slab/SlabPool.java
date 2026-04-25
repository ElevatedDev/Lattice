package com.lattice.slab;

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public final class SlabPool<T> {

    private final String name;
    private final Semaphore permits;
    private final LongAdder acquired = new LongAdder();
    private final LongAdder released = new LongAdder();

    public SlabPool(final String name, final int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.name = Objects.requireNonNull(name, "name");
        this.permits = new Semaphore(capacity);
    }

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

    public String name() {
        return name;
    }

    public long acquiredCount() {
        return acquired.sum();
    }

    public long releasedCount() {
        return released.sum();
    }

    public long leakedCount() {
        return acquired.sum() - released.sum();
    }

    public int availablePermits() {
        return permits.availablePermits();
    }
}
