package com.lattice.slab;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class SlabHandle<T> implements AutoCloseable {

    private final SlabPool<T> pool;
    private final T payload;
    private final AtomicInteger references;
    private final AtomicBoolean released = new AtomicBoolean();

    SlabHandle(final SlabPool<T> pool, final T payload, final AtomicInteger references) {
        this.pool = Objects.requireNonNull(pool, "pool");
        this.payload = Objects.requireNonNull(payload, "payload");
        this.references = Objects.requireNonNull(references, "references");
    }

    public T payload() {
        return payload;
    }

    public SlabHandle<T> retain() {
        while (true) {
            final int current = references.get();
            if (current <= 0) {
                throw new IllegalStateException("slab handle is already released");
            }
            if (references.compareAndSet(current, current + 1)) {
                return new SlabHandle<>(pool, payload, references);
            }
        }
    }

    public int references() {
        return references.get();
    }

    public boolean released() {
        return released.get();
    }

    @Override
    public void close() {
        release();
    }

    public void release() {
        if (!released.compareAndSet(false, true)) {
            return;
        }
        final int remaining = references.decrementAndGet();
        if (remaining < 0) {
            throw new IllegalStateException("slab handle released too many times");
        }
        if (remaining == 0) {
            pool.release(payload);
        }
    }
}
