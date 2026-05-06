package io.github.elevateddev.lattice.slab;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reference-counted handle for a slab payload.
 * <p>
 * Each retained handle must be released exactly once. The payload returns to
 * the owning {@link SlabPool} when the last retained handle is released.
 *
 * @param <T> payload type
 */
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

    /**
     * Returns the shared payload.
     */
    public T payload() {
        return payload;
    }

    /**
     * Retains the payload and returns another handle sharing the same reference
     * count.
     */
    public SlabHandle<T> retain() {
        if (released.get()) {
            throw new IllegalStateException("slab handle is already released");
        }
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

    /**
     * Returns the current shared reference count.
     */
    public int references() {
        return references.get();
    }

    /**
     * Returns whether this handle instance has been released.
     */
    public boolean released() {
        return released.get();
    }

    /**
     * Releases this handle.
     */
    @Override
    public void close() {
        release();
    }

    /**
     * Releases this handle if it has not already been released.
     */
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
