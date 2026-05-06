package io.github.elevateddev.lattice.internal.runtime;

import io.github.elevateddev.lattice.graph.GraphRuntimeException;
import io.github.elevateddev.lattice.stage.PreallocatedEmitter;
import java.time.Duration;
import java.util.Objects;

final class PreallocatedSourceEmitter<T> implements PreallocatedEmitter<T> {
    private static final Object CLOSED = new Object();

    private final SourceEmitter<T> source;
    private final Object[] pool;
    private final int mask;
    private final int reuseBound;
    private long nextSequence;
    private boolean claimed;
    private Object claimedItem;

    PreallocatedSourceEmitter(
        final SourceEmitter<T> source,
        final Object[] pool,
        final int reuseBound
    ) {
        this.source = Objects.requireNonNull(source, "source");
        this.pool = Objects.requireNonNull(pool, "pool");
        this.mask = pool.length - 1;
        this.reuseBound = reuseBound;
    }

    @Override
    public String name() {
        return source.name();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T claim() {
        if (claimed) {
            throw claimUnavailable();
        }
        final T item = (T) pool[(int) nextSequence & mask];
        nextSequence++;
        claimedItem = item;
        claimed = true;
        return item;
    }

    @Override
    public void emit(final T item) {
        ensureClaimed(item);
        try {
            source.emitPreallocatedTrusted(item);
        } finally {
            clearClaim();
        }
    }

    @Override
    public boolean emit(final T item, final Duration timeout) {
        ensureClaimed(item);
        if (!source.emitPlain(item, timeout)) {
            return false;
        }
        clearClaim();
        return true;
    }

    @Override
    public boolean tryEmit(final T item) {
        ensureClaimed(item);
        if (!source.tryEmitPlain(item)) {
            return false;
        }
        clearClaim();
        return true;
    }

    @Override
    public void discard(final T item) {
        ensureClaimed(item);
        clearClaim();
    }

    @Override
    public int poolSize() {
        return pool.length;
    }

    @Override
    public int reuseBound() {
        return reuseBound;
    }

    @Override
    public boolean isClosed() {
        return source.isClosed();
    }

    @Override
    public void close() {
        claimedItem = CLOSED;
        claimed = true;
        source.close();
    }

    private void ensureClaimed(final T item) {
        final Object current = claimedItem;
        if (!claimed || item != current) {
            throw new GraphRuntimeException("item was not claimed from preallocated source " + source.name());
        }
    }

    private void clearClaim() {
        claimedItem = null;
        claimed = false;
    }

    private GraphRuntimeException claimUnavailable() {
        if (claimedItem == CLOSED) {
            return new GraphRuntimeException("source is closed: " + source.name());
        }
        return new GraphRuntimeException("source " + source.name()
                + " already has an outstanding preallocated item");
    }
}
