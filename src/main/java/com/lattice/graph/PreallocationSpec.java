package com.lattice.graph;

import java.util.Objects;
import java.util.function.IntFunction;

/**
 * Describes the object pool used by a preallocated source.
 * <p>
 * Factory-backed pools are sized by the graph builder from the source reuse
 * bound unless {@link #poolSize(int)} is set. Explicit pool sizes and fixed
 * pools must be powers of two and larger than the computed reuse bound.
 *
 * @param <T> pooled item type emitted by the source
 */
public final class PreallocationSpec<T> {
    private final IntFunction<? extends T> factory;
    private final T[] fixedPool;
    private final int poolSize;

    private PreallocationSpec(
        final IntFunction<? extends T> factory,
        final T[] fixedPool,
        final int poolSize
    ) {
        this.factory = factory;
        this.fixedPool = fixedPool;
        this.poolSize = poolSize;
    }

    public static <T> PreallocationSpec<T> pool(final IntFunction<? extends T> factory) {
        return new PreallocationSpec<>(Objects.requireNonNull(factory, "factory"), null, 0);
    }

    /**
     * Creates a factory-backed pool with an explicit size.
     *
     * @param factory called once per pool slot during graph build
     * @param poolSize requested pool size; must be positive, and graph build
     *                 requires a power of two larger than the source reuse bound
     * @param <T> item type
     * @return preallocation spec
     */
    public static <T> PreallocationSpec<T> pool(
        final IntFunction<? extends T> factory,
        final int poolSize
    ) {
        return new PreallocationSpec<T>(Objects.requireNonNull(factory, "factory"), null, 0)
            .poolSize(poolSize);
    }

    /**
     * Uses caller-provided objects as the source pool.
     * <p>
     * The array is cloned when the spec is created and again when the graph is
     * built. The items themselves are reused and must match the source type.
     *
     * @param pool non-empty pool with no null items
     * @param <T> item type
     * @return preallocation spec
     */
    public static <T> PreallocationSpec<T> fixedPool(final T[] pool) {
        final T[] fixedPool = Objects.requireNonNull(pool, "pool");
        if (fixedPool.length == 0) {
            throw new IllegalArgumentException("preallocated pool must not be empty");
        }
        for (int i = 0; i < fixedPool.length; i++) {
            if (fixedPool[i] == null) {
                throw new IllegalArgumentException("preallocated pool item " + i + " must not be null");
            }
        }
        return new PreallocationSpec<>(null, fixedPool.clone(), fixedPool.length);
    }

    /**
     * Requests an explicit factory-backed pool size.
     * <p>
     * If this is not called for a factory-backed pool, the runtime chooses the
     * next power of two above the source reuse bound. Fixed pools already define
     * their size, so this method only accepts their existing length.
     *
     * @param poolSize requested pool size
     * @return a new spec with the requested size
     */
    public PreallocationSpec<T> poolSize(final int poolSize) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("preallocated pool size must be positive");
        }
        if (fixedPool != null && poolSize != fixedPool.length) {
            throw new IllegalArgumentException("fixed preallocated pool size is " + fixedPool.length);
        }
        return new PreallocationSpec<>(factory, fixedPool, poolSize);
    }

    public boolean fixed() {
        return fixedPool != null;
    }

    public IntFunction<? extends T> factory() {
        return factory;
    }

    public T[] fixedPool() {
        return fixedPool == null ? null : fixedPool.clone();
    }

    public int requestedPoolSize() {
        return poolSize;
    }
}
