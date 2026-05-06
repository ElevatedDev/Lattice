package io.github.elevateddev.lattice.edge;

import io.github.elevateddev.lattice.placement.MemoryMode;
import io.github.elevateddev.lattice.stage.BatchPolicy;
import io.github.elevateddev.lattice.wait.WaitSpec;
import java.util.Objects;

/**
 * Immutable configuration for an edge between graph nodes.
 * <p>
 * Edge specs describe queue shape, capacity, wait behavior, overflow handling,
 * memory placement, and optional batching metadata. Builder-style methods
 * return new instances and leave the original spec unchanged.
 */
public final class EdgeSpec {
    private final EdgeKind kind;
    private final int capacity;
    private final WaitSpec waitSpec;
    private final OverflowPolicy overflowPolicy;
    private final MemoryMode memoryMode;
    private final BatchPolicy batchPolicy;

    private EdgeSpec(
        final EdgeKind kind,
        final int capacity,
        final WaitSpec waitSpec,
        final OverflowPolicy overflowPolicy,
        final MemoryMode memoryMode,
        final BatchPolicy batchPolicy
    ) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.kind = Objects.requireNonNull(kind, "kind");
        this.capacity = capacity;
        this.waitSpec = Objects.requireNonNull(waitSpec, "waitSpec");
        this.overflowPolicy = Objects.requireNonNull(overflowPolicy, "overflowPolicy");
        this.memoryMode = Objects.requireNonNull(memoryMode, "memoryMode");
        this.batchPolicy = Objects.requireNonNull(batchPolicy, "batchPolicy");
    }

    /**
     * Creates a single-producer/single-consumer ring edge.
     * <p>
     * Use this only when the upstream node has one producer thread. For source
     * ingress, {@link io.github.elevateddev.lattice.graph.SourceMode#SINGLE_PRODUCER} is required.
     *
     * @param capacity ring capacity; graph build validates power-of-two limits
     * @return edge spec
     */
    public static EdgeSpec spscRing(final int capacity) {
        return new EdgeSpec(EdgeKind.SPSC_RING, capacity, WaitSpec.phasedDefault(), OverflowPolicy.block(), MemoryMode.onHeapSlots(), BatchPolicy.disabled());
    }

    /**
     * Creates a multi-producer/single-consumer ring edge.
     *
     * @param capacity ring capacity; graph build validates power-of-two limits
     * @return edge spec
     */
    public static EdgeSpec mpscRing(final int capacity) {
        return new EdgeSpec(EdgeKind.MPSC_RING, capacity, WaitSpec.phasedDefault(), OverflowPolicy.block(), MemoryMode.onHeapSlots(), BatchPolicy.disabled());
    }

    /**
     * Returns a copy with a different wait strategy.
     */
    public EdgeSpec wait(final WaitSpec waitSpec) {
        return new EdgeSpec(kind, capacity, waitSpec, overflowPolicy, memoryMode, batchPolicy);
    }

    /**
     * Returns a copy with a different overflow policy.
     */
    public EdgeSpec overflow(final OverflowPolicy overflowPolicy) {
        return new EdgeSpec(kind, capacity, waitSpec, overflowPolicy, memoryMode, batchPolicy);
    }

    /**
     * Returns a copy with a different memory mode.
     */
    public EdgeSpec memory(final MemoryMode memoryMode) {
        return new EdgeSpec(kind, capacity, waitSpec, overflowPolicy, memoryMode, batchPolicy);
    }

    /**
     * Returns a copy with a different batch policy.
     */
    public EdgeSpec batch(final BatchPolicy batchPolicy) {
        return new EdgeSpec(kind, capacity, waitSpec, overflowPolicy, memoryMode, batchPolicy);
    }

    /**
     * Returns a copy with the same options and a different edge kind.
     */
    public EdgeSpec withKind(final EdgeKind kind) {
        return new EdgeSpec(kind, capacity, waitSpec, overflowPolicy, memoryMode, batchPolicy);
    }

    /**
     * Returns a copy with the same options and a different capacity.
     *
     * @param capacity new edge capacity
     * @return edge spec
     */
    public EdgeSpec capacity(final int capacity) {
        return new EdgeSpec(kind, capacity, waitSpec, overflowPolicy, memoryMode, batchPolicy);
    }

    /**
     * Returns the selected edge implementation family.
     */
    public EdgeKind kind() {
        return kind;
    }

    /**
     * Returns the configured ring capacity.
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Returns the wait behavior used when producers or workers cannot make
     * immediate progress.
     */
    public WaitSpec waitSpec() {
        return waitSpec;
    }

    /**
     * Returns the overflow behavior used when the edge is full.
     */
    public OverflowPolicy overflowPolicy() {
        return overflowPolicy;
    }

    /**
     * Returns where edge slot or handle metadata is allocated.
     */
    public MemoryMode memoryMode() {
        return memoryMode;
    }

    /**
     * Returns the batch policy associated with this edge.
     */
    public BatchPolicy batchPolicy() {
        return batchPolicy;
    }

    /**
     * Edge implementation families supported by the public API.
     */
    public enum EdgeKind {
        /**
         * Single producer, single consumer ring.
         */
        SPSC_RING,

        /**
         * Multi producer, single consumer ring.
         */
        MPSC_RING
    }
}
