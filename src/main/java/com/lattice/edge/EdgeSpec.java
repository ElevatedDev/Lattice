package com.lattice.edge;

import com.lattice.placement.MemoryMode;
import com.lattice.stage.BatchPolicy;
import com.lattice.wait.WaitSpec;
import java.util.Objects;

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

    public static EdgeSpec spscRing(final int capacity) {
        return new EdgeSpec(EdgeKind.SPSC_RING, capacity, WaitSpec.phasedDefault(), OverflowPolicy.block(), MemoryMode.onHeapSlots(), BatchPolicy.disabled());
    }

    public static EdgeSpec mpscRing(final int capacity) {
        return new EdgeSpec(EdgeKind.MPSC_RING, capacity, WaitSpec.phasedDefault(), OverflowPolicy.block(), MemoryMode.onHeapSlots(), BatchPolicy.disabled());
    }

    public EdgeSpec wait(final WaitSpec waitSpec) {
        return new EdgeSpec(kind, capacity, waitSpec, overflowPolicy, memoryMode, batchPolicy);
    }

    public EdgeSpec overflow(final OverflowPolicy overflowPolicy) {
        return new EdgeSpec(kind, capacity, waitSpec, overflowPolicy, memoryMode, batchPolicy);
    }

    public EdgeSpec memory(final MemoryMode memoryMode) {
        return new EdgeSpec(kind, capacity, waitSpec, overflowPolicy, memoryMode, batchPolicy);
    }

    public EdgeSpec batch(final BatchPolicy batchPolicy) {
        return new EdgeSpec(kind, capacity, waitSpec, overflowPolicy, memoryMode, batchPolicy);
    }

    public EdgeSpec withKind(final EdgeKind kind) {
        return new EdgeSpec(kind, capacity, waitSpec, overflowPolicy, memoryMode, batchPolicy);
    }

    public EdgeKind kind() {
        return kind;
    }

    public int capacity() {
        return capacity;
    }

    public WaitSpec waitSpec() {
        return waitSpec;
    }

    public OverflowPolicy overflowPolicy() {
        return overflowPolicy;
    }

    public MemoryMode memoryMode() {
        return memoryMode;
    }

    public BatchPolicy batchPolicy() {
        return batchPolicy;
    }

    public enum EdgeKind {
        SPSC_RING,
        MPSC_RING
    }
}
