package com.lattice.placement;

public final class MemoryMode {

    private final MemoryKind kind;
    private final long bytes;

    private MemoryMode(final MemoryKind kind, final long bytes) {
        this.kind = kind;
        this.bytes = bytes;
    }

    public static MemoryMode onHeapSlots() {
        return new MemoryMode(MemoryKind.ON_HEAP_SLOTS, 0L);
    }

    /**
     * Kept as an explicit unsupported mode for callers that still reference the
     * phase 1 placeholder. Use {@link #offHeapHandles()} for phase 3 metadata.
     */
    public static MemoryMode offHeapSlots() {
        return new MemoryMode(MemoryKind.OFF_HEAP_SLOTS, 0L);
    }

    public static MemoryMode offHeapHandles() {
        return new MemoryMode(MemoryKind.OFF_HEAP_HANDLES, 0L);
    }

    /**
     * Declares a minimum off-heap metadata budget for handle/sequence storage.
     * The graph compiler validates the budget against each edge capacity.
     */
    public static MemoryMode offHeapHandles(final long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must not be negative");
        }
        return new MemoryMode(MemoryKind.OFF_HEAP_HANDLES, bytes);
    }

    public MemoryKind kind() {
        return kind;
    }

    public long bytes() {
        return bytes;
    }

    public enum MemoryKind {
        ON_HEAP_SLOTS,
        OFF_HEAP_HANDLES,
        OFF_HEAP_SLOTS
    }
}
