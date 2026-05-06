package io.github.elevateddev.lattice.placement;

/**
 * Declares where edge slot or handle metadata is allocated.
 * <p>
 * Memory modes are immutable. The graph compiler validates whether a selected
 * mode is supported for the target edge implementation and capacity.
 */
public final class MemoryMode {

    private final MemoryKind kind;
    private final long bytes;

    private MemoryMode(final MemoryKind kind, final long bytes) {
        this.kind = kind;
        this.bytes = bytes;
    }

    /**
     * Uses normal Java heap references for edge slots.
     */
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

    /**
     * Uses off-heap metadata for handle and sequence storage with compiler
     * selected sizing.
     */
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

    /**
     * Returns the memory mode kind.
     */
    public MemoryKind kind() {
        return kind;
    }

    /**
     * Returns the requested off-heap metadata budget, or {@code 0} when the
     * compiler should choose the budget.
     */
    public long bytes() {
        return bytes;
    }

    /**
     * Memory allocation families.
     */
    public enum MemoryKind {
        /**
         * Java heap slots.
         */
        ON_HEAP_SLOTS,
        /**
         * Off-heap handle and sequence metadata.
         */
        OFF_HEAP_HANDLES,
        /**
         * Unsupported phase-1 placeholder retained for source compatibility.
         */
        OFF_HEAP_SLOTS
    }
}
