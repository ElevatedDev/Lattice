package com.staticgraph.runtime.nativeaccess;

/**
 * Capability bits reported by the loaded native topology library.
 */
public record NativeCapabilities(
    boolean linux,
    boolean affinity,
    boolean currentCpu,
    boolean numaQuery,
    boolean localMemoryPolicy,
    boolean firstTouch
) {

    static final long LINUX = 1L;
    static final long AFFINITY = 1L << 1;
    static final long CURRENT_CPU = 1L << 2;
    static final long NUMA_QUERY = 1L << 3;
    static final long LOCAL_MEMORY_POLICY = 1L << 4;
    static final long FIRST_TOUCH = 1L << 5;

    static NativeCapabilities fromBits(final long bits) {
        return new NativeCapabilities(
            (bits & LINUX) != 0,
            (bits & AFFINITY) != 0,
            (bits & CURRENT_CPU) != 0,
            (bits & NUMA_QUERY) != 0,
            (bits & LOCAL_MEMORY_POLICY) != 0,
            (bits & FIRST_TOUCH) != 0
        );
    }
}
