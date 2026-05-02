package com.lattice.nativeaccess;

import java.util.BitSet;
import java.util.Objects;

/**
 * Java 21 wrapper for worker-bootstrap native operations: CPU affinity, NUMA
 * diagnostics, local allocation policy, and first-touch memory placement.
 */
public final class NativeTopology {
    private NativeTopology() {
    }

    /**
     * Returns whether the optional native topology library was loaded.
     */
    public static boolean isLoaded() {
        return NativeTopologyNatives.loaded();
    }

    /**
     * Returns the native library load failure message, or an empty string when
     * the library loaded.
     */
    public static String loadFailureMessage() {
        return NativeTopologyNatives.loadFailureMessage();
    }

    /**
     * Returns the process-wide native topology snapshot.
     */
    public static NativeTopologySnapshot snapshot() {
        return SnapshotHolder.SNAPSHOT;
    }

    /**
     * Returns capabilities reported by the loaded native library.
     *
     * @throws NativeTopologyUnavailableException if the library is unavailable
     */
    public static NativeCapabilities capabilities() {
        NativeTopologyNatives.ensureLoaded();
        return NativeCapabilities.fromBits(NativeTopologyNatives.nativeCapabilities0());
    }

    /**
     * Returns the maximum CPU id scan bound reported by the native library.
     */
    public static int maxCpuCount() {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.maxCpuCount0(), "max CPU count");
    }

    /**
     * Returns the configured CPU count for the process host.
     */
    public static int configuredCpuCount() {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.configuredCpuCount0(), "configured CPU count");
    }

    /**
     * Returns the online CPU count for the process host.
     */
    public static int onlineCpuCount() {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.onlineCpuCount0(), "online CPU count");
    }

    /**
     * Returns the CPU currently running this thread.
     */
    public static int currentCpu() {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.currentCpu0(), "current CPU");
    }

    /**
     * Returns the NUMA node currently running this thread.
     */
    public static int currentNumaNode() {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.currentNumaNode0(), "current NUMA node");
    }

    /**
     * Returns the NUMA node associated with a CPU id.
     */
    public static int numaNodeOfCpu(final int cpu) {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.numaNodeOfCpu0(cpu), "NUMA node of CPU " + cpu);
    }

    /**
     * Returns whether the process affinity mask allows the supplied CPU id.
     */
    public static boolean isCpuAllowed(final int cpu) {
        NativeTopologyNatives.ensureLoaded();
        if (cpu < 0) {
            throw new IllegalArgumentException("cpu must not be negative");
        }
        final int rc = NativeTopologyNatives.isCpuAllowed0(cpu);
        if (rc == 0 || rc == 1) {
            return rc == 1;
        }
        throw failure("query allowed CPU " + cpu, rc);
    }

    /**
     * Pins the current Java thread to a CPU id.
     */
    public static void pinCurrentThreadToCpu(final int cpu) {
        NativeTopologyNatives.ensureLoaded();
        requireZero(NativeTopologyNatives.pinCurrentThreadToCpu0(cpu), "pin current thread to CPU " + cpu);
    }

    /**
     * Pins the current Java thread to any CPU in a NUMA node.
     *
     * @return selected CPU id
     */
    public static int pinCurrentThreadToNumaNode(final int numaNode) {
        NativeTopologyNatives.ensureLoaded();
        if (numaNode < 0) {
            throw new IllegalArgumentException("NUMA node must not be negative");
        }
        try {
            return requireNonNegative(
                NativeTopologyNatives.pinCurrentThreadToNumaNode0(numaNode),
                "pin current thread to NUMA node " + numaNode
            );
        } catch (final UnsatisfiedLinkError error) {
            throw new NativeTopologyUnavailableException(
                "pin current thread to NUMA node " + numaNode
                    + " is not supported by the loaded native topology library",
                error
            );
        }
    }

    /**
     * Pins the current Java thread to a CPU set using the native CPU scan bound.
     */
    public static void pinCurrentThreadToCpuSet(final BitSet cpus) {
        pinCurrentThreadToCpuSet(cpus, maxCpuCount());
    }

    /**
     * Pins the current Java thread to a CPU set.
     * <p>
     * Linux applies CPU sets as any-of placement within the current allowed
     * affinity mask. A requested set with no allowed CPUs fails.
     *
     * @param cpus non-empty CPU set
     * @param maxCpu exclusive upper bound accepted by the native mask
     */
    public static void pinCurrentThreadToCpuSet(final BitSet cpus, final int maxCpu) {
        Objects.requireNonNull(cpus, "cpus");
        NativeTopologyNatives.ensureLoaded();

        if (maxCpu <= 0) {
            throw new IllegalArgumentException("maxCpu must be positive");
        }
        if (cpus.isEmpty()) {
            throw new IllegalArgumentException("CPU set must not be empty");
        }
        if (cpus.length() > maxCpu) {
            throw new IllegalArgumentException("CPU set exceeds native limit " + maxCpu + ": highest CPU "
                + (cpus.length() - 1));
        }

        final long[] words = cpus.toLongArray();
        if (words.length > NativeTopologyNatives.CPU_MASK_WORDS) {
            throw new IllegalArgumentException("CPU set exceeds native mask word count "
                + NativeTopologyNatives.CPU_MASK_WORDS);
        }

        final long[] mask = new long[NativeTopologyNatives.CPU_MASK_WORDS];
        System.arraycopy(words, 0, mask, 0, words.length);

        requireZero(NativeTopologyNatives.pinCurrentThreadToCpuMask0(
            mask[0],
            mask[1],
            mask[2],
            mask[3],
            mask[4],
            mask[5],
            mask[6],
            mask[7],
            mask[8],
            mask[9],
            mask[10],
            mask[11],
            mask[12],
            mask[13],
            mask[14],
            mask[15]
        ), "pin current thread to CPU set " + cpus);
    }

    /**
     * Requests local NUMA allocation policy for future allocations on the
     * current thread.
     */
    public static void setLocalAllocationPolicy() {
        NativeTopologyNatives.ensureLoaded();
        requireZero(NativeTopologyNatives.setLocalAllocationPolicy0(), "set local NUMA allocation policy");
    }

    /**
     * Touches one byte in each page covered by {@code [address, address + bytes)}
     * from the current thread. Pin the thread and set local allocation policy
     * before calling this method.
     */
    public static void firstTouchMemory(final long address, final long bytes) {
        NativeTopologyNatives.ensureLoaded();
        if (address <= 0) {
            throw new IllegalArgumentException("address must be positive");
        }
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes must be non-negative");
        }
        requireZero(NativeTopologyNatives.firstTouchMemory0(address, bytes), "first-touch memory");
    }

    private static int requireNonNegative(final int rc, final String operation) {
        if (rc >= 0) {
            return rc;
        }
        throw failure(operation, rc);
    }

    private static void requireZero(final int rc, final String operation) {
        if (rc != 0) {
            throw failure(operation, rc);
        }
    }

    private static RuntimeException failure(final String operation, final int rc) {
        final int errno = -rc;
        if (errno == 38) {
            return new NativeTopologyUnavailableException(operation + " is not supported on this platform");
        }
        return new NativeTopologyException(operation + " failed with errno " + errno + " (" + errnoName(errno) + ")");
    }

    private static String errnoName(final int errno) {
        return switch (errno) {
            case 1 -> "EPERM";
            case 2 -> "ENOENT";
            case 3 -> "ESRCH";
            case 12 -> "ENOMEM";
            case 14 -> "EFAULT";
            case 16 -> "EBUSY";
            case 19 -> "ENODEV";
            case 22 -> "EINVAL";
            case 34 -> "ERANGE";
            case 38 -> "ENOSYS";
            default -> "errno";
        };
    }

    private static final class SnapshotHolder {
        private static final NativeTopologySnapshot SNAPSHOT = NativeTopologySnapshot.captureSystem();
    }
}
