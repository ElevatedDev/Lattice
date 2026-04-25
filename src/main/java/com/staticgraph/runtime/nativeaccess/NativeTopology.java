package com.staticgraph.runtime.nativeaccess;

import java.util.BitSet;
import java.util.Objects;

/**
 * Java 21 wrapper for worker-bootstrap native operations: CPU affinity, NUMA
 * diagnostics, local allocation policy, and first-touch memory placement.
 */
public final class NativeTopology {
    private NativeTopology() {
    }

    public static boolean isLoaded() {
        return NativeTopologyNatives.loaded();
    }

    public static NativeCapabilities capabilities() {
        NativeTopologyNatives.ensureLoaded();
        return NativeCapabilities.fromBits(NativeTopologyNatives.nativeCapabilities0());
    }

    public static int maxCpuCount() {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.maxCpuCount0(), "max CPU count");
    }

    public static int configuredCpuCount() {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.configuredCpuCount0(), "configured CPU count");
    }

    public static int onlineCpuCount() {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.onlineCpuCount0(), "online CPU count");
    }

    public static int currentCpu() {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.currentCpu0(), "current CPU");
    }

    public static int currentNumaNode() {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.currentNumaNode0(), "current NUMA node");
    }

    public static int numaNodeOfCpu(final int cpu) {
        NativeTopologyNatives.ensureLoaded();
        return requireNonNegative(NativeTopologyNatives.numaNodeOfCpu0(cpu), "NUMA node of CPU " + cpu);
    }

    public static void pinCurrentThreadToCpu(final int cpu) {
        NativeTopologyNatives.ensureLoaded();
        requireZero(NativeTopologyNatives.pinCurrentThreadToCpu0(cpu), "pin current thread to CPU " + cpu);
    }

    public static void pinCurrentThreadToCpuSet(final BitSet cpus) {
        Objects.requireNonNull(cpus, "cpus");
        NativeTopologyNatives.ensureLoaded();

        final int maxCpu = maxCpuCount();
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
}
