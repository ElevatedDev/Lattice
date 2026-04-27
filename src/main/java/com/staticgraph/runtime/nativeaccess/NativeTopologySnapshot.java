package com.staticgraph.runtime.nativeaccess;

import java.util.BitSet;

/**
 * Immutable snapshot of the system's native topology captured once at startup.
 * Falls back to safe defaults when the native library is unavailable.
 */
public final class NativeTopologySnapshot {

    private final boolean loaded;
    private final boolean hasCapabilities;
    private final NativeCapabilities capabilities;
    private final String failureMessage;
    private final Throwable failure;
    private final CpuTopology cpuTopology;
    private final int maxCpuCount;

    private NativeTopologySnapshot(
        final boolean loaded,
        final boolean hasCapabilities,
        final NativeCapabilities capabilities,
        final String failureMessage,
        final Throwable failure,
        final CpuTopology cpuTopology,
        final int maxCpuCount
    ) {
        this.loaded = loaded;
        this.hasCapabilities = hasCapabilities;
        this.capabilities = capabilities;
        this.failureMessage = failureMessage == null ? "" : failureMessage;
        this.failure = failure;
        this.cpuTopology = cpuTopology;
        this.maxCpuCount = maxCpuCount;
    }

    /**
     * Captures a snapshot of the current system topology. Safe to call even when
     * the native library failed to load — returns a degraded snapshot.
     */
    public static NativeTopologySnapshot captureSystem() {
        if (!NativeTopologyNatives.loaded()) {
            return new NativeTopologySnapshot(
                false, false, null,
                NativeTopologyNatives.loadFailureMessage(),
                null,
                CpuTopology.EMPTY,
                0
            );
        }

        NativeCapabilities caps = null;
        String failureMsg = "";
        Throwable failureCause = null;
        CpuTopology topology = CpuTopology.EMPTY;
        int maxCpus = 0;

        try {
            caps = NativeCapabilities.fromBits(NativeTopologyNatives.nativeCapabilities0());
        } catch (final Throwable t) {
            failureMsg = t.getMessage();
            failureCause = t;
        }

        try {
            maxCpus = NativeTopologyNatives.maxCpuCount0();
        } catch (final Throwable ignored) {
        }

        if (caps != null) {
            topology = buildCpuTopology(caps);
        }

        return new NativeTopologySnapshot(true, caps != null, caps, failureMsg, failureCause, topology, maxCpus);
    }

    public boolean loaded() {
        return loaded;
    }

    public boolean hasCapabilities() {
        return hasCapabilities;
    }

    public NativeCapabilities capabilities() {
        if (capabilities == null) {
            throw new NativeTopologyUnavailableException("capabilities not available");
        }
        return capabilities;
    }

    public String failureMessage() {
        return failureMessage;
    }

    public CpuTopology cpuTopology() {
        return cpuTopology;
    }

    public int maxCpuCount() {
        return maxCpuCount;
    }

    public Throwable failure() {
        return failure;
    }

    public int cachedNumaNodeOfCpu(final int cpu) {
        return cpuTopology.numaNodeOfCpu(cpu);
    }

    private static CpuTopology buildCpuTopology(final NativeCapabilities caps) {
        try {
            final int maxCpus = NativeTopologyNatives.maxCpuCount0();
            if (maxCpus <= 0) {
                return CpuTopology.EMPTY;
            }

            final BitSet allowed = new BitSet(maxCpus);
            for (int cpu = 0; cpu < maxCpus; cpu++) {
                final int rc = NativeTopologyNatives.isCpuAllowed0(cpu);
                if (rc == 1) {
                    allowed.set(cpu);
                }
            }

            final boolean hasNuma = caps.numaQuery();
            final int[] numaNodes;
            if (hasNuma) {
                numaNodes = new int[maxCpus];
                for (int cpu = 0; cpu < maxCpus; cpu++) {
                    try {
                        numaNodes[cpu] = NativeTopologyNatives.numaNodeOfCpu0(cpu);
                    } catch (final Throwable t) {
                        numaNodes[cpu] = -1;
                    }
                }
            } else {
                numaNodes = null;
            }

            return new CpuTopology(allowed, hasNuma, numaNodes);
        } catch (final Throwable t) {
            return CpuTopology.EMPTY;
        }
    }

    /**
     * CPU topology snapshot: allowed CPUs and per-CPU NUMA node mapping.
     */
    public static final class CpuTopology {

        static final CpuTopology EMPTY = new CpuTopology(new BitSet(), false, null);

        private final BitSet allowedCpus;
        private final boolean hasNumaMetadata;
        private final int[] numaNodes; // indexed by CPU id, or null

        CpuTopology(final BitSet allowedCpus, final boolean hasNumaMetadata, final int[] numaNodes) {
            this.allowedCpus = allowedCpus;
            this.hasNumaMetadata = hasNumaMetadata;
            this.numaNodes = numaNodes;
        }

        public boolean hasNumaMetadata() {
            return hasNumaMetadata;
        }

        public BitSet allowedCpus() {
            return (BitSet) allowedCpus.clone();
        }

        public int numaNodeOfCpu(final int cpu) {
            if (numaNodes == null || cpu < 0 || cpu >= numaNodes.length) {
                return -1;
            }
            return numaNodes[cpu];
        }
    }
}

