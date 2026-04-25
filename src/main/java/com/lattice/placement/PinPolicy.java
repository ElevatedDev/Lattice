package com.lattice.placement;

import java.util.BitSet;
import java.util.Objects;

public final class PinPolicy {

    private final PinKind kind;
    private final int cpuId;
    private final BitSet cpuSet;
    private final int numaNode;

    private PinPolicy(final PinKind kind, final int cpuId, final BitSet cpuSet, final int numaNode) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.cpuId = cpuId;
        this.cpuSet = cpuSet == null ? new BitSet() : (BitSet) cpuSet.clone();
        this.numaNode = numaNode;
    }

    public static PinPolicy none() {
        return new PinPolicy(PinKind.NONE, -1, null, -1);
    }

    public static PinPolicy cpu(final int cpuId) {
        if (cpuId < 0) {
            throw new IllegalArgumentException("cpuId must not be negative");
        }
        return new PinPolicy(PinKind.CPU, cpuId, null, -1);
    }

    /**
     * Alias kept for source compatibility with the phase 1 API.
     */
    public static PinPolicy core(final int coreId) {
        if (coreId < 0) {
            throw new IllegalArgumentException("coreId must not be negative");
        }
        return new PinPolicy(PinKind.CORE, coreId, null, -1);
    }

    public static PinPolicy cpuSet(final int firstCpu, final int... additionalCpus) {
        final BitSet cpus = new BitSet();
        setCpu(cpus, firstCpu);
        for (final int cpu : Objects.requireNonNull(additionalCpus, "additionalCpus")) {
            setCpu(cpus, cpu);
        }
        return cpuSet(cpus);
    }

    public static PinPolicy cpuSet(final BitSet cpus) {
        final BitSet copy = (BitSet) Objects.requireNonNull(cpus, "cpus").clone();
        if (copy.isEmpty()) {
            throw new IllegalArgumentException("cpus must not be empty");
        }
        return new PinPolicy(PinKind.CPU_SET, -1, copy, -1);
    }

    public static PinPolicy numaNode(final int numaNode) {
        if (numaNode < 0) {
            throw new IllegalArgumentException("numaNode must not be negative");
        }
        return new PinPolicy(PinKind.NUMA_NODE, -1, null, numaNode);
    }

    public static PinPolicy inheritCpuset() {
        return new PinPolicy(PinKind.INHERIT_CPUSET, -1, null, -1);
    }

    public PinKind kind() {
        return kind;
    }

    public int cpuId() {
        return cpuId;
    }

    public int coreId() {
        return cpuId;
    }

    public BitSet cpuSet() {
        return (BitSet) cpuSet.clone();
    }

    public int numaNode() {
        return numaNode;
    }

    public boolean requiresNativePlacement() {
        return kind != PinKind.NONE;
    }

    private static void setCpu(final BitSet cpus, final int cpu) {
        if (cpu < 0) {
            throw new IllegalArgumentException("cpu must not be negative");
        }
        cpus.set(cpu);
    }

    public enum PinKind {
        NONE,
        CPU,
        CPU_SET,
        NUMA_NODE,
        INHERIT_CPUSET,
        CORE
    }
}
