package io.github.elevateddev.lattice.placement;

import java.util.BitSet;
import java.util.Objects;

/**
 * Declares preferred CPU or NUMA placement for a stage worker.
 * <p>
 * Pin policies are best-effort unless native placement support is available.
 * Placement outcomes are visible through stage and graph metrics.
 */
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

    /**
     * Requests no explicit placement.
     */
    public static PinPolicy none() {
        return new PinPolicy(PinKind.NONE, -1, null, -1);
    }

    /**
     * Requests placement on one logical CPU.
     */
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

    /**
     * Requests placement on any CPU in the supplied set.
     * <p>
     * On Linux, the native backend applies the intersection of this set and
     * the worker thread's current allowed affinity. This keeps explicit
     * any-of placement compatible with cgroups, {@code taskset}, and service
     * manager CPU limits.
     */
    public static PinPolicy cpuSet(final int firstCpu, final int... additionalCpus) {
        final BitSet cpus = new BitSet();
        setCpu(cpus, firstCpu);
        for (final int cpu : Objects.requireNonNull(additionalCpus, "additionalCpus")) {
            setCpu(cpus, cpu);
        }
        return cpuSet(cpus);
    }

    /**
     * Requests placement on any CPU in the supplied set.
     * <p>
     * On Linux, the native backend applies the intersection of this set and
     * the worker thread's current allowed affinity. This keeps explicit
     * any-of placement compatible with cgroups, {@code taskset}, and service
     * manager CPU limits.
     */
    public static PinPolicy cpuSet(final BitSet cpus) {
        final BitSet copy = (BitSet) Objects.requireNonNull(cpus, "cpus").clone();
        if (copy.isEmpty()) {
            throw new IllegalArgumentException("cpus must not be empty");
        }
        return new PinPolicy(PinKind.CPU_SET, -1, copy, -1);
    }

    /**
     * Requests placement on a NUMA node.
     */
    public static PinPolicy numaNode(final int numaNode) {
        if (numaNode < 0) {
            throw new IllegalArgumentException("numaNode must not be negative");
        }
        return new PinPolicy(PinKind.NUMA_NODE, -1, null, numaNode);
    }

    /**
     * Requests that the worker inherit the process CPU set.
     */
    public static PinPolicy inheritCpuset() {
        return new PinPolicy(PinKind.INHERIT_CPUSET, -1, null, -1);
    }

    /**
     * Returns the placement policy kind.
     */
    public PinKind kind() {
        return kind;
    }

    /**
     * Returns the requested CPU id, or {@code -1} when not applicable.
     */
    public int cpuId() {
        return cpuId;
    }

    /**
     * Returns the requested core id for source-compatible core policies.
     */
    public int coreId() {
        return cpuId;
    }

    /**
     * Returns a defensive copy of the requested CPU set.
     */
    public BitSet cpuSet() {
        return (BitSet) cpuSet.clone();
    }

    /**
     * Returns the requested NUMA node, or {@code -1} when not applicable.
     */
    public int numaNode() {
        return numaNode;
    }

    /**
     * Returns whether this policy needs native placement support.
     */
    public boolean requiresNativePlacement() {
        return kind != PinKind.NONE;
    }

    private static void setCpu(final BitSet cpus, final int cpu) {
        if (cpu < 0) {
            throw new IllegalArgumentException("cpu must not be negative");
        }
        cpus.set(cpu);
    }

    /**
     * Placement request kinds.
     */
    public enum PinKind {
        /**
         * No explicit placement.
         */
        NONE,
        /**
         * One logical CPU.
         */
        CPU,
        /**
         * A set of logical CPUs.
         */
        CPU_SET,
        /**
         * A NUMA node.
         */
        NUMA_NODE,
        /**
         * The process CPU set.
         */
        INHERIT_CPUSET,
        /**
         * Source-compatible alias for CPU placement.
         */
        CORE
    }
}
