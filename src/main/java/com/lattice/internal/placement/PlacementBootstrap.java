package com.lattice.internal.placement;

import com.lattice.graph.GraphRuntimeException;
import com.lattice.internal.edge.MessageEdge;
import com.lattice.metrics.PlacementStatus;
import com.lattice.placement.PinPolicy;
import com.staticgraph.runtime.nativeaccess.NativeCapabilities;
import com.staticgraph.runtime.nativeaccess.NativeTopology;
import com.staticgraph.runtime.nativeaccess.NativeTopologyException;
import com.staticgraph.runtime.nativeaccess.NativeTopologyUnavailableException;
import java.util.BitSet;

public final class PlacementBootstrap {

    private PlacementBootstrap() {
    }

    public static PlacementResult bootstrap(
        final String stageName,
        final PinPolicy policy,
        final MessageEdge[] ownedEdges
    ) {
        bootstrapDelay();
        final StringBuilder message = new StringBuilder();
        final boolean placementRequested = policy.kind() != PinPolicy.PinKind.NONE;
        boolean pinApplied = false;
        boolean localPolicyApplied = false;
        PlacementStatus status = placementRequested ? PlacementStatus.DEGRADED : PlacementStatus.NOT_REQUESTED;
        int expectedCpu = expectedCpu(policy);
        int expectedNumaNode = expectedNumaNode(policy);

        final boolean nativeLoaded = NativeTopology.isLoaded();
        NativeCapabilities capabilities = null;
        if (nativeLoaded) {
            try {
                capabilities = NativeTopology.capabilities();
            } catch (final NativeTopologyException ex) {
                append(message, ex.getMessage());
                if (placementRequested) {
                    maybeStrict(stageName, ex);
                }
            }
        } else if (placementRequested) {
            status = PlacementStatus.UNAVAILABLE;
            final NativeTopologyUnavailableException unavailable = new NativeTopologyUnavailableException(
                nativeUnavailableMessage()
            );
            append(message, unavailable.getMessage());
            maybeStrict(stageName, unavailable);
        }

        if (nativeLoaded && capabilities != null) {
            if (placementRequested && !capabilities.linux()) {
                append(message, "native topology backend does not support this platform");
            }

            final PinAttempt pinAttempt = applyPin(stageName, policy, capabilities);
            pinApplied = pinAttempt.applied();
            expectedCpu = pinAttempt.expectedCpu(expectedCpu);
            expectedNumaNode = pinAttempt.expectedNumaNode(expectedNumaNode);
            append(message, pinAttempt.message());

            if ((placementRequested || pinApplied || policy.kind() == PinPolicy.PinKind.INHERIT_CPUSET)
                && capabilities.localMemoryPolicy()) {
                try {
                    NativeTopology.setLocalAllocationPolicy();
                    localPolicyApplied = true;
                } catch (final NativeTopologyException ex) {
                    append(message, ex.getMessage());
                    maybeStrict(stageName, ex);
                }
            }
        }

        final long firstTouchNanos = firstTouch(ownedEdges, stageName);
        if (firstTouchNanos > 0L) {
            append(message, "first-touch completed in " + firstTouchNanos + " ns");
        }

        final int observedCpu = observeCpu(nativeLoaded, capabilities, message);
        final int observedNumaNode = observeNumaNode(nativeLoaded, capabilities, message);
        final boolean affinityViolation = affinityViolation(policy, expectedCpu, observedCpu);
        final boolean numaViolation = expectedNumaNode >= 0
            && observedNumaNode >= 0
            && expectedNumaNode != observedNumaNode;
        final boolean localPolicyOk = !placementRequested
            || capabilities == null
            || !capabilities.localMemoryPolicy()
            || localPolicyApplied;
        final boolean inheritedCpuset = policy.kind() == PinPolicy.PinKind.INHERIT_CPUSET
            && capabilities != null
            && capabilities.linux();

        if (!placementRequested) {
            status = PlacementStatus.NOT_REQUESTED;
        } else if ((pinApplied || inheritedCpuset)
            && localPolicyOk
            && !affinityViolation
            && !numaViolation) {
            status = PlacementStatus.APPLIED;
        }

        if (affinityViolation) {
            append(message, "observed CPU does not match requested affinity");
        }
        if (numaViolation) {
            append(message, "observed NUMA node does not match requested node");
        }

        if (message.isEmpty()) {
            append(message, status.name().toLowerCase());
        }

        return new PlacementResult(
            status,
            message.toString(),
            expectedCpu,
            observedCpu,
            expectedNumaNode,
            observedNumaNode,
            stageName,
            affinityViolation,
            numaViolation
        );
    }

    private static PinAttempt applyPin(
        final String stageName,
        final PinPolicy policy,
        final NativeCapabilities capabilities
    ) {
        if (policy.kind() == PinPolicy.PinKind.NONE || policy.kind() == PinPolicy.PinKind.INHERIT_CPUSET) {
            return PinAttempt.notApplied("", -1, -1);
        }
        if (!capabilities.affinity()) {
            maybeStrict(stageName, new NativeTopologyException("native affinity is unavailable"));
            return PinAttempt.notApplied("native affinity is unavailable", -1, -1);
        }

        try {
            return switch (policy.kind()) {
                case CPU, CORE -> {
                    NativeTopology.pinCurrentThreadToCpu(policy.cpuId());
                    yield PinAttempt.applied("pinned to CPU " + policy.cpuId(), policy.cpuId(), -1);
                }
                case CPU_SET -> {
                    final BitSet cpus = policy.cpuSet();
                    NativeTopology.pinCurrentThreadToCpuSet(cpus);
                    yield PinAttempt.applied("pinned to CPU set " + cpus, -1, -1);
                }
                case NUMA_NODE -> pinToNumaNode(policy.numaNode());
                default -> PinAttempt.notApplied("", -1, -1);
            };
        } catch (final NativeTopologyException | IllegalArgumentException ex) {
            maybeStrict(stageName, ex);
            return PinAttempt.notApplied(ex.getMessage(), -1, policy.numaNode());
        }
    }

    private static PinAttempt pinToNumaNode(final int numaNode) {
        final int maxCpu = NativeTopology.maxCpuCount();
        for (int cpu = 0; cpu < maxCpu; cpu++) {
            try {
                if (NativeTopology.numaNodeOfCpu(cpu) != numaNode) {
                    continue;
                }
                NativeTopology.pinCurrentThreadToCpu(cpu);
                return PinAttempt.applied("pinned to CPU " + cpu + " on NUMA node " + numaNode, cpu, numaNode);
            } catch (final NativeTopologyException ignored) {
                // Try the next CPU in the requested node; cpusets can reject a subset.
            }
        }
        throw new NativeTopologyException("no usable CPU found for NUMA node " + numaNode);
    }

    private static long firstTouch(final MessageEdge[] ownedEdges, final String ownerName) {
        long total = 0L;
        for (int i = 0; i < ownedEdges.length; i++) {
            final long started = System.nanoTime();
            ownedEdges[i].firstTouch(ownerName);
            total += System.nanoTime() - started;
        }
        return total;
    }

    private static int observeCpu(
        final boolean nativeLoaded,
        final NativeCapabilities capabilities,
        final StringBuilder message
    ) {
        if (!nativeLoaded || capabilities == null || !capabilities.currentCpu()) {
            return -1;
        }
        try {
            return NativeTopology.currentCpu();
        } catch (final NativeTopologyException ex) {
            append(message, ex.getMessage());
            return -1;
        }
    }

    private static int observeNumaNode(
        final boolean nativeLoaded,
        final NativeCapabilities capabilities,
        final StringBuilder message
    ) {
        if (!nativeLoaded || capabilities == null || !capabilities.numaQuery()) {
            return -1;
        }
        try {
            return NativeTopology.currentNumaNode();
        } catch (final NativeTopologyException ex) {
            append(message, ex.getMessage());
            return -1;
        }
    }

    private static boolean affinityViolation(final PinPolicy policy, final int expectedCpu, final int observedCpu) {
        if (observedCpu < 0) {
            return false;
        }
        if (expectedCpu >= 0) {
            return expectedCpu != observedCpu;
        }
        if (policy.kind() == PinPolicy.PinKind.CPU_SET) {
            return !policy.cpuSet().get(observedCpu);
        }
        return false;
    }

    private static int expectedCpu(final PinPolicy policy) {
        return switch (policy.kind()) {
            case CPU, CORE -> policy.cpuId();
            default -> -1;
        };
    }

    private static int expectedNumaNode(final PinPolicy policy) {
        return policy.kind() == PinPolicy.PinKind.NUMA_NODE ? policy.numaNode() : -1;
    }

    private static void append(final StringBuilder message, final String text) {
        if (text == null || text.isBlank()) {
            return;
        }
        if (!message.isEmpty()) {
            message.append("; ");
        }
        message.append(text);
    }

    private static String nativeUnavailableMessage() {
        final String loadFailure = NativeTopology.loadFailureMessage();
        if (loadFailure.isBlank()) {
            return "native topology library is not loaded";
        }
        return "native topology library is not loaded: " + loadFailure;
    }

    private static void maybeStrict(final String stageName, final Throwable failure) {
        if (strict()) {
            throw new GraphRuntimeException("worker placement failed for " + stageName, failure);
        }
    }

    private static boolean strict() {
        return Boolean.getBoolean("lattice.placement.strict");
    }

    private static void bootstrapDelay() {
        final long delayMillis = Long.getLong("lattice.placement.bootstrapDelayMillis", 0L);
        if (delayMillis <= 0L) {
            return;
        }
        try {
            Thread.sleep(delayMillis);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private record PinAttempt(boolean applied, String message, int expectedCpu, int expectedNumaNode) {
        static PinAttempt applied(final String message, final int expectedCpu, final int expectedNumaNode) {
            return new PinAttempt(true, message, expectedCpu, expectedNumaNode);
        }

        static PinAttempt notApplied(final String message, final int expectedCpu, final int expectedNumaNode) {
            return new PinAttempt(false, message, expectedCpu, expectedNumaNode);
        }

        int expectedCpu(final int fallback) {
            return expectedCpu >= 0 ? expectedCpu : fallback;
        }

        int expectedNumaNode(final int fallback) {
            return expectedNumaNode >= 0 ? expectedNumaNode : fallback;
        }
    }
}
