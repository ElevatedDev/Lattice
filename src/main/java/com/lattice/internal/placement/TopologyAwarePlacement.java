package com.lattice.internal.placement;

import com.lattice.internal.graph.CompiledGraph;
import com.lattice.internal.graph.NodeDefinition;
import com.lattice.placement.PinPolicy;
import com.staticgraph.runtime.nativeaccess.NativeCapabilities;
import com.staticgraph.runtime.nativeaccess.NativeTopology;
import com.staticgraph.runtime.nativeaccess.NativeTopologyException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TopologyAwarePlacement {

    public static final String ENABLED_PROPERTY = "lattice.placement.topologyAware.enabled";

    private TopologyAwarePlacement() {
    }

    public static Map<String, PinPolicy> plan(final CompiledGraph compiled, final List<String> workerOrder) {
        if (!Boolean.getBoolean(ENABLED_PROPERTY) || !NativeTopology.isLoaded()) {
            return Map.of();
        }

        final NativeCapabilities capabilities;
        try {
            capabilities = NativeTopology.capabilities();
        } catch (final NativeTopologyException ex) {
            return Map.of();
        }
        if (!capabilities.affinity() || !capabilities.numaQuery()) {
            return Map.of();
        }

        final List<CpuCandidate> candidates = candidates();
        if (candidates.isEmpty()) {
            return Map.of();
        }

        final List<String> unpinnedWorkers = unpinnedWorkers(compiled, workerOrder);
        if (unpinnedWorkers.isEmpty()) {
            return Map.of();
        }

        final List<CpuCandidate> localCandidates = bestNumaGroup(candidates, unpinnedWorkers.size());
        final Map<String, PinPolicy> pins = new LinkedHashMap<>();
        for (int i = 0; i < unpinnedWorkers.size(); i++) {
            pins.put(unpinnedWorkers.get(i), PinPolicy.cpu(localCandidates.get(i % localCandidates.size()).cpu()));
        }
        return Map.copyOf(pins);
    }

    private static List<String> unpinnedWorkers(final CompiledGraph compiled, final List<String> workerOrder) {
        final List<String> workers = new ArrayList<>(workerOrder.size());
        for (final String worker : workerOrder) {
            final NodeDefinition node = compiled.nodes().get(worker);
            if (node != null && node.spec().pinPolicy().kind() == PinPolicy.PinKind.NONE) {
                workers.add(worker);
            }
        }
        return workers;
    }

    private static List<CpuCandidate> candidates() {
        final int limit = cpuScanLimit();
        final List<CpuCandidate> candidates = new ArrayList<>(limit);
        for (int cpu = 0; cpu < limit; cpu++) {
            final boolean allowed;
            try {
                allowed = NativeTopology.isCpuAllowed(cpu);
            } catch (final RuntimeException ex) {
                continue;
            }
            if (!allowed) {
                continue;
            }
            try {
                candidates.add(new CpuCandidate(cpu, NativeTopology.numaNodeOfCpu(cpu)));
            } catch (final RuntimeException ex) {
                // CPUs without NUMA metadata are not useful for locality planning.
            }
        }
        candidates.sort(Comparator.comparingInt(CpuCandidate::numaNode).thenComparingInt(CpuCandidate::cpu));
        return candidates;
    }

    private static int cpuScanLimit() {
        final int max = positiveNativeCount(1024, NativeTopology::maxCpuCount);
        final int configured = positiveNativeCount(max, NativeTopology::configuredCpuCount);
        final int online = positiveNativeCount(configured, NativeTopology::onlineCpuCount);
        return Math.max(1, Math.min(max, Math.max(configured, online)));
    }

    private static int positiveNativeCount(final int fallback, final NativeCount count) {
        try {
            final int value = count.get();
            return value > 0 ? value : fallback;
        } catch (final RuntimeException ex) {
            return fallback;
        }
    }

    private static List<CpuCandidate> bestNumaGroup(final List<CpuCandidate> candidates, final int workerCount) {
        final Map<Integer, List<CpuCandidate>> byNuma = new LinkedHashMap<>();
        for (final CpuCandidate candidate : candidates) {
            byNuma.computeIfAbsent(candidate.numaNode(), ignored -> new ArrayList<>()).add(candidate);
        }

        List<CpuCandidate> best = List.of();
        for (final List<CpuCandidate> group : byNuma.values()) {
            if (group.size() >= workerCount) {
                return group;
            }
            if (group.size() > best.size()) {
                best = group;
            }
        }
        return best.isEmpty() ? candidates : best;
    }

    private record CpuCandidate(int cpu, int numaNode) {
    }

    @FunctionalInterface
    private interface NativeCount {
        int get();
    }
}
