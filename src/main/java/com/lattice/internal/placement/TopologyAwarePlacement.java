package com.lattice.internal.placement;

import com.lattice.internal.graph.CompiledGraph;
import com.lattice.internal.graph.NodeDefinition;
import com.lattice.placement.PinPolicy;
import com.staticgraph.runtime.nativeaccess.NativeCapabilities;
import com.staticgraph.runtime.nativeaccess.NativeTopology;
import com.staticgraph.runtime.nativeaccess.NativeTopologySnapshot;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TopologyAwarePlacement {

    public static final String ENABLED_PROPERTY = "lattice.placement.topologyAware.enabled";

    private TopologyAwarePlacement() {
    }

    public static Map<String, PinPolicy> plan(final CompiledGraph compiled, final List<String> workerOrder) {
        if (!Boolean.getBoolean(ENABLED_PROPERTY)) {
            return Map.of();
        }

        final NativeTopologySnapshot snapshot = NativeTopology.snapshot();
        if (!snapshot.loaded() || !snapshot.hasCapabilities()) {
            return Map.of();
        }
        final NativeCapabilities capabilities = snapshot.capabilities();
        if (!capabilities.affinity() || !capabilities.numaQuery()) {
            return Map.of();
        }

        final List<CpuCandidate> candidates = candidates(snapshot.cpuTopology());
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

    private static List<CpuCandidate> candidates(final NativeTopologySnapshot.CpuTopology topology) {
        if (!topology.hasNumaMetadata()) {
            return List.of();
        }

        final BitSet allowedCpus = topology.allowedCpus();
        final List<CpuCandidate> candidates = new ArrayList<>(allowedCpus.cardinality());
        for (int cpu = allowedCpus.nextSetBit(0); cpu >= 0; cpu = allowedCpus.nextSetBit(cpu + 1)) {
            final int numaNode = topology.numaNodeOfCpu(cpu);
            if (numaNode < 0) {
                continue;
            }
            candidates.add(new CpuCandidate(cpu, numaNode));
        }
        candidates.sort(Comparator.comparingInt(CpuCandidate::numaNode).thenComparingInt(CpuCandidate::cpu));
        return candidates;
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
}
