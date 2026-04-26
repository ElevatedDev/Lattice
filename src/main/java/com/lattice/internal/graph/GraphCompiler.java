package com.lattice.internal.graph;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.GraphBuildException;
import com.lattice.graph.GraphPlan;
import com.lattice.graph.SourceMode;
import com.lattice.placement.MemoryMode;
import com.lattice.placement.PinPolicy;
import com.lattice.routing.DispatchSpec;
import com.lattice.routing.BroadcastSpec;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.StageExceptionHandler;
import com.lattice.stage.StageSpec;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

final class GraphCompiler {
    private static final int NATIVE_CPU_LIMIT = 1024;

    private final String graphName;
    private final List<NodeDefinition> declaredNodes;
    private final List<StaticGraphBuilder.PendingEdge> declaredEdges;
    private final StageExceptionHandler exceptionHandler;

    GraphCompiler(
        final String graphName,
        final List<NodeDefinition> declaredNodes,
        final List<StaticGraphBuilder.PendingEdge> declaredEdges,
        final StageExceptionHandler exceptionHandler
    ) {
        this.graphName = graphName;
        this.declaredNodes = List.copyOf(declaredNodes);
        this.declaredEdges = List.copyOf(declaredEdges);
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler, "exceptionHandler");
    }

    CompiledGraph compile() {
        final Map<String, NodeDefinition> nodes = registerNodes();
        final Set<String> redirectKeys = redirectKeys();
        final List<EdgeDefinition> edges = registerEdges(nodes, redirectKeys);

        final Map<String, List<EdgeDefinition>> incomingByTarget = groupByTarget(edges);
        final Map<String, List<EdgeDefinition>> outgoingBySource = groupBySource(edges);
        final Map<String, List<EdgeDefinition>> normalOutgoingBySource = normalOutgoingBySource(outgoingBySource);
        final Map<String, Map<String, EdgeDefinition>> redirectBySourceAndTarget = redirectBySourceAndTarget(edges);

        validateShape(nodes, incomingByTarget, normalOutgoingBySource);
        validateRedirects(nodes, edges, redirectBySourceAndTarget);
        validatePreallocatedSources(nodes, outgoingBySource, normalOutgoingBySource);

        final List<String> topologicalOrder = topologicalOrder(nodes, edges);
        final List<String> workerOrder = topologicalOrder.stream()
            .filter(name -> nodes.get(name).kind() != GraphPlan.NodeKind.SOURCE)
            .toList();

        final GraphPlan plan = new GraphPlan(
            graphName,
            buildPlanNodes(nodes),
            buildPlanEdges(nodes, edges),
            workerOrder,
            buildPlanPlacements(nodes, workerOrder)
        );

        return new CompiledGraph(
            plan,
            Collections.unmodifiableMap(nodes),
            List.copyOf(edges),
            incomingByTarget,
            outgoingBySource,
            normalOutgoingBySource,
            redirectBySourceAndTarget,
            List.copyOf(workerOrder),
            exceptionHandler
        );
    }

    private Map<String, NodeDefinition> registerNodes() {
        if (declaredNodes.isEmpty()) {
            throw new GraphBuildException("graph must declare at least one node");
        }

        final Map<String, NodeDefinition> nodes = new LinkedHashMap<>();
        boolean hasSource = false;
        boolean hasSink = false;

        for (final NodeDefinition node : declaredNodes) {
            if (nodes.putIfAbsent(node.name(), node) != null) {
                throw new GraphBuildException("duplicate node name: " + node.name());
            }

            validateStageSpec(node);
            hasSource |= node.kind() == GraphPlan.NodeKind.SOURCE;
            hasSink |= node.kind() == GraphPlan.NodeKind.SINK;
        }
        if (!hasSource) {
            throw new GraphBuildException("graph must declare at least one source");
        }
        if (!hasSink) {
            throw new GraphBuildException("graph must declare at least one sink");
        }

        return nodes;
    }

    private Set<String> redirectKeys() {
        final Set<String> keys = new HashSet<>();
        for (final StaticGraphBuilder.PendingEdge pending : declaredEdges) {
            final String redirectTarget = pending.spec().overflowPolicy().redirectTarget();
            if (redirectTarget != null) {
                keys.add(pending.from() + "->" + redirectTarget);
            }
        }
        return keys;
    }

    private List<EdgeDefinition> registerEdges(
        final Map<String, NodeDefinition> nodes,
        final Set<String> redirectKeys
    ) {
        if (declaredEdges.isEmpty()) {
            throw new GraphBuildException("graph must declare at least one edge");
        }

        final Map<String, EdgeDefinition> unique = new LinkedHashMap<>();
        final Map<String, Integer> normalBranchIndex = new HashMap<>();
        for (final StaticGraphBuilder.PendingEdge pending : declaredEdges) {
            final NodeDefinition from = nodes.get(pending.from());
            final NodeDefinition to = nodes.get(pending.to());

            if (from == null) {
                throw new GraphBuildException("edge references unknown source node: " + pending.from());
            }
            if (to == null) {
                throw new GraphBuildException("edge references unknown target node: " + pending.to());
            }
            validateEdgeEndpoints(from, to);
            validateEdgeSpec(from, pending.spec());
            validateTypeCompatibility(from, to);
            final EdgeSpec effectiveSpec = effectiveEdgeSpec(from, pending.spec());

            final String key = from.name() + "->" + to.name();
            final boolean redirectOnly = redirectKeys.contains(key);
            final int branchIndex = redirectOnly ? -1 : normalBranchIndex.merge(from.name(), 1, Integer::sum) - 1;
            final EdgeDefinition edge = new EdgeDefinition(
                from.name(),
                to.name(),
                from.outputType(),
                effectiveSpec,
                pending.declarationOrder(),
                branchIndex,
                redirectOnly
            );
            if (unique.putIfAbsent(edge.key(), edge) != null) {
                throw new GraphBuildException("duplicate edge: " + edge.key());
            }
        }
        return new ArrayList<>(unique.values());
    }

    private void validateStageSpec(final NodeDefinition node) {
        final StageSpec spec = node.spec();
        if (spec == null) {
            return;
        }
        if (spec.execution() != StageSpec.StageExecution.SINGLE_THREADED) {
            throw new GraphBuildException("unsupported stage execution for " + node.name() + ": " + spec.execution());
        }
        validatePinPolicy(node.name(), spec.pinPolicy());
        if (node.batchLogic() == null && node.kind() == GraphPlan.NodeKind.STAGE
            && spec.batchPolicy().kind() != BatchPolicy.BatchKind.DISABLED) {
            throw new GraphBuildException("single-message stage cannot use a stage batch policy: " + node.name());
        }
    }

    private void validateEdgeEndpoints(final NodeDefinition from, final NodeDefinition to) {
        if (from.name().equals(to.name())) {
            throw new GraphBuildException("self edges are not supported: " + from.name());
        }
        if (from.kind() == GraphPlan.NodeKind.SINK) {
            throw new GraphBuildException("sink node cannot have outgoing edge: " + from.name());
        }
        if (to.kind() == GraphPlan.NodeKind.SOURCE) {
            throw new GraphBuildException("source node cannot have incoming edge: " + to.name());
        }
    }

    private void validateEdgeSpec(final NodeDefinition from, final EdgeSpec spec) {
        if (Integer.bitCount(spec.capacity()) != 1) {
            throw new GraphBuildException("edge capacity must be a power of two: " + spec.capacity());
        }
        if (spec.kind() == EdgeSpec.EdgeKind.MPSC_RING && spec.capacity() < 2) {
            throw new GraphBuildException("MPSC edge capacity must be at least 2");
        }
        if (spec.memoryMode().kind() == MemoryMode.MemoryKind.OFF_HEAP_SLOTS) {
            throw new GraphBuildException("off-heap payload slots are not implemented; use slab handles for payloads");
        }
        if (spec.memoryMode().kind() == MemoryMode.MemoryKind.OFF_HEAP_HANDLES && spec.memoryMode().bytes() > 0L) {
            final long requiredBytes = (long) spec.capacity() * Long.BYTES;
            if (spec.memoryMode().bytes() < requiredBytes) {
                throw new GraphBuildException("off-heap handle metadata budget for edge from " + from.name()
                    + " must be at least " + requiredBytes + " bytes");
            }
        }
        if (spec.overflowPolicy().kind() == OverflowPolicy.OverflowKind.COALESCE
            && spec.overflowPolicy().coalescingKey() == null) {
            throw new GraphBuildException("coalescing overflow requires a key extractor");
        }
        if (from.kind() == GraphPlan.NodeKind.SOURCE
            && from.sourceMode() != SourceMode.SINGLE_PRODUCER
            && spec.kind() != EdgeSpec.EdgeKind.MPSC_RING) {
            throw new GraphBuildException("multi-producer source ingress edges must use EdgeSpec.mpscRing(...)");
        }
        if (from.kind() != GraphPlan.NodeKind.SOURCE && spec.kind() != EdgeSpec.EdgeKind.SPSC_RING) {
            throw new GraphBuildException("worker-owned edges must use EdgeSpec.spscRing(...)");
        }
    }

    private static EdgeSpec effectiveEdgeSpec(final NodeDefinition from, final EdgeSpec spec) {
        if (from.kind() == GraphPlan.NodeKind.SOURCE && from.sourceMode() == SourceMode.SINGLE_PRODUCER) {
            return spec.withKind(EdgeSpec.EdgeKind.SPSC_RING);
        }
        return spec;
    }

    private void validatePinPolicy(final String stageName, final PinPolicy pinPolicy) {
        switch (pinPolicy.kind()) {
            case CPU, CORE -> validateCpu(stageName, pinPolicy.cpuId());
            case CPU_SET -> {
                if (pinPolicy.cpuSet().length() > NATIVE_CPU_LIMIT) {
                    throw new GraphBuildException("CPU set for stage " + stageName
                        + " exceeds native CPU limit " + NATIVE_CPU_LIMIT);
                }
            }
            case NUMA_NODE -> {
                if (pinPolicy.numaNode() > NATIVE_CPU_LIMIT) {
                    throw new GraphBuildException("NUMA node for stage " + stageName + " is implausibly high: "
                        + pinPolicy.numaNode());
                }
            }
            case NONE, INHERIT_CPUSET -> {
            }
            default -> throw new GraphBuildException("unsupported pin policy for stage " + stageName + ": "
                + pinPolicy.kind());
        }
    }

    private static void validateCpu(final String stageName, final int cpuId) {
        if (cpuId >= NATIVE_CPU_LIMIT) {
            throw new GraphBuildException("CPU id for stage " + stageName
                + " exceeds native CPU limit " + (NATIVE_CPU_LIMIT - 1) + ": " + cpuId);
        }
    }

    private void validateTypeCompatibility(final NodeDefinition from, final NodeDefinition to) {
        final Class<?> outputType = Objects.requireNonNull(from.outputType(), "outputType");
        final Class<?> inputType = Objects.requireNonNull(to.inputType(), "inputType");

        if (to.kind() == GraphPlan.NodeKind.JOIN) {
            return;
        }
        if (!inputType.isAssignableFrom(outputType)) {
            throw new GraphBuildException(
                "edge " + from.name() + "->" + to.name() + " type mismatch: "
                    + outputType.getName() + " is not assignable to " + inputType.getName()
            );
        }
    }

    private void validateShape(
        final Map<String, NodeDefinition> nodes,
        final Map<String, List<EdgeDefinition>> incomingByTarget,
        final Map<String, List<EdgeDefinition>> normalOutgoingBySource
    ) {
        for (final NodeDefinition node : nodes.values()) {
            final int in = incomingByTarget.getOrDefault(node.name(), List.of()).size();
            final int out = normalOutgoingBySource.getOrDefault(node.name(), List.of()).size();

            switch (node.kind()) {
                case SOURCE -> {
                    if (in != 0) {
                        throw new GraphBuildException("source must not have incoming edges: " + node.name());
                    }
                    if (out != 1) {
                        throw new GraphBuildException("source must have exactly one normal outgoing edge: " + node.name());
                    }
                }
                case STAGE -> {
                    if (in != 1) {
                        throw new GraphBuildException("stage must have exactly one incoming edge: " + node.name());
                    }
                    if (out != 1) {
                        throw new GraphBuildException("stage must have exactly one normal outgoing edge: " + node.name());
                    }
                }
                case SINK -> {
                    if (in != 1) {
                        throw new GraphBuildException("sink must have exactly one incoming edge: " + node.name());
                    }
                    if (out != 0) {
                        throw new GraphBuildException("sink must not have outgoing edges: " + node.name());
                    }
                }
                case DISPATCH, BROADCAST -> {
                    if (in != 1) {
                        throw new GraphBuildException(node.kind() + " node must have exactly one incoming edge: " + node.name());
                    }
                    if (out < 1) {
                        throw new GraphBuildException(node.kind() + " node must have at least one branch: " + node.name());
                    }
                    validateDispatchWeights(node, out);
                    validateBroadcastCopy(node);
                }
                case PARTITION -> {
                    if (in != 1) {
                        throw new GraphBuildException("partition node must have exactly one incoming edge: " + node.name());
                    }
                    if (out != node.partitionSpec().lanes()) {
                        throw new GraphBuildException("partition node " + node.name() + " requires "
                            + node.partitionSpec().lanes() + " lanes but has " + out + " outgoing edges");
                    }
                }
                case JOIN -> {
                    if (in < 2) {
                        throw new GraphBuildException("join node must have at least two incoming edges: " + node.name());
                    }
                    if (out != 1) {
                        throw new GraphBuildException("join node must have exactly one outgoing edge: " + node.name());
                    }
                }
                default -> throw new GraphBuildException("unsupported node kind: " + node.kind());
            }
        }
    }

    private void validateDispatchWeights(final NodeDefinition node, final int out) {
        if (node.kind() != GraphPlan.NodeKind.DISPATCH) {
            return;
        }
        final DispatchSpec<?> spec = node.dispatchSpec();
        if (spec.kind() == DispatchSpec.DispatchKind.WEIGHTED && spec.weights().length != out) {
            throw new GraphBuildException("dispatch node " + node.name() + " has " + out
                + " branches but " + spec.weights().length + " weights");
        }
    }

    private void validateBroadcastCopy(final NodeDefinition node) {
        if (node.kind() != GraphPlan.NodeKind.BROADCAST) {
            return;
        }
        if (node.broadcastSpec().kind() != BroadcastSpec.BroadcastKind.COPY || node.broadcastSpec().copier() != null) {
            return;
        }
        final Class<?> type = node.inputType();
        if (type.isPrimitive()
            || type.isEnum()
            || type.isRecord()
            || String.class == type
            || Number.class.isAssignableFrom(type)
            || Boolean.class == type
            || Character.class == type) {
            return;
        }
        throw new GraphBuildException("broadcast copy for mutable type " + type.getName()
            + " requires BroadcastSpec.copy(copier)");
    }

    private void validateRedirects(
        final Map<String, NodeDefinition> nodes,
        final List<EdgeDefinition> edges,
        final Map<String, Map<String, EdgeDefinition>> redirectBySourceAndTarget
    ) {
        for (final EdgeDefinition edge : edges) {
            final String redirectTarget = edge.spec().overflowPolicy().redirectTarget();
            if (redirectTarget == null) {
                continue;
            }
            if (!nodes.containsKey(redirectTarget)) {
                throw new GraphBuildException("redirect target does not exist: " + redirectTarget);
            }
            final EdgeDefinition redirect = redirectBySourceAndTarget
                .getOrDefault(edge.from(), Map.of())
                .get(redirectTarget);
            if (redirect == null) {
                throw new GraphBuildException("redirect policy on edge " + edge.key()
                    + " requires edge " + edge.from() + "->" + redirectTarget);
            }
            if (redirect == edge) {
                throw new GraphBuildException("edge cannot redirect to itself: " + edge.key());
            }
        }
    }

    private void validatePreallocatedSources(
        final Map<String, NodeDefinition> nodes,
        final Map<String, List<EdgeDefinition>> outgoingBySource,
        final Map<String, List<EdgeDefinition>> normalOutgoingBySource
    ) {
        for (final NodeDefinition source : nodes.values()) {
            if (source.preallocationSpec() == null) {
                continue;
            }
            if (source.stampedSource()) {
                throw new GraphBuildException("preallocated source cannot be stamped: " + source.name());
            }
            if (source.sourceMode() != SourceMode.SINGLE_PRODUCER) {
                throw new GraphBuildException("preallocated source must be single-producer: " + source.name());
            }

            String currentName = source.name();
            while (true) {
                final List<EdgeDefinition> allOutgoing = outgoingBySource.getOrDefault(currentName, List.of());
                final List<EdgeDefinition> normalOutgoing = normalOutgoingBySource.getOrDefault(currentName, List.of());
                if (allOutgoing.size() != normalOutgoing.size()) {
                    throw new GraphBuildException("preallocated source " + source.name()
                        + " does not support redirect edges in its reuse domain");
                }
                if (normalOutgoing.isEmpty()) {
                    break;
                }
                if (normalOutgoing.size() != 1) {
                    throw new GraphBuildException("preallocated source " + source.name()
                        + " requires a linear single-output topology");
                }

                final EdgeDefinition edge = normalOutgoing.get(0);
                validatePreallocatedEdge(source.name(), edge);
                final NodeDefinition target = nodes.get(edge.to());
                if (target.kind() == GraphPlan.NodeKind.SINK) {
                    break;
                }
                if (target.kind() != GraphPlan.NodeKind.STAGE
                    || target.batchLogic() != null
                    || target.spec().batchPolicy().kind() != BatchPolicy.BatchKind.DISABLED) {
                    throw new GraphBuildException("preallocated source " + source.name()
                        + " supports only linear single-message stages and a terminal sink");
                }
                currentName = target.name();
            }
        }
    }

    private void validatePreallocatedEdge(final String sourceName, final EdgeDefinition edge) {
        final EdgeSpec spec = edge.spec();
        if (spec.kind() != EdgeSpec.EdgeKind.SPSC_RING
            || spec.overflowPolicy().kind() != OverflowPolicy.OverflowKind.BLOCK
            || spec.memoryMode().kind() != MemoryMode.MemoryKind.ON_HEAP_SLOTS
            || spec.batchPolicy().kind() != BatchPolicy.BatchKind.DISABLED) {
            throw new GraphBuildException("preallocated source " + sourceName
                + " requires blocking on-heap SPSC edges without edge batching");
        }
    }

    private List<String> topologicalOrder(final Map<String, NodeDefinition> nodes, final List<EdgeDefinition> edges) {
        final Map<String, List<String>> adjacency = new LinkedHashMap<>();
        final Map<String, Integer> indegree = new LinkedHashMap<>();

        for (final String name : nodes.keySet()) {
            adjacency.put(name, new ArrayList<>());
            indegree.put(name, 0);
        }
        for (final EdgeDefinition edge : edges) {
            adjacency.get(edge.from()).add(edge.to());
            indegree.compute(edge.to(), (ignored, value) -> value + 1);
        }

        final ArrayDeque<String> ready = new ArrayDeque<>();
        for (final Map.Entry<String, Integer> entry : indegree.entrySet()) {
            if (entry.getValue() == 0) {
                ready.add(entry.getKey());
            }
        }

        final List<String> order = new ArrayList<>(nodes.size());
        while (!ready.isEmpty()) {
            final String node = ready.removeFirst();
            order.add(node);
            for (final String target : adjacency.get(node)) {
                final int next = indegree.compute(target, (ignored, value) -> value - 1);
                if (next == 0) {
                    ready.addLast(target);
                }
            }
        }

        if (order.size() != nodes.size()) {
            throw new GraphBuildException("graph must be a DAG; cycle detected");
        }
        return order;
    }

    private List<GraphPlan.Node> buildPlanNodes(final Map<String, NodeDefinition> nodes) {
        return nodes.values().stream()
            .map(node -> new GraphPlan.Node(
                node.name(),
                node.kind(),
                node.inputType(),
                node.outputType(),
                node.spec(),
                node.sourceMode(),
                node.preallocationSpec() != null
            ))
            .toList();
    }

    private List<GraphPlan.Edge> buildPlanEdges(
        final Map<String, NodeDefinition> nodes,
        final List<EdgeDefinition> edges
    ) {
        return edges.stream()
            .map(edge -> new GraphPlan.Edge(
                edge.from(),
                edge.to(),
                edge.messageType(),
                edge.spec(),
                allocationOwner(nodes, edge),
                edge.branchIndex(),
                edge.redirectOnly()
            ))
            .toList();
    }

    private List<GraphPlan.Placement> buildPlanPlacements(
        final Map<String, NodeDefinition> nodes,
        final List<String> workerOrder
    ) {
        return workerOrder.stream()
            .map(nodes::get)
            .map(node -> GraphPlan.Placement.from(node.name(), node.spec().pinPolicy()))
            .toList();
    }

    private static String allocationOwner(final Map<String, NodeDefinition> nodes, final EdgeDefinition edge) {
        final NodeDefinition from = nodes.get(edge.from());
        return from.kind() == GraphPlan.NodeKind.SOURCE ? edge.to() : edge.from();
    }

    private static Map<String, List<EdgeDefinition>> groupByTarget(final List<EdgeDefinition> edges) {
        final Map<String, List<EdgeDefinition>> grouped = new LinkedHashMap<>();
        for (final EdgeDefinition edge : edges) {
            grouped.computeIfAbsent(edge.to(), ignored -> new ArrayList<>()).add(edge);
        }
        return freezeGrouped(grouped);
    }

    private static Map<String, List<EdgeDefinition>> groupBySource(final List<EdgeDefinition> edges) {
        final Map<String, List<EdgeDefinition>> grouped = new LinkedHashMap<>();
        for (final EdgeDefinition edge : edges) {
            grouped.computeIfAbsent(edge.from(), ignored -> new ArrayList<>()).add(edge);
        }
        return freezeGrouped(grouped);
    }

    private static Map<String, List<EdgeDefinition>> normalOutgoingBySource(
        final Map<String, List<EdgeDefinition>> outgoingBySource
    ) {
        final Map<String, List<EdgeDefinition>> normal = new LinkedHashMap<>();
        for (final Map.Entry<String, List<EdgeDefinition>> entry : outgoingBySource.entrySet()) {
            final List<EdgeDefinition> edges = entry.getValue().stream()
                .filter(edge -> !edge.redirectOnly())
                .toList();
            if (!edges.isEmpty()) {
                normal.put(entry.getKey(), edges);
            }
        }
        return freezeGrouped(normal);
    }

    private static Map<String, Map<String, EdgeDefinition>> redirectBySourceAndTarget(final List<EdgeDefinition> edges) {
        final Map<String, Map<String, EdgeDefinition>> grouped = new LinkedHashMap<>();
        for (final EdgeDefinition edge : edges) {
            if (!edge.redirectOnly()) {
                continue;
            }
            grouped.computeIfAbsent(edge.from(), ignored -> new LinkedHashMap<>()).put(edge.to(), edge);
        }
        final Map<String, Map<String, EdgeDefinition>> frozen = new LinkedHashMap<>();
        for (final Map.Entry<String, Map<String, EdgeDefinition>> entry : grouped.entrySet()) {
            frozen.put(entry.getKey(), Collections.unmodifiableMap(new LinkedHashMap<>(entry.getValue())));
        }
        return Collections.unmodifiableMap(frozen);
    }

    private static Map<String, List<EdgeDefinition>> freezeGrouped(
        final Map<String, List<EdgeDefinition>> grouped
    ) {
        final Map<String, List<EdgeDefinition>> frozen = new LinkedHashMap<>();
        for (final Map.Entry<String, List<EdgeDefinition>> entry : grouped.entrySet()) {
            frozen.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return Collections.unmodifiableMap(frozen);
    }
}
