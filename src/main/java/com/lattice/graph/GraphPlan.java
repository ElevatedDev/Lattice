package com.lattice.graph;

import com.lattice.edge.EdgeSpec;
import com.lattice.placement.PinPolicy;
import com.lattice.stage.StageSpec;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class GraphPlan {

    private final String name;
    private final List<Node> nodes;
    private final List<Edge> edges;
    private final List<String> workerOrder;
    private final List<Placement> placements;

    public GraphPlan(
        final String name,
        final List<Node> nodes,
        final List<Edge> edges,
        final List<String> workerOrder
    ) {
        this(name, nodes, edges, workerOrder, buildDefaultPlacements(nodes));
    }

    public GraphPlan(
        final String name,
        final List<Node> nodes,
        final List<Edge> edges,
        final List<String> workerOrder,
        final List<Placement> placements
    ) {
        this.name = requireName(name, "graph");
        this.nodes = List.copyOf(nodes);
        this.edges = List.copyOf(edges);
        this.workerOrder = List.copyOf(workerOrder);
        this.placements = List.copyOf(placements);
    }

    public String name() {
        return name;
    }

    public List<Node> nodes() {
        return nodes;
    }

    public List<Edge> edges() {
        return edges;
    }

    public List<String> workerOrder() {
        return workerOrder;
    }

    public List<Placement> placements() {
        return placements;
    }

    public Optional<Node> node(final String name) {
        return nodes.stream().filter(node -> node.name().equals(name)).findFirst();
    }

    public Optional<Edge> edge(final String from, final String to) {
        return edges.stream().filter(edge -> edge.from().equals(from) && edge.to().equals(to)).findFirst();
    }

    public Optional<Placement> placement(final String stageName) {
        return placements.stream().filter(placement -> placement.stageName().equals(stageName)).findFirst();
    }

    private static String requireName(final String value, final String label) {
        final String trimmed = Objects.requireNonNull(value, label + " name").trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(label + " name must not be blank");
        }
        return trimmed;
    }

    private static List<Placement> buildDefaultPlacements(final List<Node> nodes) {
        return nodes.stream()
            .filter(node -> node.kind() != NodeKind.SOURCE)
            .map(node -> Placement.from(node.name(), node.spec() == null ? PinPolicy.none() : node.spec().pinPolicy()))
            .toList();
    }

    public enum NodeKind {
        SOURCE,
        STAGE,
        SINK,
        DISPATCH,
        BROADCAST,
        PARTITION,
        JOIN
    }

    public static final class Node {
        private final String name;
        private final NodeKind kind;
        private final Class<?> inputType;
        private final Class<?> outputType;
        private final StageSpec spec;
        private final SourceMode sourceMode;

        public Node(
            final String name,
            final NodeKind kind,
            final Class<?> inputType,
            final Class<?> outputType,
            final StageSpec spec
        ) {
            this(name, kind, inputType, outputType, spec, SourceMode.MULTI_PRODUCER);
        }

        public Node(
            final String name,
            final NodeKind kind,
            final Class<?> inputType,
            final Class<?> outputType,
            final StageSpec spec,
            final SourceMode sourceMode
        ) {
            this.name = requireName(name, "node");
            this.kind = Objects.requireNonNull(kind, "kind");
            this.inputType = inputType;
            this.outputType = outputType;
            this.spec = spec;
            this.sourceMode = sourceMode == null ? SourceMode.MULTI_PRODUCER : sourceMode;
        }

        public String name() {
            return name;
        }

        public NodeKind kind() {
            return kind;
        }

        public Class<?> inputType() {
            return inputType;
        }

        public Class<?> outputType() {
            return outputType;
        }

        public StageSpec spec() {
            return spec;
        }

        public SourceMode sourceMode() {
            return sourceMode;
        }
    }

    public static final class Edge {
        private final String from;
        private final String to;
        private final Class<?> messageType;
        private final EdgeSpec spec;
        private final String allocationOwner;
        private final int branchIndex;
        private final boolean redirectOnly;

        public Edge(final String from, final String to, final Class<?> messageType, final EdgeSpec spec) {
            this(from, to, messageType, spec, "");
        }

        public Edge(
            final String from,
            final String to,
            final Class<?> messageType,
            final EdgeSpec spec,
            final String allocationOwner
        ) {
            this(from, to, messageType, spec, allocationOwner, -1, false);
        }

        public Edge(
            final String from,
            final String to,
            final Class<?> messageType,
            final EdgeSpec spec,
            final String allocationOwner,
            final int branchIndex,
            final boolean redirectOnly
        ) {
            this.from = requireName(from, "edge source");
            this.to = requireName(to, "edge target");
            this.messageType = Objects.requireNonNull(messageType, "messageType");
            this.spec = Objects.requireNonNull(spec, "spec");
            this.allocationOwner = allocationOwner == null ? "" : allocationOwner;
            this.branchIndex = branchIndex;
            this.redirectOnly = redirectOnly;
        }

        public String from() {
            return from;
        }

        public String to() {
            return to;
        }

        public Class<?> messageType() {
            return messageType;
        }

        public EdgeSpec spec() {
            return spec;
        }

        public String allocationOwner() {
            return allocationOwner;
        }

        public int branchIndex() {
            return branchIndex;
        }

        public boolean redirectOnly() {
            return redirectOnly;
        }
    }

    public static final class Placement {
        private final String stageName;
        private final PinPolicy pinPolicy;
        private final int expectedCpu;
        private final BitSet expectedCpuSet;
        private final int expectedNumaNode;

        public Placement(
            final String stageName,
            final PinPolicy pinPolicy,
            final int expectedCpu,
            final BitSet expectedCpuSet,
            final int expectedNumaNode
        ) {
            this.stageName = requireName(stageName, "stage");
            this.pinPolicy = Objects.requireNonNull(pinPolicy, "pinPolicy");
            this.expectedCpu = expectedCpu;
            this.expectedCpuSet = expectedCpuSet == null ? new BitSet() : (BitSet) expectedCpuSet.clone();
            this.expectedNumaNode = expectedNumaNode;
        }

        public static Placement from(final String stageName, final PinPolicy pinPolicy) {
            final PinPolicy policy = Objects.requireNonNull(pinPolicy, "pinPolicy");
            final int expectedCpu = switch (policy.kind()) {
                case CPU, CORE -> policy.cpuId();
                default -> -1;
            };
            final int expectedNumaNode = policy.kind() == PinPolicy.PinKind.NUMA_NODE ? policy.numaNode() : -1;
            return new Placement(stageName, policy, expectedCpu, policy.cpuSet(), expectedNumaNode);
        }

        public String stageName() {
            return stageName;
        }

        public PinPolicy pinPolicy() {
            return pinPolicy;
        }

        public int expectedCpu() {
            return expectedCpu;
        }

        public BitSet expectedCpuSet() {
            return (BitSet) expectedCpuSet.clone();
        }

        public int expectedNumaNode() {
            return expectedNumaNode;
        }

        public boolean inheritsCpuset() {
            return pinPolicy.kind() == PinPolicy.PinKind.INHERIT_CPUSET;
        }
    }
}
