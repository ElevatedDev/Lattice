package io.github.elevateddev.lattice.graph;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.placement.PinPolicy;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable description of a graph after validation and compilation.
 * <p>
 * A plan exposes the logical nodes, directed edges, worker order, and requested
 * placement metadata that the runtime will use when {@link StaticGraph#start()}
 * is called. The collections returned by this type are snapshots of the
 * compiled topology and do not change while the graph runs.
 */
public final class GraphPlan {

    private final String name;
    private final List<Node> nodes;
    private final List<Edge> edges;
    private final List<String> workerOrder;
    private final List<Placement> placements;

    /**
     * Creates a plan with default placement metadata derived from node specs.
     *
     * @param name graph name
     * @param nodes compiled nodes in declaration order
     * @param edges compiled directed edges
     * @param workerOrder stage worker startup order
     */
    public GraphPlan(
        final String name,
        final List<Node> nodes,
        final List<Edge> edges,
        final List<String> workerOrder
    ) {
        this(name, nodes, edges, workerOrder, buildDefaultPlacements(nodes));
    }

    /**
     * Creates a plan with explicit placement metadata.
     *
     * @param name graph name
     * @param nodes compiled nodes in declaration order
     * @param edges compiled directed edges
     * @param workerOrder stage worker startup order
     * @param placements requested worker placement metadata
     */
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

    /**
     * Returns the graph name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the immutable compiled node list.
     */
    public List<Node> nodes() {
        return nodes;
    }

    /**
     * Returns the immutable compiled edge list.
     */
    public List<Edge> edges() {
        return edges;
    }

    /**
     * Returns the immutable worker startup order.
     */
    public List<String> workerOrder() {
        return workerOrder;
    }

    /**
     * Returns the immutable placement metadata list.
     */
    public List<Placement> placements() {
        return placements;
    }

    /**
     * Finds a compiled node by name.
     *
     * @param name node name
     * @return matching node, if present
     */
    public Optional<Node> node(final String name) {
        return nodes.stream().filter(node -> node.name().equals(name)).findFirst();
    }

    /**
     * Finds a compiled edge by source and target names.
     *
     * @param from source node name
     * @param to target node name
     * @return matching edge, if present
     */
    public Optional<Edge> edge(final String from, final String to) {
        return edges.stream().filter(edge -> edge.from().equals(from) && edge.to().equals(to)).findFirst();
    }

    /**
     * Finds placement metadata for a stage worker.
     *
     * @param stageName stage node name
     * @return matching placement, if present
     */
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

    /**
     * Logical node kinds supported by the public graph DSL.
     */
    public enum NodeKind {
        /**
         * External ingress node.
         */
        SOURCE,
        /**
         * One-message or batch transformation stage.
         */
        STAGE,
        /**
         * Terminal consumer stage.
         */
        SINK,
        /**
         * Router that chooses one downstream edge.
         */
        DISPATCH,
        /**
         * Router that publishes to every downstream edge.
         */
        BROADCAST,
        /**
         * Router that maps each key to a fixed lane.
         */
        PARTITION,
        /**
         * Correlating stage that combines stamped branches.
         */
        JOIN
    }

    /**
     * Immutable node metadata in a compiled graph plan.
     */
    public static final class Node {
        private final String name;
        private final NodeKind kind;
        private final Class<?> inputType;
        private final Class<?> outputType;
        private final StageSpec spec;
        private final SourceMode sourceMode;
        private final boolean preallocatedSource;

        /**
         * Creates node metadata with the default multi-producer source mode.
         */
        public Node(
            final String name,
            final NodeKind kind,
            final Class<?> inputType,
            final Class<?> outputType,
            final StageSpec spec
        ) {
            this(name, kind, inputType, outputType, spec, SourceMode.MULTI_PRODUCER);
        }

        /**
         * Creates node metadata with an explicit source mode.
         */
        public Node(
            final String name,
            final NodeKind kind,
            final Class<?> inputType,
            final Class<?> outputType,
            final StageSpec spec,
            final SourceMode sourceMode
        ) {
            this(name, kind, inputType, outputType, spec, sourceMode, false);
        }

        /**
         * Creates node metadata with explicit source and preallocation flags.
         */
        public Node(
            final String name,
            final NodeKind kind,
            final Class<?> inputType,
            final Class<?> outputType,
            final StageSpec spec,
            final SourceMode sourceMode,
            final boolean preallocatedSource
        ) {
            this.name = requireName(name, "node");
            this.kind = Objects.requireNonNull(kind, "kind");
            this.inputType = inputType;
            this.outputType = outputType;
            this.spec = spec;
            this.sourceMode = sourceMode == null ? SourceMode.MULTI_PRODUCER : sourceMode;
            this.preallocatedSource = preallocatedSource;
        }

        /**
         * Returns the node name.
         */
        public String name() {
            return name;
        }

        /**
         * Returns the logical node kind.
         */
        public NodeKind kind() {
            return kind;
        }

        /**
         * Returns the input type, or {@code null} for source nodes.
         */
        public Class<?> inputType() {
            return inputType;
        }

        /**
         * Returns the output type, or {@code null} for terminal sink nodes.
         */
        public Class<?> outputType() {
            return outputType;
        }

        /**
         * Returns the stage configuration when this node owns a worker.
         */
        public StageSpec spec() {
            return spec;
        }

        /**
         * Returns the source producer mode for source nodes.
         */
        public SourceMode sourceMode() {
            return sourceMode;
        }

        /**
         * Returns whether this source uses a preallocated payload pool.
         */
        public boolean preallocatedSource() {
            return preallocatedSource;
        }
    }

    /**
     * Immutable edge metadata in a compiled graph plan.
     */
    public static final class Edge {
        private final String from;
        private final String to;
        private final Class<?> messageType;
        private final EdgeSpec spec;
        private final String allocationOwner;
        private final int branchIndex;
        private final boolean redirectOnly;

        /**
         * Creates normal edge metadata.
         */
        public Edge(final String from, final String to, final Class<?> messageType, final EdgeSpec spec) {
            this(from, to, messageType, spec, "");
        }

        /**
         * Creates edge metadata with allocation ownership information.
         */
        public Edge(
            final String from,
            final String to,
            final Class<?> messageType,
            final EdgeSpec spec,
            final String allocationOwner
        ) {
            this(from, to, messageType, spec, allocationOwner, -1, false);
        }

        /**
         * Creates edge metadata with routing branch information.
         */
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

        /**
         * Returns the source node name.
         */
        public String from() {
            return from;
        }

        /**
         * Returns the target node name.
         */
        public String to() {
            return to;
        }

        /**
         * Returns the runtime message type accepted by the edge.
         */
        public Class<?> messageType() {
            return messageType;
        }

        /**
         * Returns the edge configuration.
         */
        public EdgeSpec spec() {
            return spec;
        }

        /**
         * Returns the worker or subsystem that owns this edge allocation, when
         * known.
         */
        public String allocationOwner() {
            return allocationOwner;
        }

        /**
         * Returns the branch index for routed edges, or {@code -1} when not
         * applicable.
         */
        public int branchIndex() {
            return branchIndex;
        }

        /**
         * Returns whether this edge exists only as a redirect target.
         */
        public boolean redirectOnly() {
            return redirectOnly;
        }
    }

    /**
     * Requested placement metadata for one stage worker.
     */
    public static final class Placement {
        private final String stageName;
        private final PinPolicy pinPolicy;
        private final int expectedCpu;
        private final BitSet expectedCpuSet;
        private final int expectedNumaNode;

        /**
         * Creates placement metadata.
         */
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

        /**
         * Creates placement metadata from a public pin policy.
         */
        public static Placement from(final String stageName, final PinPolicy pinPolicy) {
            final PinPolicy policy = Objects.requireNonNull(pinPolicy, "pinPolicy");
            final int expectedCpu = switch (policy.kind()) {
                case CPU, CORE -> policy.cpuId();
                default -> -1;
            };
            final int expectedNumaNode = policy.kind() == PinPolicy.PinKind.NUMA_NODE ? policy.numaNode() : -1;
            return new Placement(stageName, policy, expectedCpu, policy.cpuSet(), expectedNumaNode);
        }

        /**
         * Returns the stage name.
         */
        public String stageName() {
            return stageName;
        }

        /**
         * Returns the requested pin policy.
         */
        public PinPolicy pinPolicy() {
            return pinPolicy;
        }

        /**
         * Returns the requested CPU id, or {@code -1} when not requested.
         */
        public int expectedCpu() {
            return expectedCpu;
        }

        /**
         * Returns the requested CPU set. The returned bit set is a defensive
         * copy.
         */
        public BitSet expectedCpuSet() {
            return (BitSet) expectedCpuSet.clone();
        }

        /**
         * Returns the requested NUMA node, or {@code -1} when not requested.
         */
        public int expectedNumaNode() {
            return expectedNumaNode;
        }

        /**
         * Returns whether this worker inherits the process CPU set.
         */
        public boolean inheritsCpuset() {
            return pinPolicy.kind() == PinPolicy.PinKind.INHERIT_CPUSET;
        }
    }
}
