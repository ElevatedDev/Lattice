package io.github.elevateddev.lattice.graph;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.placement.PinPolicy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable build-time report explaining physical runtime decisions for a
 * compiled graph.
 * <p>
 * The logical topology remains available through {@link StaticGraph#plan()}.
 * This report describes how that topology was lowered into runtime workers,
 * edges, senders, and merge decisions.
 */
public final class GraphCompilationReport {
    private final String graphName;
    private final Controls controls;
    private final List<Worker> workers;
    private final List<Edge> edges;
    private final List<Sender> senders;
    private final List<Merge> merges;
    private final List<Fallback> fallbacks;
    private final Map<String, Worker> workersByName;
    private final Map<String, Edge> edgesByKey;
    private final Map<String, List<Merge>> mergesByOwner;
    private final Map<String, List<Fallback>> fallbacksBySubject;

    /**
     * Creates a compilation report from immutable row snapshots.
     */
    public GraphCompilationReport(
        final String graphName,
        final Controls controls,
        final List<Worker> workers,
        final List<Edge> edges,
        final List<Sender> senders,
        final List<Merge> merges,
        final List<Fallback> fallbacks
    ) {
        this.graphName = requireName(graphName, "graph");
        this.controls = Objects.requireNonNull(controls, "controls");
        this.workers = List.copyOf(workers);
        this.edges = List.copyOf(edges);
        this.senders = List.copyOf(senders);
        this.merges = List.copyOf(merges);
        this.fallbacks = List.copyOf(fallbacks);
        this.workersByName = indexWorkers(this.workers);
        this.edgesByKey = indexEdges(this.edges);
        this.mergesByOwner = groupMerges(this.merges);
        this.fallbacksBySubject = groupFallbacks(this.fallbacks);
    }

    /**
     * Returns the graph name.
     */
    public String graphName() {
        return graphName;
    }

    /**
     * Returns graph-level controls that influenced compilation.
     */
    public Controls controls() {
        return controls;
    }

    /**
     * Returns worker decision rows in deterministic compilation order.
     */
    public List<Worker> workers() {
        return workers;
    }

    /**
     * Returns edge decision rows in declaration order.
     */
    public List<Edge> edges() {
        return edges;
    }

    /**
     * Returns sender decision rows in deterministic edge order.
     */
    public List<Sender> senders() {
        return senders;
    }

    /**
     * Returns positive merge decisions in deterministic owner order.
     */
    public List<Merge> merges() {
        return merges;
    }

    /**
     * Returns fallback decisions for candidates that did not merge.
     */
    public List<Fallback> fallbacks() {
        return fallbacks;
    }

    /**
     * Finds a worker decision by logical node name.
     */
    public Optional<Worker> worker(final String name) {
        return Optional.ofNullable(workersByName.get(name));
    }

    /**
     * Finds an edge decision by source and target names.
     */
    public Optional<Edge> edge(final String from, final String to) {
        return Optional.ofNullable(edgesByKey.get(edgeKey(from, to)));
    }

    /**
     * Returns merge decisions owned by the given worker.
     */
    public List<Merge> mergesForOwner(final String ownerName) {
        return mergesByOwner.getOrDefault(ownerName, List.of());
    }

    /**
     * Returns fallback decisions for the given subject.
     */
    public List<Fallback> fallbacksFor(final String subject) {
        return fallbacksBySubject.getOrDefault(subject, List.of());
    }

    /**
     * Returns whether the compiler applied at least one merge.
     */
    public boolean hasMerges() {
        return !merges.isEmpty();
    }

    /**
     * Returns whether the compiler recorded any fallback explanations.
     */
    public boolean hasFallbacks() {
        return !fallbacks.isEmpty();
    }

    /**
     * Renders a concise human-readable summary.
     * <p>
     * This output is intended for humans, examples, and support conversations.
     * Machine consumers should use the structured rows instead.
     */
    public String toSummaryString() {
        final StringBuilder builder = new StringBuilder(512);
        builder.append("GraphCompilationReport{name=").append(graphName).append("}\n");
        builder.append("controls: fusion=").append(enabled(controls.fusionEnabled()))
            .append(" sourceInline=").append(enabled(controls.sourceInlineEnabled()))
            .append(" inlineElision=").append(enabled(controls.inlineSourcePhysicalElisionEnabled()))
            .append(" topologyAwarePlacement=").append(enabled(controls.topologyAwarePlacementEnabled()))
            .append(" customExceptionHandler=").append(controls.customExceptionHandler())
            .append('\n');

        builder.append("workers:\n");
        appendOrNone(builder, workers, GraphCompilationReport::appendWorker);

        builder.append("edges:\n");
        appendOrNone(builder, edges, GraphCompilationReport::appendEdge);

        builder.append("senders:\n");
        appendOrNone(builder, senders, GraphCompilationReport::appendSender);

        builder.append("merges:\n");
        appendOrNone(builder, merges, GraphCompilationReport::appendMerge);

        builder.append("fallbacks:\n");
        appendOrNone(builder, fallbacks, GraphCompilationReport::appendFallback);
        return builder.toString();
    }

    /**
     * Builds a logical-only report for non-Lattice {@link StaticGraph}
     * implementations that expose a {@link GraphPlan} but no physical planner.
     */
    public static GraphCompilationReport logicalOnly(final GraphPlan plan) {
        final GraphPlan graphPlan = Objects.requireNonNull(plan, "plan");
        final List<Worker> workers = new ArrayList<>();
        for (final String workerName : graphPlan.workerOrder()) {
            final GraphPlan.Node node = graphPlan.node(workerName).orElse(null);
            if (node == null) {
                continue;
            }
            workers.add(new Worker(
                node.name(),
                node.kind(),
                OperatorKind.from(node.kind(), false),
                WorkerDecisionKind.RUNNABLE,
                graphPlan.edges().stream().anyMatch(edge -> edge.from().equals(node.name()))
                    ? OutputKind.EDGE
                    : OutputKind.NONE,
                "",
                graphPlan.placement(node.name())
                    .map(GraphPlan.Placement::pinPolicy)
                    .orElse(node.spec() == null ? PinPolicy.none() : node.spec().pinPolicy()),
                null,
                ""
            ));
        }

        final List<Edge> edges = graphPlan.edges().stream()
            .map(edge -> new Edge(
                edge.from(),
                edge.to(),
                edge.spec().kind(),
                edge.spec().kind(),
                edge.redirectOnly() ? EdgeUseKind.REDIRECT_ONLY : EdgeUseKind.NORMAL,
                edge.allocationOwner(),
                false,
                edge.redirectOnly(),
                null,
                ""
            ))
            .toList();

        return new GraphCompilationReport(
            graphPlan.name(),
            new Controls(false, false, false, false, false),
            workers,
            edges,
            List.of(),
            List.of(),
            List.of()
        );
    }

    private static String enabled(final boolean enabled) {
        return enabled ? "enabled" : "disabled";
    }

    private interface RowAppender<T> {
        void append(StringBuilder builder, T row);
    }

    private static <T> void appendOrNone(
        final StringBuilder builder,
        final List<T> rows,
        final RowAppender<T> appender
    ) {
        if (rows.isEmpty()) {
            builder.append("  none\n");
            return;
        }
        for (final T row : rows) {
            appender.append(builder, row);
        }
    }

    private static void appendWorker(final StringBuilder builder, final Worker worker) {
        builder.append("  ").append(worker.name())
            .append(' ').append(worker.decision())
            .append(" operator=").append(worker.operatorKind())
            .append(" output=").append(worker.output())
            .append(" pin=").append(pin(worker.effectivePinPolicy()));
        if (!worker.fusedOwner().isEmpty()) {
            builder.append(" owner=").append(worker.fusedOwner());
        }
        builder.append('\n');
        appendReason(builder, worker.reason(), worker.explanation());
    }

    private static void appendEdge(final StringBuilder builder, final Edge edge) {
        builder.append("  ").append(edge.key())
            .append(" declared=").append(edge.declaredKind())
            .append(" effective=").append(edge.effectiveKind())
            .append(" use=").append(edge.use())
            .append(" owner=").append(edge.allocationOwner());
        if (edge.redirectOnly()) {
            builder.append(" redirectOnly=true");
        }
        if (edge.sourceIngressCloseGuard()) {
            builder.append(" closeGuard=true");
        }
        builder.append('\n');
        appendReason(builder, edge.reason(), edge.explanation());
    }

    private static void appendSender(final StringBuilder builder, final Sender sender) {
        builder.append("  ").append(sender.edgeKey())
            .append(" owner=").append(sender.ownerName())
            .append(" kind=").append(sender.kind())
            .append(" output=").append(sender.output());
        if (!sender.redirectEdgeKey().isEmpty()) {
            builder.append(" redirect=").append(sender.redirectEdgeKey());
        }
        builder.append('\n');
        appendReason(builder, sender.reason(), sender.explanation());
    }

    private static void appendMerge(final StringBuilder builder, final Merge merge) {
        builder.append("  ").append(merge.kind())
            .append(" owner=").append(merge.ownerName())
            .append(" merged=").append(merge.mergedNodes())
            .append(" elided=").append(merge.elidedEdges());
        if (!merge.terminalNode().isEmpty()) {
            builder.append(" terminal=").append(merge.terminalNode());
        }
        merge.reason().ifPresent(reason -> builder.append(" reason=").append(reason.code()));
        builder.append('\n');
        if (!merge.explanation().isEmpty()) {
            builder.append("    ").append(merge.explanation()).append('\n');
        }
    }

    private static void appendFallback(final StringBuilder builder, final Fallback fallback) {
        builder.append("  ").append(fallback.reason().code())
            .append(' ').append(fallback.subjectKind().name().toLowerCase())
            .append('=').append(fallback.subject())
            .append(": ").append(fallback.explanation())
            .append('\n');
    }

    private static void appendReason(
        final StringBuilder builder,
        final Optional<Reason> reason,
        final String explanation
    ) {
        reason.ifPresent(value -> builder.append("    ")
            .append(value.code())
            .append(": ")
            .append(explanation.isEmpty() ? value.defaultExplanation() : explanation)
            .append('\n'));
    }

    private static String pin(final PinPolicy pinPolicy) {
        return switch (pinPolicy.kind()) {
            case NONE -> "NONE";
            case CPU -> "CPU(" + pinPolicy.cpuId() + ")";
            case CORE -> "CORE(" + pinPolicy.coreId() + ")";
            case NUMA_NODE -> "NUMA_NODE(" + pinPolicy.numaNode() + ")";
            case CPU_SET -> "CPU_SET(" + pinPolicy.cpuSet() + ")";
            case INHERIT_CPUSET -> "INHERIT_CPUSET";
        };
    }

    private static Map<String, Worker> indexWorkers(final List<Worker> rows) {
        final Map<String, Worker> indexed = new LinkedHashMap<>();
        for (final Worker row : rows) {
            indexed.put(row.name(), row);
        }
        return Collections.unmodifiableMap(indexed);
    }

    private static Map<String, Edge> indexEdges(final List<Edge> rows) {
        final Map<String, Edge> indexed = new LinkedHashMap<>();
        for (final Edge row : rows) {
            indexed.put(row.key(), row);
        }
        return Collections.unmodifiableMap(indexed);
    }

    private static Map<String, List<Merge>> groupMerges(final List<Merge> rows) {
        final Map<String, List<Merge>> grouped = new LinkedHashMap<>();
        for (final Merge row : rows) {
            grouped.computeIfAbsent(row.ownerName(), ignored -> new ArrayList<>()).add(row);
        }
        return copyGrouped(grouped);
    }

    private static Map<String, List<Fallback>> groupFallbacks(final List<Fallback> rows) {
        final Map<String, List<Fallback>> grouped = new LinkedHashMap<>();
        for (final Fallback row : rows) {
            grouped.computeIfAbsent(row.subject(), ignored -> new ArrayList<>()).add(row);
        }
        return copyGrouped(grouped);
    }

    private static <T> Map<String, List<T>> copyGrouped(final Map<String, List<T>> grouped) {
        final Map<String, List<T>> copied = new LinkedHashMap<>();
        for (final Map.Entry<String, List<T>> entry : grouped.entrySet()) {
            copied.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return Collections.unmodifiableMap(copied);
    }

    private static String edgeKey(final String from, final String to) {
        return requireName(from, "edge source") + "->" + requireName(to, "edge target");
    }

    private static String requireName(final String value, final String label) {
        final String trimmed = Objects.requireNonNull(value, label + " name").trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(label + " name must not be blank");
        }
        return trimmed;
    }

    /**
     * Graph-level controls that influenced compilation.
     */
    public static final class Controls {
        private final boolean fusionEnabled;
        private final boolean sourceInlineEnabled;
        private final boolean inlineSourcePhysicalElisionEnabled;
        private final boolean topologyAwarePlacementEnabled;
        private final boolean customExceptionHandler;

        /**
         * Creates a controls row.
         */
        public Controls(
            final boolean fusionEnabled,
            final boolean sourceInlineEnabled,
            final boolean inlineSourcePhysicalElisionEnabled,
            final boolean topologyAwarePlacementEnabled,
            final boolean customExceptionHandler
        ) {
            this.fusionEnabled = fusionEnabled;
            this.sourceInlineEnabled = sourceInlineEnabled;
            this.inlineSourcePhysicalElisionEnabled = inlineSourcePhysicalElisionEnabled;
            this.topologyAwarePlacementEnabled = topologyAwarePlacementEnabled;
            this.customExceptionHandler = customExceptionHandler;
        }

        /**
         * Returns whether downstream fusion was enabled.
         */
        public boolean fusionEnabled() {
            return fusionEnabled;
        }

        /**
         * Returns whether source-inline fusion was enabled.
         */
        public boolean sourceInlineEnabled() {
            return sourceInlineEnabled;
        }

        /**
         * Returns whether source-inline physical elision was enabled.
         */
        public boolean inlineSourcePhysicalElisionEnabled() {
            return inlineSourcePhysicalElisionEnabled;
        }

        /**
         * Returns whether topology-aware placement was enabled.
         */
        public boolean topologyAwarePlacementEnabled() {
            return topologyAwarePlacementEnabled;
        }

        /**
         * Returns whether a custom stage exception handler was installed.
         */
        public boolean customExceptionHandler() {
            return customExceptionHandler;
        }
    }

    /**
     * Decision row for one logical worker-capable node.
     */
    public static final class Worker {
        private final String name;
        private final GraphPlan.NodeKind nodeKind;
        private final OperatorKind operatorKind;
        private final WorkerDecisionKind decision;
        private final OutputKind output;
        private final String fusedOwner;
        private final PinPolicy effectivePinPolicy;
        private final Reason reason;
        private final String explanation;

        /**
         * Creates a worker decision row.
         */
        public Worker(
            final String name,
            final GraphPlan.NodeKind nodeKind,
            final OperatorKind operatorKind,
            final WorkerDecisionKind decision,
            final OutputKind output,
            final String fusedOwner,
            final PinPolicy effectivePinPolicy,
            final Reason reason,
            final String explanation
        ) {
            this.name = requireName(name, "worker");
            this.nodeKind = Objects.requireNonNull(nodeKind, "nodeKind");
            this.operatorKind = Objects.requireNonNull(operatorKind, "operatorKind");
            this.decision = Objects.requireNonNull(decision, "decision");
            this.output = Objects.requireNonNull(output, "output");
            this.fusedOwner = fusedOwner == null ? "" : fusedOwner;
            this.effectivePinPolicy = Objects.requireNonNull(effectivePinPolicy, "effectivePinPolicy");
            this.reason = reason;
            this.explanation = explanation == null ? "" : explanation;
        }

        /**
         * Returns the logical node name.
         */
        public String name() {
            return name;
        }

        /**
         * Returns the logical node kind.
         */
        public GraphPlan.NodeKind nodeKind() {
            return nodeKind;
        }

        /**
         * Returns the executable operator kind.
         */
        public OperatorKind operatorKind() {
            return operatorKind;
        }

        /**
         * Returns the worker decision.
         */
        public WorkerDecisionKind decision() {
            return decision;
        }

        /**
         * Returns the output strategy selected for this worker.
         */
        public OutputKind output() {
            return output;
        }

        /**
         * Returns the worker this node was fused into, or an empty string.
         */
        public String fusedOwner() {
            return fusedOwner;
        }

        /**
         * Returns the effective pin policy after merge decisions.
         */
        public PinPolicy effectivePinPolicy() {
            return effectivePinPolicy;
        }

        /**
         * Returns the fallback reason, when this row carries one.
         */
        public Optional<Reason> reason() {
            return Optional.ofNullable(reason);
        }

        /**
         * Returns a human-readable explanation, or an empty string.
         */
        public String explanation() {
            return explanation;
        }
    }

    /**
     * Decision row for one logical edge.
     */
    public static final class Edge {
        private final String key;
        private final String from;
        private final String to;
        private final EdgeSpec.EdgeKind declaredKind;
        private final EdgeSpec.EdgeKind effectiveKind;
        private final EdgeUseKind use;
        private final String allocationOwner;
        private final boolean sourceIngressCloseGuard;
        private final boolean redirectOnly;
        private final Reason reason;
        private final String explanation;

        /**
         * Creates an edge decision row.
         */
        public Edge(
            final String from,
            final String to,
            final EdgeSpec.EdgeKind declaredKind,
            final EdgeSpec.EdgeKind effectiveKind,
            final EdgeUseKind use,
            final String allocationOwner,
            final boolean sourceIngressCloseGuard,
            final boolean redirectOnly,
            final Reason reason,
            final String explanation
        ) {
            this.from = requireName(from, "edge source");
            this.to = requireName(to, "edge target");
            this.key = this.from + "->" + this.to;
            this.declaredKind = Objects.requireNonNull(declaredKind, "declaredKind");
            this.effectiveKind = Objects.requireNonNull(effectiveKind, "effectiveKind");
            this.use = Objects.requireNonNull(use, "use");
            this.allocationOwner = allocationOwner == null ? "" : allocationOwner;
            this.sourceIngressCloseGuard = sourceIngressCloseGuard;
            this.redirectOnly = redirectOnly;
            this.reason = reason;
            this.explanation = explanation == null ? "" : explanation;
        }

        /**
         * Returns the edge key in {@code from->to} form.
         */
        public String key() {
            return key;
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
         * Returns the declared edge kind before compiler specialization.
         */
        public EdgeSpec.EdgeKind declaredKind() {
            return declaredKind;
        }

        /**
         * Returns the effective runtime edge kind.
         */
        public EdgeSpec.EdgeKind effectiveKind() {
            return effectiveKind;
        }

        /**
         * Returns how the runtime uses this edge.
         */
        public EdgeUseKind use() {
            return use;
        }

        /**
         * Returns the worker or subsystem that owns allocation for this edge.
         */
        public String allocationOwner() {
            return allocationOwner;
        }

        /**
         * Returns whether this source ingress uses the SPSC close guard.
         */
        public boolean sourceIngressCloseGuard() {
            return sourceIngressCloseGuard;
        }

        /**
         * Returns whether this edge exists only as a redirect target.
         */
        public boolean redirectOnly() {
            return redirectOnly;
        }

        /**
         * Returns the fallback or specialization reason, when present.
         */
        public Optional<Reason> reason() {
            return Optional.ofNullable(reason);
        }

        /**
         * Returns a human-readable explanation, or an empty string.
         */
        public String explanation() {
            return explanation;
        }
    }

    /**
     * Decision row for one runtime sender.
     */
    public static final class Sender {
        private final String ownerName;
        private final String edgeKey;
        private final SenderKind kind;
        private final String redirectEdgeKey;
        private final OutputKind output;
        private final Reason reason;
        private final String explanation;

        /**
         * Creates a sender decision row.
         */
        public Sender(
            final String ownerName,
            final String edgeKey,
            final SenderKind kind,
            final String redirectEdgeKey,
            final OutputKind output,
            final Reason reason,
            final String explanation
        ) {
            this.ownerName = requireName(ownerName, "sender owner");
            this.edgeKey = requireName(edgeKey, "sender edge");
            this.kind = Objects.requireNonNull(kind, "kind");
            this.redirectEdgeKey = redirectEdgeKey == null ? "" : redirectEdgeKey;
            this.output = Objects.requireNonNull(output, "output");
            this.reason = reason;
            this.explanation = explanation == null ? "" : explanation;
        }

        /**
         * Returns the sender owner.
         */
        public String ownerName() {
            return ownerName;
        }

        /**
         * Returns the target edge key.
         */
        public String edgeKey() {
            return edgeKey;
        }

        /**
         * Returns the sender kind.
         */
        public SenderKind kind() {
            return kind;
        }

        /**
         * Returns the redirect edge key, or an empty string.
         */
        public String redirectEdgeKey() {
            return redirectEdgeKey;
        }

        /**
         * Returns the output mode.
         */
        public OutputKind output() {
            return output;
        }

        /**
         * Returns the fallback reason, when present.
         */
        public Optional<Reason> reason() {
            return Optional.ofNullable(reason);
        }

        /**
         * Returns a human-readable explanation, or an empty string.
         */
        public String explanation() {
            return explanation;
        }
    }

    /**
     * Positive merge decision.
     */
    public static final class Merge {
        private final MergeKind kind;
        private final String ownerName;
        private final List<String> mergedNodes;
        private final List<String> elidedEdges;
        private final String terminalNode;
        private final Reason reason;
        private final String explanation;

        /**
         * Creates a merge row.
         */
        public Merge(
            final MergeKind kind,
            final String ownerName,
            final List<String> mergedNodes,
            final List<String> elidedEdges,
            final String terminalNode,
            final Reason reason,
            final String explanation
        ) {
            this.kind = Objects.requireNonNull(kind, "kind");
            this.ownerName = requireName(ownerName, "merge owner");
            this.mergedNodes = List.copyOf(mergedNodes);
            this.elidedEdges = List.copyOf(elidedEdges);
            this.terminalNode = terminalNode == null ? "" : terminalNode;
            this.reason = reason;
            this.explanation = explanation == null ? "" : explanation;
        }

        /**
         * Returns the merge kind.
         */
        public MergeKind kind() {
            return kind;
        }

        /**
         * Returns the owner worker name.
         */
        public String ownerName() {
            return ownerName;
        }

        /**
         * Returns nodes merged into the owner. The returned list is immutable.
         */
        public List<String> mergedNodes() {
            return mergedNodes;
        }

        /**
         * Returns edge keys elided by the merge. The returned list is immutable.
         */
        public List<String> elidedEdges() {
            return elidedEdges;
        }

        /**
         * Returns the terminal node involved in the merge, or an empty string.
         */
        public String terminalNode() {
            return terminalNode;
        }

        /**
         * Returns the positive decision reason, when present.
         */
        public Optional<Reason> reason() {
            return Optional.ofNullable(reason);
        }

        /**
         * Returns a human-readable explanation, or an empty string.
         */
        public String explanation() {
            return explanation;
        }
    }

    /**
     * Explanation for a merge candidate or optimization that did not apply.
     */
    public static final class Fallback {
        private final SubjectKind subjectKind;
        private final String subject;
        private final Reason reason;
        private final String explanation;

        /**
         * Creates a fallback row.
         */
        public Fallback(
            final SubjectKind subjectKind,
            final String subject,
            final Reason reason,
            final String explanation
        ) {
            this.subjectKind = Objects.requireNonNull(subjectKind, "subjectKind");
            this.subject = requireName(subject, "fallback subject");
            this.reason = Objects.requireNonNull(reason, "reason");
            this.explanation = explanation == null ? reason.defaultExplanation() : explanation;
        }

        /**
         * Returns the subject kind.
         */
        public SubjectKind subjectKind() {
            return subjectKind;
        }

        /**
         * Returns the graph, node, source, router, or edge subject.
         */
        public String subject() {
            return subject;
        }

        /**
         * Returns the fallback reason.
         */
        public Reason reason() {
            return reason;
        }

        /**
         * Returns the stable reason code.
         */
        public String reasonCode() {
            return reason.code();
        }

        /**
         * Returns the human-readable explanation.
         */
        public String explanation() {
            return explanation;
        }
    }

    /**
     * Fallback subject categories.
     */
    public enum SubjectKind {
        /**
         * Whole-graph subject.
         */
        GRAPH,
        /**
         * Logical node subject.
         */
        NODE,
        /**
         * Logical edge subject.
         */
        EDGE,
        /**
         * Source node subject.
         */
        SOURCE,
        /**
         * Router node subject.
         */
        ROUTER
    }

    /**
     * Executable operator categories.
     */
    public enum OperatorKind {
        SOURCE,
        STAGE,
        BATCH_STAGE,
        SINK,
        DISPATCH,
        BROADCAST,
        PARTITION,
        JOIN;

        private static OperatorKind from(final GraphPlan.NodeKind kind, final boolean batchStage) {
            return switch (kind) {
                case SOURCE -> SOURCE;
                case STAGE -> batchStage ? BATCH_STAGE : STAGE;
                case SINK -> SINK;
                case DISPATCH -> DISPATCH;
                case BROADCAST -> BROADCAST;
                case PARTITION -> PARTITION;
                case JOIN -> JOIN;
            };
        }
    }

    /**
     * Worker decision categories.
     */
    public enum WorkerDecisionKind {
        RUNNABLE,
        FUSED_INTO_STAGE,
        FUSED_INTO_SINK,
        INLINE_SOURCE_CHAIN,
        LIFECYCLE_ONLY
    }

    /**
     * Output strategy categories.
     */
    public enum OutputKind {
        NONE,
        EDGE,
        DIRECT_STAGE,
        DIRECT_SINK,
        DIRECT_ROUTER,
        LINEAR_FUSED
    }

    /**
     * Runtime edge-use categories.
     */
    public enum EdgeUseKind {
        NORMAL,
        REDIRECT_ONLY,
        ELIDED_FUSED_STAGE,
        ELIDED_FUSED_SINK,
        ELIDED_FUSED_ROUTER,
        ELIDED_INLINE_SOURCE
    }

    /**
     * Runtime sender categories.
     */
    public enum SenderKind {
        /**
         * Message is published into a physical edge.
         */
        EDGE,
        /**
         * Message is published into a physical edge with a redirect edge.
         */
        EDGE_WITH_REDIRECT,
        /**
         * Message is delivered by direct stage invocation inside a fused chain.
         */
        DIRECT_STAGE,
        /**
         * Message is delivered by direct sink invocation.
         */
        DIRECT_SINK,
        /**
         * Message is delivered by direct router invocation.
         */
        DIRECT_ROUTER,
        /**
         * Source emit may enter a fused chain inline on the producer thread.
         */
        INLINE_ENTRY
    }

    /**
     * Positive merge categories.
     */
    public enum MergeKind {
        STAGE_CHAIN,
        STAGE_TO_SINK,
        JOIN_TO_SINK,
        STAGE_TO_ROUTER,
        INLINE_SOURCE_CHAIN
    }

    /**
     * Curated compilation decision reasons.
     * <p>
     * New reasons may be added in future minor releases; consumers switching
     * on this enum should keep a default branch.
     */
    public enum Reason {
        FUSION_DISABLED("fusion.disabled", "Graph fusion is disabled for this graph."),
        CUSTOM_EXCEPTION_HANDLER("source_inline.custom_exception_handler",
            "Source-inline fusion is disabled when a custom exception handler is installed."),
        REQUIRES_WORKER_PLACEMENT("fusion.requires_worker_placement",
            "The candidate requires worker placement that cannot be preserved by this merge."),
        CONFLICTING_EXPLICIT_PINS("fusion.conflicting_explicit_pins",
            "Both candidate workers requested explicit placement."),
        NON_LINEAR_TOPOLOGY("fusion.non_linear_topology",
            "The candidate is not a single-input/single-output linear segment."),
        NON_FUSIBLE_EDGE("fusion.non_fusible_edge",
            "The candidate edge does not satisfy fusion requirements."),
        NON_FUSIBLE_EDGE_KIND("fusion.non_fusible_edge.kind",
            "Fusion requires an SPSC edge."),
        NON_FUSIBLE_EDGE_OVERFLOW("fusion.non_fusible_edge.overflow",
            "Fusion requires blocking overflow."),
        NON_FUSIBLE_EDGE_MEMORY("fusion.non_fusible_edge.memory",
            "Fusion requires on-heap edge slots."),
        NON_FUSIBLE_EDGE_BATCH("fusion.non_fusible_edge.batch",
            "Fusion requires edge batching to be disabled."),
        NON_FUSIBLE_STAGE("fusion.non_fusible_stage",
            "The stage cannot be directly invoked in a fused chain."),
        BATCH_STAGE("fusion.batch_stage", "Batch stages cannot be fused into a stage chain."),
        MULTI_PRODUCER_SOURCE("source_inline.multi_producer_source",
            "Source-inline fusion requires a single-producer source."),
        STAMPED_SOURCE("source_inline.stamped_source",
            "Stamped sources cannot use source-inline fusion."),
        PREALLOCATED_SOURCE("source_inline.preallocated_source",
            "Preallocated sources keep their physical ownership path."),
        HANDLE_CAPABLE_PAYLOAD("source_inline.handle_capable_payload",
            "Handle-capable payloads require the retaining path."),
        FAN_OUT_SOURCE("source_inline.fan_out_source",
            "Source-inline fusion requires one normal outgoing source edge."),
        SOURCE_SPECIALIZED_TO_SPSC("source.specialized_to_spsc",
            "Single-producer source ingress was specialized to SPSC.");

        private final String code;
        private final String defaultExplanation;

        Reason(final String code, final String defaultExplanation) {
            this.code = code;
            this.defaultExplanation = defaultExplanation;
        }

        /**
         * Returns the stable machine-readable reason code.
         */
        public String code() {
            return code;
        }

        /**
         * Returns the default human-readable explanation.
         */
        public String defaultExplanation() {
            return defaultExplanation;
        }

        /**
         * Finds a reason by stable code.
         */
        public static Optional<Reason> fromCode(final String code) {
            for (final Reason reason : values()) {
                if (reason.code.equals(code)) {
                    return Optional.of(reason);
                }
            }
            return Optional.empty();
        }
    }
}
