package io.github.elevateddev.lattice.internal.runtime;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.GraphCompilationReport;
import io.github.elevateddev.lattice.graph.GraphPlan;
import io.github.elevateddev.lattice.internal.graph.CompiledGraph;
import io.github.elevateddev.lattice.internal.graph.EdgeDefinition;
import io.github.elevateddev.lattice.internal.graph.NodeDefinition;
import io.github.elevateddev.lattice.placement.PinPolicy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class GraphCompilationReports {

    private GraphCompilationReports() {
    }

    static GraphCompilationReport from(final CompiledGraph compiled, final PhysicalPlan physical) {
        return from(compiled, physical, Map.of());
    }

    static GraphCompilationReport from(
        final CompiledGraph compiled,
        final PhysicalPlan physical,
        final Map<String, PinPolicy> topologyAwarePins
    ) {
        return new GraphCompilationReport(
            compiled.plan().name(),
            controls(compiled),
            workers(compiled, physical, topologyAwarePins),
            edges(compiled, physical),
            senders(compiled, physical),
            merges(compiled, physical),
            fallbacks(compiled, physical)
        );
    }

    private static GraphCompilationReport.Controls controls(final CompiledGraph compiled) {
        return new GraphCompilationReport.Controls(
            compiled.runtimeConfig().fusion().enabled(),
            compiled.runtimeConfig().fusion().inlineSources(),
            compiled.runtimeConfig().fusion().elideInlineSourcePhysicalPath(),
            compiled.runtimeConfig().placement().topologyAware(),
            compiled.customExceptionHandler()
        );
    }

    private static List<GraphCompilationReport.Worker> workers(
        final CompiledGraph compiled,
        final PhysicalPlan physical,
        final Map<String, PinPolicy> topologyAwarePins
    ) {
        final List<GraphCompilationReport.Worker> rows = new ArrayList<>();
        for (final String workerName : compiled.workerOrder()) {
            final WorkerDecision decision = physical.workerDecisions().get(workerName);
            final NodeDefinition node = compiled.nodes().get(workerName);
            if (decision == null || node == null) {
                continue;
            }
            final GraphCompilationReport.Reason reason = reason(decision.fallbackReason());
            rows.add(new GraphCompilationReport.Worker(
                workerName,
                node.kind(),
                operatorKind(decision.operatorKind()),
                workerDecisionKind(decision.workerKind()),
                outputKind(decision.outputKind()),
                decision.fusedOwnerName(),
                effectivePinPolicy(decision, topologyAwarePins),
                reason,
                explanation(reason, decision.fallbackReason())
            ));
        }
        return rows;
    }

    private static List<GraphCompilationReport.Edge> edges(
        final CompiledGraph compiled,
        final PhysicalPlan physical
    ) {
        final List<GraphCompilationReport.Edge> rows = new ArrayList<>();
        for (final EdgeDefinition edge : compiled.edges()) {
            final EdgeDecision decision = physical.edgeDecision(edge.key());
            final GraphCompilationReport.Reason fallbackReason = decision == null
                ? null
                : reason(decision.fallbackReason());
            final GraphCompilationReport.Reason reason = specialized(edge)
                ? GraphCompilationReport.Reason.SOURCE_SPECIALIZED_TO_SPSC
                : fallbackReason;
            rows.add(new GraphCompilationReport.Edge(
                edge.from(),
                edge.to(),
                edge.declaredKind(),
                decision == null ? edge.spec().kind() : edgeKind(decision.implementationKind()),
                decision == null ? GraphCompilationReport.EdgeUseKind.NORMAL : edgeUseKind(decision.useKind()),
                decision == null ? allocationOwner(compiled, edge) : decision.allocationOwner(),
                decision != null && decision.sourceIngressCloseGuard(),
                edge.redirectOnly(),
                reason,
                edgeExplanation(edge, reason, decision == null ? null : decision.fallbackReason())
            ));
        }
        return rows;
    }

    private static List<GraphCompilationReport.Sender> senders(
        final CompiledGraph compiled,
        final PhysicalPlan physical
    ) {
        final List<GraphCompilationReport.Sender> rows = new ArrayList<>();
        final Map<String, List<GraphCompilationReport.Sender>> directSenders = directSenders(physical);
        for (final EdgeDefinition edge : compiled.edges()) {
            final SenderDecision decision = physical.senderDecisions().get(edge.key());
            if (decision != null) {
                final GraphCompilationReport.Reason reason = reason(decision.fallbackReason());
                rows.add(new GraphCompilationReport.Sender(
                    decision.ownerName(),
                    decision.edgeKey(),
                    senderKind(decision.senderKind()),
                    decision.redirectEdgeKey(),
                    outputKind(decision.outputKind()),
                    reason,
                    explanation(reason, decision.fallbackReason())
                ));
            }
            rows.addAll(directSenders.getOrDefault(edge.key(), List.of()));
        }
        return rows;
    }

    private static Map<String, List<GraphCompilationReport.Sender>> directSenders(final PhysicalPlan physical) {
        final Map<String, List<GraphCompilationReport.Sender>> rows = new LinkedHashMap<>();
        for (final FusedStagePlan fusedStage : physical.fusedStages().values()) {
            for (final EdgeDefinition edge : fusedStage.stageInputEdges()) {
                addDirectSender(
                    rows,
                    edge.key(),
                    fusedStage.ownerName(),
                    GraphCompilationReport.SenderKind.DIRECT_STAGE,
                    GraphCompilationReport.OutputKind.DIRECT_STAGE,
                    "Logical edge is invoked directly inside fused stage chain owned by "
                        + fusedStage.ownerName() + "."
                );
            }
            if (fusedStage.sinkPlan() != null) {
                addDirectSender(
                    rows,
                    fusedStage.sinkPlan().edge().key(),
                    fusedStage.ownerName(),
                    GraphCompilationReport.SenderKind.DIRECT_SINK,
                    GraphCompilationReport.OutputKind.DIRECT_SINK,
                    "Terminal sink is invoked directly inside fused stage chain owned by "
                        + fusedStage.ownerName() + "."
                );
            }
        }
        for (final FusedSinkPlan fusedSink : physical.fusedSinks().values()) {
            addDirectSender(
                rows,
                fusedSink.edge().key(),
                fusedSink.stageName(),
                GraphCompilationReport.SenderKind.DIRECT_SINK,
                GraphCompilationReport.OutputKind.DIRECT_SINK,
                "Terminal sink is invoked directly by " + fusedSink.stageName() + "."
            );
        }
        for (final FusedRouterPlan fusedRouter : physical.fusedRouters().values()) {
            addDirectSender(
                rows,
                fusedRouter.inputEdge().key(),
                fusedRouter.ownerName(),
                GraphCompilationReport.SenderKind.DIRECT_ROUTER,
                GraphCompilationReport.OutputKind.DIRECT_ROUTER,
                "Router is invoked directly by " + fusedRouter.ownerName() + "."
            );
        }
        for (final InlineSourceBinding binding : physical.inlineSourceBindings().values()) {
            final WorkerDecision decision = physical.workerDecisions().get(binding.workerName());
            addDirectSender(
                rows,
                binding.edgeKey(),
                binding.sourceName(),
                GraphCompilationReport.SenderKind.INLINE_ENTRY,
                decision == null
                    ? GraphCompilationReport.OutputKind.LINEAR_FUSED
                    : outputKind(decision.outputKind()),
                "Source may enter the fused chain inline at emit time."
            );
        }
        return Map.copyOf(rows);
    }

    private static void addDirectSender(
        final Map<String, List<GraphCompilationReport.Sender>> rows,
        final String edgeKey,
        final String ownerName,
        final GraphCompilationReport.SenderKind kind,
        final GraphCompilationReport.OutputKind output,
        final String explanation
    ) {
        rows.computeIfAbsent(edgeKey, ignored -> new ArrayList<>()).add(new GraphCompilationReport.Sender(
            ownerName,
            edgeKey,
            kind,
            "",
            output,
            null,
            explanation
        ));
    }

    private static List<GraphCompilationReport.Merge> merges(
        final CompiledGraph compiled,
        final PhysicalPlan physical
    ) {
        final List<GraphCompilationReport.Merge> rows = new ArrayList<>();
        for (final String workerName : compiled.workerOrder()) {
            final FusedStagePlan fusedStage = physical.fusedStage(workerName);
            if (fusedStage != null) {
                final List<String> mergedNodes = new ArrayList<>(fusedStage.stageNames());
                String terminal = "";
                if (fusedStage.sinkPlan() != null) {
                    mergedNodes.add(fusedStage.sinkPlan().sinkName());
                    terminal = fusedStage.sinkPlan().sinkName();
                }
                rows.add(new GraphCompilationReport.Merge(
                    GraphCompilationReport.MergeKind.STAGE_CHAIN,
                    workerName,
                    mergedNodes,
                    fusedStage.elidedEdges().stream().map(EdgeDefinition::key).toList(),
                    terminal,
                    null,
                    "Linear stage chain runs on " + workerName + "."
                ));
            }

            final FusedSinkPlan fusedSink = physical.fusedSink(workerName);
            if (fusedSink != null) {
                final NodeDefinition owner = compiled.nodes().get(workerName);
                rows.add(new GraphCompilationReport.Merge(
                    owner != null && owner.kind() == GraphPlan.NodeKind.JOIN
                        ? GraphCompilationReport.MergeKind.JOIN_TO_SINK
                        : GraphCompilationReport.MergeKind.STAGE_TO_SINK,
                    workerName,
                    List.of(fusedSink.sinkName()),
                    List.of(fusedSink.edge().key()),
                    fusedSink.sinkName(),
                    null,
                    "Terminal sink " + fusedSink.sinkName() + " runs on " + workerName + "."
                ));
            }

            final FusedRouterPlan fusedRouter = physical.fusedRouter(workerName);
            if (fusedRouter != null) {
                rows.add(new GraphCompilationReport.Merge(
                    GraphCompilationReport.MergeKind.STAGE_TO_ROUTER,
                    workerName,
                    List.of(fusedRouter.routerName()),
                    List.of(fusedRouter.inputEdge().key()),
                    fusedRouter.routerName(),
                    null,
                    "Router " + fusedRouter.routerName() + " runs on " + workerName + "."
                ));
            }

            final InlineSourceBinding binding = physical.inlineSourceBindings().get(workerName);
            if (binding != null) {
                final EdgeDecision edgeDecision = physical.edgeDecision(binding.edgeKey());
                final List<String> elidedEdges = edgeDecision != null
                    && edgeDecision.useKind() == EdgeUseKind.ELIDED_INLINE_SOURCE
                    ? List.of(binding.edgeKey())
                    : List.of();
                rows.add(new GraphCompilationReport.Merge(
                    GraphCompilationReport.MergeKind.INLINE_SOURCE_CHAIN,
                    workerName,
                    List.of(binding.sourceName()),
                    elidedEdges,
                    workerName,
                    null,
                    "Source " + binding.sourceName() + " may run the fused chain inline at emit time."
                ));
            }
        }
        return rows;
    }

    private static PinPolicy effectivePinPolicy(
        final WorkerDecision decision,
        final Map<String, PinPolicy> topologyAwarePins
    ) {
        final PinPolicy policy = decision.effectivePinPolicy();
        if (policy.kind() != PinPolicy.PinKind.NONE) {
            return policy;
        }
        final String ownerName = decision.fusedOwnerName() == null || decision.fusedOwnerName().isEmpty()
            ? decision.workerName()
            : decision.fusedOwnerName();
        return topologyAwarePins.getOrDefault(ownerName, policy);
    }

    private static List<GraphCompilationReport.Fallback> fallbacks(
        final CompiledGraph compiled,
        final PhysicalPlan physical
    ) {
        final List<GraphCompilationReport.Fallback> rows = new ArrayList<>();
        for (final PlanningFallback fallback : physical.fallbackReasons()) {
            final GraphCompilationReport.Reason reason = reason(fallback.reason());
            if (reason == null) {
                continue;
            }
            rows.add(new GraphCompilationReport.Fallback(
                subjectKind(compiled, fallback.subjectName()),
                fallback.subjectName(),
                reason,
                explanation(reason, fallback.reason())
            ));
        }
        return rows;
    }

    private static boolean specialized(final EdgeDefinition edge) {
        return edge.sourceIngress()
            && edge.declaredKind() == EdgeSpec.EdgeKind.MPSC_RING
            && edge.spec().kind() == EdgeSpec.EdgeKind.SPSC_RING;
    }

    private static String edgeExplanation(
        final EdgeDefinition edge,
        final GraphCompilationReport.Reason reason,
        final FallbackReason fallbackReason
    ) {
        if (reason == GraphCompilationReport.Reason.SOURCE_SPECIALIZED_TO_SPSC) {
            return "Source ingress specialized because " + edge.from() + " is SINGLE_PRODUCER.";
        }
        return explanation(reason, fallbackReason);
    }

    private static String explanation(
        final GraphCompilationReport.Reason reason,
        final FallbackReason fallbackReason
    ) {
        if (reason == null) {
            return "";
        }
        if (fallbackReason == null) {
            return reason.defaultExplanation();
        }
        return switch (fallbackReason) {
            case FUSION_DISABLED -> "Graph fusion is disabled for this graph.";
            case CUSTOM_EXCEPTION_HANDLER ->
                "Source-inline fusion is disabled when a custom exception handler is installed.";
            case CONFLICTING_EXPLICIT_PINS -> "Both candidate workers requested explicit placement.";
            case NON_FUSIBLE_STAGE -> "The stage cannot be directly invoked in a fused chain.";
            case BATCH_STAGE -> "Batch stages cannot be fused into a stage chain.";
            case NON_FUSIBLE_EDGE -> "The candidate edge does not satisfy fusion requirements.";
            case NON_FUSIBLE_EDGE_KIND -> "Stage-chain fusion requires an SPSC edge.";
            case NON_FUSIBLE_EDGE_OVERFLOW -> "Stage-chain fusion requires BLOCK overflow.";
            case NON_FUSIBLE_EDGE_MEMORY -> "Stage-chain fusion requires on-heap edge slots.";
            case NON_FUSIBLE_EDGE_BATCH -> "Stage-chain fusion requires edge batching to be disabled.";
            case NON_LINEAR_TOPOLOGY -> "The candidate is not a supported linear segment.";
            case MULTI_PRODUCER_SOURCE -> "Source-inline fusion requires SourceMode.SINGLE_PRODUCER.";
            case STAMPED_SOURCE -> "Stamped sources cannot use source-inline fusion.";
            case PREALLOCATED_SOURCE -> "Preallocated sources keep their physical ownership path.";
            case HANDLE_CAPABLE_PAYLOAD -> "Handle-capable payloads require the retaining path.";
            case FAN_OUT_SOURCE -> "Source-inline fusion requires one normal outgoing source edge.";
            case REQUIRES_WORKER_PLACEMENT ->
                "Source-inline fusion cannot move placed stage logic onto the producer thread.";
        };
    }

    private static GraphCompilationReport.SubjectKind subjectKind(
        final CompiledGraph compiled,
        final String subject
    ) {
        if (compiled.plan().name().equals(subject)) {
            return GraphCompilationReport.SubjectKind.GRAPH;
        }
        if (subject.contains("->")) {
            return GraphCompilationReport.SubjectKind.EDGE;
        }
        final NodeDefinition node = compiled.nodes().get(subject);
        if (node == null) {
            return GraphCompilationReport.SubjectKind.NODE;
        }
        return switch (node.kind()) {
            case SOURCE -> GraphCompilationReport.SubjectKind.SOURCE;
            case DISPATCH, BROADCAST, PARTITION -> GraphCompilationReport.SubjectKind.ROUTER;
            default -> GraphCompilationReport.SubjectKind.NODE;
        };
    }

    private static String allocationOwner(final CompiledGraph compiled, final EdgeDefinition edge) {
        final NodeDefinition from = compiled.nodes().get(edge.from());
        return from != null && from.kind() == GraphPlan.NodeKind.SOURCE ? edge.to() : edge.from();
    }

    private static EdgeSpec.EdgeKind edgeKind(final EdgeImplementationKind kind) {
        return kind == EdgeImplementationKind.SPSC_RING
            ? EdgeSpec.EdgeKind.SPSC_RING
            : EdgeSpec.EdgeKind.MPSC_RING;
    }

    private static GraphCompilationReport.WorkerDecisionKind workerDecisionKind(final WorkerKind kind) {
        return switch (kind) {
            case RUNNABLE -> GraphCompilationReport.WorkerDecisionKind.RUNNABLE;
            case FUSED_INTO_STAGE -> GraphCompilationReport.WorkerDecisionKind.FUSED_INTO_STAGE;
            case FUSED_INTO_SINK -> GraphCompilationReport.WorkerDecisionKind.FUSED_INTO_SINK;
            case INLINE_SOURCE_CHAIN -> GraphCompilationReport.WorkerDecisionKind.INLINE_SOURCE_CHAIN;
            case LIFECYCLE_ONLY -> GraphCompilationReport.WorkerDecisionKind.LIFECYCLE_ONLY;
        };
    }

    private static GraphCompilationReport.OperatorKind operatorKind(final OperatorKind kind) {
        return switch (kind) {
            case SOURCE -> GraphCompilationReport.OperatorKind.SOURCE;
            case STAGE -> GraphCompilationReport.OperatorKind.STAGE;
            case BATCH_STAGE -> GraphCompilationReport.OperatorKind.BATCH_STAGE;
            case SINK -> GraphCompilationReport.OperatorKind.SINK;
            case DISPATCH -> GraphCompilationReport.OperatorKind.DISPATCH;
            case BROADCAST -> GraphCompilationReport.OperatorKind.BROADCAST;
            case PARTITION -> GraphCompilationReport.OperatorKind.PARTITION;
            case JOIN -> GraphCompilationReport.OperatorKind.JOIN;
        };
    }

    private static GraphCompilationReport.OutputKind outputKind(final OutputKind kind) {
        return switch (kind) {
            case NONE -> GraphCompilationReport.OutputKind.NONE;
            case EDGE -> GraphCompilationReport.OutputKind.EDGE;
            case DIRECT_STAGE -> GraphCompilationReport.OutputKind.DIRECT_STAGE;
            case DIRECT_SINK -> GraphCompilationReport.OutputKind.DIRECT_SINK;
            case DIRECT_ROUTER -> GraphCompilationReport.OutputKind.DIRECT_ROUTER;
            case LINEAR_FUSED -> GraphCompilationReport.OutputKind.LINEAR_FUSED;
        };
    }

    private static GraphCompilationReport.EdgeUseKind edgeUseKind(final EdgeUseKind kind) {
        return switch (kind) {
            case NORMAL -> GraphCompilationReport.EdgeUseKind.NORMAL;
            case REDIRECT_ONLY -> GraphCompilationReport.EdgeUseKind.REDIRECT_ONLY;
            case ELIDED_FUSED_STAGE -> GraphCompilationReport.EdgeUseKind.ELIDED_FUSED_STAGE;
            case ELIDED_FUSED_SINK -> GraphCompilationReport.EdgeUseKind.ELIDED_FUSED_SINK;
            case ELIDED_FUSED_ROUTER -> GraphCompilationReport.EdgeUseKind.ELIDED_FUSED_ROUTER;
            case ELIDED_INLINE_SOURCE -> GraphCompilationReport.EdgeUseKind.ELIDED_INLINE_SOURCE;
        };
    }

    private static GraphCompilationReport.SenderKind senderKind(final SenderKind kind) {
        return switch (kind) {
            case EDGE -> GraphCompilationReport.SenderKind.EDGE;
            case EDGE_WITH_REDIRECT -> GraphCompilationReport.SenderKind.EDGE_WITH_REDIRECT;
            case DIRECT_STAGE -> GraphCompilationReport.SenderKind.DIRECT_STAGE;
            case DIRECT_SINK -> GraphCompilationReport.SenderKind.DIRECT_SINK;
            case DIRECT_ROUTER -> GraphCompilationReport.SenderKind.DIRECT_ROUTER;
            case INLINE_ENTRY -> GraphCompilationReport.SenderKind.INLINE_ENTRY;
        };
    }

    private static GraphCompilationReport.Reason reason(final FallbackReason reason) {
        if (reason == null) {
            return null;
        }
        return switch (reason) {
            case FUSION_DISABLED -> GraphCompilationReport.Reason.FUSION_DISABLED;
            case CUSTOM_EXCEPTION_HANDLER -> GraphCompilationReport.Reason.CUSTOM_EXCEPTION_HANDLER;
            case CONFLICTING_EXPLICIT_PINS -> GraphCompilationReport.Reason.CONFLICTING_EXPLICIT_PINS;
            case NON_FUSIBLE_STAGE -> GraphCompilationReport.Reason.NON_FUSIBLE_STAGE;
            case BATCH_STAGE -> GraphCompilationReport.Reason.BATCH_STAGE;
            case NON_FUSIBLE_EDGE -> GraphCompilationReport.Reason.NON_FUSIBLE_EDGE;
            case NON_FUSIBLE_EDGE_KIND -> GraphCompilationReport.Reason.NON_FUSIBLE_EDGE_KIND;
            case NON_FUSIBLE_EDGE_OVERFLOW -> GraphCompilationReport.Reason.NON_FUSIBLE_EDGE_OVERFLOW;
            case NON_FUSIBLE_EDGE_MEMORY -> GraphCompilationReport.Reason.NON_FUSIBLE_EDGE_MEMORY;
            case NON_FUSIBLE_EDGE_BATCH -> GraphCompilationReport.Reason.NON_FUSIBLE_EDGE_BATCH;
            case NON_LINEAR_TOPOLOGY -> GraphCompilationReport.Reason.NON_LINEAR_TOPOLOGY;
            case MULTI_PRODUCER_SOURCE -> GraphCompilationReport.Reason.MULTI_PRODUCER_SOURCE;
            case STAMPED_SOURCE -> GraphCompilationReport.Reason.STAMPED_SOURCE;
            case PREALLOCATED_SOURCE -> GraphCompilationReport.Reason.PREALLOCATED_SOURCE;
            case HANDLE_CAPABLE_PAYLOAD -> GraphCompilationReport.Reason.HANDLE_CAPABLE_PAYLOAD;
            case FAN_OUT_SOURCE -> GraphCompilationReport.Reason.FAN_OUT_SOURCE;
            case REQUIRES_WORKER_PLACEMENT -> GraphCompilationReport.Reason.REQUIRES_WORKER_PLACEMENT;
        };
    }
}
