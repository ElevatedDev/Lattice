package io.github.elevateddev.lattice.internal.runtime;

import io.github.elevateddev.lattice.internal.graph.EdgeDefinition;
import io.github.elevateddev.lattice.placement.PinPolicy;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

record PhysicalPlan(
        List<String> workerOrder,
        Map<String, FusedSinkPlan> fusedSinks,
        Map<String, FusedStagePlan> fusedStages,
        Map<String, FusedRouterPlan> fusedRouters,
        Set<String> elidedEdgeKeys,
        Map<String, WorkerDecision> workerDecisions,
        Map<String, EdgeDecision> edgeDecisions,
        Map<String, SenderDecision> senderDecisions,
        Map<String, InlineSourceBinding> inlineSourceBindings,
        List<PlanningFallback> fallbackReasons
) {
    FusedSinkPlan fusedSink(final String workerName) {
        return fusedSinks.get(workerName);
    }

    FusedStagePlan fusedStage(final String workerName) {
        return fusedStages.get(workerName);
    }

    FusedRouterPlan fusedRouter(final String workerName) {
        return fusedRouters.get(workerName);
    }

    EdgeDecision edgeDecision(final String edgeKey) {
        return edgeDecisions.get(edgeKey);
    }

    Map<String, String> inlineFusedWorkerToSource() {
        return inlineSourceBindings.values().stream()
            .collect(Collectors.toUnmodifiableMap(InlineSourceBinding::workerName, InlineSourceBinding::sourceName));
    }

    int lifecycleParticipantCount() {
        return workerOrder.size() + inlineOnlyWorkerCount();
    }

    private int inlineOnlyWorkerCount() {
        return (int) workerDecisions.values().stream()
            .filter(decision -> decision.workerKind() == WorkerKind.INLINE_SOURCE_CHAIN)
            .count();
    }
}

enum WorkerKind {
    RUNNABLE,
    FUSED_INTO_STAGE,
    FUSED_INTO_SINK,
    INLINE_SOURCE_CHAIN,
    LIFECYCLE_ONLY
}

enum OperatorKind {
    SOURCE,
    STAGE,
    BATCH_STAGE,
    SINK,
    DISPATCH,
    BROADCAST,
    PARTITION,
    JOIN
}

enum OutputKind {
    NONE,
    EDGE,
    DIRECT_STAGE,
    DIRECT_SINK,
    DIRECT_ROUTER,
    LINEAR_FUSED
}

enum EdgeImplementationKind {
    SPSC_RING,
    MPSC_RING
}

enum EdgeUseKind {
    NORMAL,
    REDIRECT_ONLY,
    ELIDED_FUSED_STAGE,
    ELIDED_FUSED_SINK,
    ELIDED_FUSED_ROUTER,
    ELIDED_INLINE_SOURCE
}

enum SenderKind {
    EDGE,
    EDGE_WITH_REDIRECT,
    DIRECT_STAGE,
    DIRECT_SINK,
    DIRECT_ROUTER,
    INLINE_ENTRY
}

enum FallbackReason {
    FUSION_DISABLED,
    CUSTOM_EXCEPTION_HANDLER,
    CONFLICTING_EXPLICIT_PINS,
    NON_FUSIBLE_STAGE,
    NON_FUSIBLE_EDGE,
    NON_FUSIBLE_EDGE_KIND,
    NON_FUSIBLE_EDGE_OVERFLOW,
    NON_FUSIBLE_EDGE_MEMORY,
    NON_FUSIBLE_EDGE_BATCH,
    BATCH_STAGE,
    NON_LINEAR_TOPOLOGY,
    MULTI_PRODUCER_SOURCE,
    STAMPED_SOURCE,
    PREALLOCATED_SOURCE,
    HANDLE_CAPABLE_PAYLOAD,
    FAN_OUT_SOURCE,
    REQUIRES_WORKER_PLACEMENT
}

record WorkerDecision(
        String workerName,
        WorkerKind workerKind,
        OperatorKind operatorKind,
        OutputKind outputKind,
        PinPolicy effectivePinPolicy,
        String fusedOwnerName,
        FallbackReason fallbackReason
) {
}

record EdgeDecision(
        String edgeKey,
        EdgeImplementationKind implementationKind,
        EdgeUseKind useKind,
        String allocationOwner,
        boolean sourceIngressCloseGuard,
        FallbackReason fallbackReason
) {
}

record SenderDecision(
        String ownerName,
        String edgeKey,
        SenderKind senderKind,
        String redirectEdgeKey,
        OutputKind outputKind,
        FallbackReason fallbackReason
) {
}

record InlineSourceBinding(
        String workerName,
        String sourceName,
        String edgeKey
) {
}

record PlanningFallback(
        String subjectName,
        FallbackReason reason
) {
}

record FusedSinkPlan(
        String stageName,
        String sinkName,
        EdgeDefinition edge,
        PinPolicy effectivePinPolicy
) {
}

record FusedStagePlan(
        String ownerName,
        List<String> stageNames,
        List<EdgeDefinition> stageInputEdges,
        List<EdgeDefinition> elidedEdges,
        List<EdgeDefinition> downstreamOutgoing,
        FusedSinkPlan sinkPlan,
        PinPolicy effectivePinPolicy
) {
    String firstStageName() {
        return stageNames.get(0);
    }

    EdgeDefinition firstEdge() {
        return stageInputEdges.get(0);
    }
}

record FusedRouterPlan(
        String ownerName,
        String routerName,
        EdgeDefinition inputEdge,
        List<EdgeDefinition> outgoingEdges,
        PinPolicy effectivePinPolicy
) {
}
