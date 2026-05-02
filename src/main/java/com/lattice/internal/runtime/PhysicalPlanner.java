package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.FusionSpec;
import com.lattice.graph.GraphPlan;
import com.lattice.graph.SourceMode;
import com.lattice.internal.graph.CompiledGraph;
import com.lattice.internal.graph.EdgeDefinition;
import com.lattice.internal.graph.NodeDefinition;
import com.lattice.placement.MemoryMode;
import com.lattice.placement.PinPolicy;
import com.lattice.routing.Stamped;
import com.lattice.slab.SlabHandle;
import com.lattice.stage.BatchPolicy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class PhysicalPlanner {

    private PhysicalPlanner() {
    }

    static PhysicalPlan plan(final CompiledGraph compiled) {
        final List<PlanningFallback> fallbackReasons = new ArrayList<>();
        final FusionSpec fusion = compiled.runtimeConfig().fusion();
        if (!fusion.enabled()) {
            fallbackReasons.add(new PlanningFallback(compiled.plan().name(), FallbackReason.FUSION_DISABLED));
            return physicalPlan(compiled, compiled.workerOrder(), Map.of(), Map.of(), Map.of(), Set.of(), Map.of(), fallbackReasons);
        }

        final Map<String, FusedSinkPlan> fusedSinks = new LinkedHashMap<>();
        final Map<String, FusedStagePlan> fusedStages = new LinkedHashMap<>();
        final Map<String, FusedRouterPlan> fusedRouters = new LinkedHashMap<>();
        final Set<String> skippedWorkers = new HashSet<>();
        final Set<String> elidedEdges = new HashSet<>();
        for (final String workerName : compiled.workerOrder()) {
            if (skippedWorkers.contains(workerName)) {
                continue;
            }
            final FusedStagePlan stagePlan = fusedStagePlan(compiled, workerName, fallbackReasons);
            if (stagePlan != null) {
                fusedStages.put(workerName, stagePlan);
                skippedWorkers.addAll(stagePlan.stageNames());
                for (final EdgeDefinition edge : stagePlan.elidedEdges()) {
                    elidedEdges.add(edge.key());
                }
                if (stagePlan.sinkPlan() != null) {
                    skippedWorkers.add(stagePlan.sinkPlan().sinkName());
                    elidedEdges.add(stagePlan.sinkPlan().edge().key());
                }
                continue;
            }
            final FusedSinkPlan plan = fusedSinkPlan(compiled, workerName, fallbackReasons);
            if (plan != null) {
                fusedSinks.put(workerName, plan);
                skippedWorkers.add(plan.sinkName());
                elidedEdges.add(plan.edge().key());
                continue;
            }
            final FusedRouterPlan routerPlan = fusedRouterPlan(compiled, workerName, fallbackReasons);
            if (routerPlan != null) {
                fusedRouters.put(workerName, routerPlan);
                skippedWorkers.add(routerPlan.routerName());
                elidedEdges.add(routerPlan.inputEdge().key());
            }
        }

        if (fusedSinks.isEmpty() && fusedStages.isEmpty() && fusedRouters.isEmpty()) {
            return physicalPlan(compiled, compiled.workerOrder(), Map.of(), Map.of(), Map.of(), Set.of(), Map.of(), fallbackReasons);
        }

        // Inline source-side fusion: when explicitly enabled via system property, identify
        // fused-stage / fused-sink workers whose only input is a single-producer source
        // emitting a non-handle, non-stamped payload over a fusible BLOCK SPSC edge. Such
        // workers can either keep their lifecycle-only parked thread (the default) or be
        // fully represented as inline lifecycle participants when the physical elision
        // rollout flag is enabled.
        final Map<String, InlineSourceBinding> inlineFused = inlineFusedWorkerToSource(
                compiled,
                fusedStages,
                fusedSinks,
                elidedEdges,
                fallbackReasons
        );
        final boolean elideInlinePhysical = fusion.elideInlineSourcePhysicalPath();
        final Set<String> inlineWorkers = elideInlinePhysical
                ? Set.copyOf(inlineFused.keySet())
                : Set.of();
        final Set<String> runtimeElidedEdges = new HashSet<>(elidedEdges);
        if (elideInlinePhysical) {
            for (final InlineSourceBinding binding : inlineFused.values()) {
                runtimeElidedEdges.add(binding.edgeKey());
            }
        }
        final List<String> actualWorkers = compiled.workerOrder().stream()
                .filter(worker -> !skippedWorkers.contains(worker))
                .filter(worker -> !inlineWorkers.contains(worker))
                .toList();
        return physicalPlan(
                compiled,
                actualWorkers,
                Map.copyOf(fusedSinks),
                Map.copyOf(fusedStages),
                Map.copyOf(fusedRouters),
                Set.copyOf(runtimeElidedEdges),
                Map.copyOf(inlineFused),
                fallbackReasons
        );
    }

    private static PhysicalPlan physicalPlan(
            final CompiledGraph compiled,
            final List<String> workerOrder,
            final Map<String, FusedSinkPlan> fusedSinks,
            final Map<String, FusedStagePlan> fusedStages,
            final Map<String, FusedRouterPlan> fusedRouters,
            final Set<String> elidedEdges,
            final Map<String, InlineSourceBinding> inlineBindings,
            final List<PlanningFallback> fallbackReasons
    ) {
        return new PhysicalPlan(
                List.copyOf(workerOrder),
                Map.copyOf(fusedSinks),
                Map.copyOf(fusedStages),
                Map.copyOf(fusedRouters),
                Set.copyOf(elidedEdges),
                Map.copyOf(workerDecisions(compiled, workerOrder, fusedSinks, fusedStages, fusedRouters, inlineBindings)),
                Map.copyOf(edgeDecisions(compiled, fusedSinks, fusedStages, fusedRouters, elidedEdges, inlineBindings)),
                Map.copyOf(senderDecisions(compiled, fusedRouters, elidedEdges)),
                Map.copyOf(inlineBindings),
                List.copyOf(fallbackReasons)
        );
    }

    private static Map<String, WorkerDecision> workerDecisions(
            final CompiledGraph compiled,
            final List<String> workerOrder,
            final Map<String, FusedSinkPlan> fusedSinks,
            final Map<String, FusedStagePlan> fusedStages,
            final Map<String, FusedRouterPlan> fusedRouters,
            final Map<String, InlineSourceBinding> inlineBindings
    ) {
        final Set<String> runnable = Set.copyOf(workerOrder);
        final Map<String, WorkerDecision> decisions = new LinkedHashMap<>();
        for (final String workerName : compiled.workerOrder()) {
            final NodeDefinition node = compiled.nodes().get(workerName);
            if (node == null) {
                continue;
            }
            final FusedSinkPlan fusedSink = fusedSinks.get(workerName);
            final FusedStagePlan fusedStage = fusedStages.get(workerName);
            final FusedRouterPlan fusedRouter = fusedRouters.get(workerName);
            if (inlineBindings.containsKey(workerName) && !runnable.contains(workerName)) {
                decisions.put(workerName, new WorkerDecision(
                        workerName,
                        WorkerKind.INLINE_SOURCE_CHAIN,
                        operatorKind(node),
                        outputKind(compiled, workerName, fusedSink, fusedStage, fusedRouter),
                        effectivePinPolicy(node, fusedSink, fusedStage, fusedRouter),
                        null,
                        null
                ));
                continue;
            }
            if (runnable.contains(workerName)) {
                decisions.put(workerName, new WorkerDecision(
                        workerName,
                        WorkerKind.RUNNABLE,
                        operatorKind(node),
                        outputKind(compiled, workerName, fusedSink, fusedStage, fusedRouter),
                        effectivePinPolicy(node, fusedSink, fusedStage, fusedRouter),
                        null,
                        null
                ));
            } else {
                decisions.put(workerName, fusedWorkerDecision(compiled, node, fusedStages, fusedSinks, fusedRouters));
            }
        }
        return decisions;
    }

    private static WorkerDecision fusedWorkerDecision(
            final CompiledGraph compiled,
            final NodeDefinition node,
            final Map<String, FusedStagePlan> fusedStages,
            final Map<String, FusedSinkPlan> fusedSinks,
            final Map<String, FusedRouterPlan> fusedRouters
    ) {
        for (final Map.Entry<String, FusedStagePlan> entry : fusedStages.entrySet()) {
            final FusedStagePlan stagePlan = entry.getValue();
            if (stagePlan.stageNames().contains(node.name())) {
                return new WorkerDecision(
                        node.name(),
                        WorkerKind.FUSED_INTO_STAGE,
                        operatorKind(node),
                        OutputKind.DIRECT_STAGE,
                        node.spec().pinPolicy(),
                        entry.getKey(),
                        null
                );
            }
            if (stagePlan.sinkPlan() != null && stagePlan.sinkPlan().sinkName().equals(node.name())) {
                return new WorkerDecision(
                        node.name(),
                        WorkerKind.FUSED_INTO_STAGE,
                        operatorKind(node),
                        OutputKind.NONE,
                        node.spec().pinPolicy(),
                        entry.getKey(),
                        null
                );
            }
        }
        for (final Map.Entry<String, FusedSinkPlan> entry : fusedSinks.entrySet()) {
            if (entry.getValue().sinkName().equals(node.name())) {
                return new WorkerDecision(
                        node.name(),
                        WorkerKind.FUSED_INTO_SINK,
                        operatorKind(node),
                        OutputKind.NONE,
                        node.spec().pinPolicy(),
                        entry.getKey(),
                        null
                );
            }
        }
        for (final Map.Entry<String, FusedRouterPlan> entry : fusedRouters.entrySet()) {
            if (entry.getValue().routerName().equals(node.name())) {
                return new WorkerDecision(
                        node.name(),
                        WorkerKind.FUSED_INTO_STAGE,
                        operatorKind(node),
                        OutputKind.DIRECT_ROUTER,
                        node.spec().pinPolicy(),
                        entry.getKey(),
                        null
                );
            }
        }
        return new WorkerDecision(
                node.name(),
                WorkerKind.RUNNABLE,
                operatorKind(node),
                outputKind(compiled, node.name(), null, null, null),
                node.spec().pinPolicy(),
                null,
                FallbackReason.NON_LINEAR_TOPOLOGY
        );
    }

    private static OutputKind outputKind(
            final CompiledGraph compiled,
            final String workerName,
            final FusedSinkPlan fusedSink,
            final FusedStagePlan fusedStage,
            final FusedRouterPlan fusedRouter
    ) {
        if (fusedStage != null) {
            return OutputKind.LINEAR_FUSED;
        }
        if (fusedRouter != null) {
            return OutputKind.DIRECT_ROUTER;
        }
        if (fusedSink != null) {
            return OutputKind.DIRECT_SINK;
        }
        return compiled.normalOutgoingBySource().getOrDefault(workerName, List.of()).isEmpty()
                ? OutputKind.NONE
                : OutputKind.EDGE;
    }

    private static PinPolicy effectivePinPolicy(
            final NodeDefinition node,
            final FusedSinkPlan fusedSink,
            final FusedStagePlan fusedStage,
            final FusedRouterPlan fusedRouter
    ) {
        if (fusedStage != null) {
            return fusedStage.effectivePinPolicy();
        }
        if (fusedRouter != null) {
            return fusedRouter.effectivePinPolicy();
        }
        if (fusedSink != null) {
            return fusedSink.effectivePinPolicy();
        }
        return node.spec().pinPolicy();
    }

    private static OperatorKind operatorKind(final NodeDefinition node) {
        return switch (node.kind()) {
            case SOURCE -> OperatorKind.SOURCE;
            case STAGE -> node.batchLogic() == null ? OperatorKind.STAGE : OperatorKind.BATCH_STAGE;
            case SINK -> OperatorKind.SINK;
            case DISPATCH -> OperatorKind.DISPATCH;
            case BROADCAST -> OperatorKind.BROADCAST;
            case PARTITION -> OperatorKind.PARTITION;
            case JOIN -> OperatorKind.JOIN;
        };
    }

    private static Map<String, EdgeDecision> edgeDecisions(
            final CompiledGraph compiled,
            final Map<String, FusedSinkPlan> fusedSinks,
            final Map<String, FusedStagePlan> fusedStages,
            final Map<String, FusedRouterPlan> fusedRouters,
            final Set<String> elidedEdges,
            final Map<String, InlineSourceBinding> inlineBindings
    ) {
        final Set<String> fusedSinkEdges = new HashSet<>();
        for (final FusedSinkPlan plan : fusedSinks.values()) {
            fusedSinkEdges.add(plan.edge().key());
        }
        for (final FusedStagePlan plan : fusedStages.values()) {
            if (plan.sinkPlan() != null) {
                fusedSinkEdges.add(plan.sinkPlan().edge().key());
            }
        }
        final Map<String, String> fusedRouterInputOwners = new LinkedHashMap<>();
        final Map<String, String> fusedRouterOutputOwners = new LinkedHashMap<>();
        for (final FusedRouterPlan plan : fusedRouters.values()) {
            fusedRouterInputOwners.put(plan.inputEdge().key(), plan.ownerName());
            for (final EdgeDefinition outgoing : plan.outgoingEdges()) {
                fusedRouterOutputOwners.put(outgoing.key(), plan.ownerName());
            }
        }
        final Set<String> inlineSourceEdges = new HashSet<>();
        for (final InlineSourceBinding binding : inlineBindings.values()) {
            if (elidedEdges.contains(binding.edgeKey())) {
                inlineSourceEdges.add(binding.edgeKey());
            }
        }
        final Map<String, EdgeDecision> decisions = new LinkedHashMap<>();
        for (final EdgeDefinition edge : compiled.edges()) {
            final EdgeUseKind useKind;
            if (inlineSourceEdges.contains(edge.key())) {
                useKind = EdgeUseKind.ELIDED_INLINE_SOURCE;
            } else if (fusedRouterInputOwners.containsKey(edge.key())) {
                useKind = EdgeUseKind.ELIDED_FUSED_ROUTER;
            } else if (elidedEdges.contains(edge.key())) {
                useKind = fusedSinkEdges.contains(edge.key())
                        ? EdgeUseKind.ELIDED_FUSED_SINK
                        : EdgeUseKind.ELIDED_FUSED_STAGE;
            } else if (edge.redirectOnly()) {
                useKind = EdgeUseKind.REDIRECT_ONLY;
            } else {
                useKind = EdgeUseKind.NORMAL;
            }
            decisions.put(edge.key(), new EdgeDecision(
                    edge.key(),
                    edge.spec().kind() == EdgeSpec.EdgeKind.SPSC_RING
                            ? EdgeImplementationKind.SPSC_RING
                            : EdgeImplementationKind.MPSC_RING,
                    useKind,
                    fusedRouterOutputOwners.getOrDefault(edge.key(), allocationOwner(compiled, edge)),
                    sourceIngressCloseGuard(compiled, edge),
                    null
            ));
        }
        return decisions;
    }

    private static Map<String, SenderDecision> senderDecisions(
            final CompiledGraph compiled,
            final Map<String, FusedRouterPlan> fusedRouters,
            final Set<String> elidedEdges
    ) {
        final Map<String, String> fusedRouterOutputOwners = new LinkedHashMap<>();
        for (final FusedRouterPlan plan : fusedRouters.values()) {
            for (final EdgeDefinition outgoing : plan.outgoingEdges()) {
                fusedRouterOutputOwners.put(outgoing.key(), plan.ownerName());
            }
        }
        final Map<String, SenderDecision> decisions = new LinkedHashMap<>();
        for (final EdgeDefinition edge : compiled.edges()) {
            if (edge.redirectOnly() || elidedEdges.contains(edge.key())) {
                continue;
            }
            final String redirectTarget = edge.spec().overflowPolicy().redirectTarget();
            final EdgeDefinition redirect = redirectTarget == null ? null
                    : compiled.redirectBySourceAndTarget()
                    .getOrDefault(edge.from(), Map.of())
                    .get(redirectTarget);
            decisions.put(edge.key(), new SenderDecision(
                    fusedRouterOutputOwners.getOrDefault(edge.key(), edge.from()),
                    edge.key(),
                    fusedRouterOutputOwners.containsKey(edge.key())
                            ? SenderKind.DIRECT_ROUTER
                            : redirect == null ? SenderKind.EDGE : SenderKind.EDGE_WITH_REDIRECT,
                    redirect == null ? null : redirect.key(),
                    OutputKind.EDGE,
                    null
            ));
        }
        return decisions;
    }

    private static String allocationOwner(final CompiledGraph compiled, final EdgeDefinition edge) {
        final NodeDefinition from = compiled.nodes().get(edge.from());
        return from.kind() == GraphPlan.NodeKind.SOURCE ? edge.to() : edge.from();
    }

    private static boolean sourceIngressCloseGuard(final CompiledGraph compiled, final EdgeDefinition edge) {
        if (!edge.sourceIngress() || edge.spec().kind() != EdgeSpec.EdgeKind.SPSC_RING) {
            return false;
        }
        final NodeDefinition source = compiled.nodes().get(edge.from());
        return source == null || source.sourceMode() != SourceMode.SINGLE_PRODUCER;
    }

    private static FusedSinkPlan fusedSinkPlan(
            final CompiledGraph compiled,
            final String workerName,
            final List<PlanningFallback> fallbackReasons
    ) {
        final NodeDefinition stage = compiled.nodes().get(workerName);
        if (stage == null || !sinkFusionOwner(stage)) {
            return null;
        }

        final List<EdgeDefinition> normalOutgoing = compiled.normalOutgoingBySource()
                .getOrDefault(workerName, List.of());
        final List<EdgeDefinition> allOutgoing = compiled.outgoingBySource()
                .getOrDefault(workerName, List.of());
        if (normalOutgoing.size() != 1 || allOutgoing.size() != 1) {
            return null;
        }

        final EdgeDefinition edge = normalOutgoing.get(0);
        if (!fusibleEdge(edge.spec())) {
            return null;
        }

        final NodeDefinition sink = compiled.nodes().get(edge.to());
        if (sink == null || sink.kind() != GraphPlan.NodeKind.SINK) {
            return null;
        }

        final PinPolicy effectivePinPolicy = effectivePinPolicy(stage.spec().pinPolicy(), sink.spec().pinPolicy());
        if (effectivePinPolicy == null) {
            fallbackReasons.add(new PlanningFallback(edge.key(), FallbackReason.CONFLICTING_EXPLICIT_PINS));
            return null;
        }

        return new FusedSinkPlan(workerName, sink.name(), edge, effectivePinPolicy);
    }

    private static FusedRouterPlan fusedRouterPlan(
            final CompiledGraph compiled,
            final String workerName,
            final List<PlanningFallback> fallbackReasons
    ) {
        final NodeDefinition owner = compiled.nodes().get(workerName);
        if (!inlineCapableOwner(owner)) {
            return null;
        }

        final List<EdgeDefinition> normalOutgoing = compiled.normalOutgoingBySource()
                .getOrDefault(workerName, List.of());
        final List<EdgeDefinition> allOutgoing = compiled.outgoingBySource()
                .getOrDefault(workerName, List.of());
        if (normalOutgoing.size() != 1 || allOutgoing.size() != 1) {
            return null;
        }

        final EdgeDefinition inputEdge = normalOutgoing.get(0);
        if (!fusibleEdge(inputEdge.spec())) {
            return null;
        }

        final NodeDefinition router = compiled.nodes().get(inputEdge.to());
        if (!fusibleRouter(router)) {
            return null;
        }
        final List<EdgeDefinition> routerIncoming = compiled.incomingByTarget()
                .getOrDefault(router.name(), List.of());
        if (routerIncoming.size() != 1) {
            fallbackReasons.add(new PlanningFallback(router.name(), FallbackReason.NON_LINEAR_TOPOLOGY));
            return null;
        }
        if (router.spec().pinPolicy().kind() != PinPolicy.PinKind.NONE) {
            fallbackReasons.add(new PlanningFallback(router.name(), FallbackReason.REQUIRES_WORKER_PLACEMENT));
            return null;
        }
        final List<EdgeDefinition> routerOutgoing = compiled.normalOutgoingBySource()
                .getOrDefault(router.name(), List.of());
        final List<EdgeDefinition> allRouterOutgoing = compiled.outgoingBySource()
                .getOrDefault(router.name(), List.of());
        if (routerOutgoing.isEmpty()) {
            fallbackReasons.add(new PlanningFallback(router.name(), FallbackReason.NON_LINEAR_TOPOLOGY));
            return null;
        }
        if (routerOutgoing.size() != allRouterOutgoing.size()) {
            fallbackReasons.add(new PlanningFallback(router.name(), FallbackReason.NON_FUSIBLE_EDGE));
            return null;
        }
        return new FusedRouterPlan(
                workerName,
                router.name(),
                inputEdge,
                List.copyOf(routerOutgoing),
                owner.spec().pinPolicy()
        );
    }

    private static boolean fusibleRouter(final NodeDefinition node) {
        return node != null
                && (node.kind() == GraphPlan.NodeKind.DISPATCH
                || node.kind() == GraphPlan.NodeKind.BROADCAST
                || node.kind() == GraphPlan.NodeKind.PARTITION)
                && node.spec().batchPolicy().kind() == BatchPolicy.BatchKind.DISABLED;
    }

    private static PinPolicy effectivePinPolicy(final PinPolicy owner, final PinPolicy fused) {
        final boolean ownerExplicit = owner.kind() != PinPolicy.PinKind.NONE;
        final boolean fusedExplicit = fused.kind() != PinPolicy.PinKind.NONE;
        if (ownerExplicit && fusedExplicit) {
            return null;
        }
        return ownerExplicit ? owner : fused;
    }

    private static boolean sinkFusionOwner(final NodeDefinition node) {
        return node.kind() == GraphPlan.NodeKind.STAGE || node.kind() == GraphPlan.NodeKind.JOIN;
    }

    private static FusedStagePlan fusedStagePlan(
            final CompiledGraph compiled,
            final String workerName,
            final List<PlanningFallback> fallbackReasons
    ) {
        final NodeDefinition stage = compiled.nodes().get(workerName);
        if (!fusibleStage(stage)) {
            return null;
        }

        final List<String> stageNames = new ArrayList<>();
        final List<EdgeDefinition> stageInputEdges = new ArrayList<>();
        final List<EdgeDefinition> elidedEdges = new ArrayList<>();
        final List<EdgeDefinition> downstreamOutgoing = new ArrayList<>();
        String currentName = workerName;
        PinPolicy effectivePinPolicy = stage.spec().pinPolicy();
        FusedSinkPlan sinkPlan = null;

        while (true) {
            final List<EdgeDefinition> normalOutgoing = compiled.normalOutgoingBySource()
                    .getOrDefault(currentName, List.of());
            final List<EdgeDefinition> allOutgoing = compiled.outgoingBySource()
                    .getOrDefault(currentName, List.of());
            if (normalOutgoing.size() != 1 || allOutgoing.size() != 1) {
                break;
            }

            final EdgeDefinition edge = normalOutgoing.get(0);
            if (!fusibleEdge(edge.spec())) {
                break;
            }

            final NodeDefinition downstream = compiled.nodes().get(edge.to());
            if (fusibleStage(downstream) && downstream.spec().pinPolicy().kind() == PinPolicy.PinKind.NONE) {
                stageNames.add(downstream.name());
                stageInputEdges.add(edge);
                elidedEdges.add(edge);
                downstreamOutgoing.addAll(compiled.outgoingBySource().getOrDefault(downstream.name(), List.of()));
                currentName = downstream.name();
                continue;
            }

            if (downstream != null && downstream.kind() == GraphPlan.NodeKind.SINK && !stageNames.isEmpty()) {
                final FusedSinkPlan candidate = fusedSinkPlan(compiled, currentName, fallbackReasons);
                if (candidate != null) {
                    final PinPolicy combinedPinPolicy = effectivePinPolicy(
                            effectivePinPolicy,
                            candidate.effectivePinPolicy()
                    );
                    if (combinedPinPolicy != null) {
                        effectivePinPolicy = combinedPinPolicy;
                        sinkPlan = candidate;
                        elidedEdges.add(candidate.edge());
                    }
                }
            }
            break;
        }

        if (stageNames.isEmpty()) {
            return null;
        }
        return new FusedStagePlan(
                workerName,
                List.copyOf(stageNames),
                List.copyOf(stageInputEdges),
                List.copyOf(elidedEdges),
                List.copyOf(downstreamOutgoing),
                sinkPlan,
                effectivePinPolicy
        );
    }

    private static boolean fusibleStage(final NodeDefinition stage) {
        return stage != null
                && stage.kind() == GraphPlan.NodeKind.STAGE
                && stage.batchLogic() == null
                && stage.spec().batchPolicy().kind() == BatchPolicy.BatchKind.DISABLED;
    }

    private static boolean fusibleEdge(final EdgeSpec spec) {
        return spec.kind() == EdgeSpec.EdgeKind.SPSC_RING
                && spec.overflowPolicy().kind() == OverflowPolicy.OverflowKind.BLOCK
                && spec.memoryMode().kind() == MemoryMode.MemoryKind.ON_HEAP_SLOTS
                && spec.batchPolicy().kind() == BatchPolicy.BatchKind.DISABLED;
    }

    private static Map<String, InlineSourceBinding> inlineFusedWorkerToSource(
            final CompiledGraph compiled,
            final Map<String, FusedStagePlan> fusedStages,
            final Map<String, FusedSinkPlan> fusedSinks,
            final Set<String> elidedEdges,
            final List<PlanningFallback> fallbackReasons
    ) {
        if (!compiled.runtimeConfig().fusion().inlineSources()) {
            return Map.of();
        }
        if (compiled.customExceptionHandler()) {
            fallbackReasons.add(new PlanningFallback(compiled.plan().name(), FallbackReason.CUSTOM_EXCEPTION_HANDLER));
            return Map.of();
        }
        final Map<String, InlineSourceBinding> mapping = new LinkedHashMap<>();
        final Map<String, PinPolicy> sinkOwnerPins = new LinkedHashMap<>();
        for (final FusedSinkPlan sinkPlan : fusedSinks.values()) {
            sinkOwnerPins.put(sinkPlan.stageName(), sinkPlan.effectivePinPolicy());
        }
        collectInlineEligible(compiled, sinkOwnerPins, elidedEdges, mapping, fallbackReasons);

        final Map<String, PinPolicy> terminalChainPins = new LinkedHashMap<>();
        for (final Map.Entry<String, FusedStagePlan> entry : fusedStages.entrySet()) {
            if (entry.getValue().sinkPlan() != null) {
                terminalChainPins.put(entry.getKey(), entry.getValue().effectivePinPolicy());
            }
        }
        collectInlineEligible(compiled, terminalChainPins, elidedEdges, mapping, fallbackReasons);
        return mapping;
    }

    private static void collectInlineEligible(
            final CompiledGraph compiled,
            final Map<String, PinPolicy> effectivePinsByWorker,
            final Set<String> elidedEdges,
            final Map<String, InlineSourceBinding> mapping,
            final List<PlanningFallback> fallbackReasons
    ) {
        for (final Map.Entry<String, PinPolicy> entry : effectivePinsByWorker.entrySet()) {
            final String workerName = entry.getKey();
            final NodeDefinition owner = compiled.nodes().get(workerName);
            if (!inlineCapableOwner(owner)) {
                fallbackReasons.add(new PlanningFallback(workerName, FallbackReason.REQUIRES_WORKER_PLACEMENT));
                continue;
            }
            if (entry.getValue().kind() != PinPolicy.PinKind.NONE
                    || compiled.runtimeConfig().placement().topologyAware()) {
                fallbackReasons.add(new PlanningFallback(workerName, FallbackReason.REQUIRES_WORKER_PLACEMENT));
                continue;
            }
            final List<EdgeDefinition> incoming = compiled.incomingByTarget()
                    .getOrDefault(workerName, List.of());
            if (incoming.size() != 1) {
                fallbackReasons.add(new PlanningFallback(workerName, FallbackReason.NON_LINEAR_TOPOLOGY));
                continue;
            }
            final EdgeDefinition edge = incoming.get(0);
            if (elidedEdges.contains(edge.key()) || !fusibleEdge(edge.spec())) {
                fallbackReasons.add(new PlanningFallback(edge.key(), FallbackReason.NON_FUSIBLE_EDGE));
                continue;
            }
            final NodeDefinition source = compiled.nodes().get(edge.from());
            if (source == null || source.kind() != GraphPlan.NodeKind.SOURCE) {
                fallbackReasons.add(new PlanningFallback(workerName, FallbackReason.NON_LINEAR_TOPOLOGY));
                continue;
            }
            if (source.sourceMode() != SourceMode.SINGLE_PRODUCER) {
                fallbackReasons.add(new PlanningFallback(source.name(), FallbackReason.MULTI_PRODUCER_SOURCE));
                continue;
            }
            if (source.preallocationSpec() != null) {
                fallbackReasons.add(new PlanningFallback(source.name(), FallbackReason.PREALLOCATED_SOURCE));
                continue;
            }
            if (source.stampedSource()) {
                fallbackReasons.add(new PlanningFallback(source.name(), FallbackReason.STAMPED_SOURCE));
                continue;
            }
            if (mayCarryHandle(source.outputType())) {
                fallbackReasons.add(new PlanningFallback(source.name(), FallbackReason.HANDLE_CAPABLE_PAYLOAD));
                continue;
            }
            final List<EdgeDefinition> sourceOutgoing = compiled.normalOutgoingBySource()
                    .getOrDefault(source.name(), List.of());
            if (sourceOutgoing.size() != 1) {
                fallbackReasons.add(new PlanningFallback(source.name(), FallbackReason.FAN_OUT_SOURCE));
                continue;
            }
            mapping.put(workerName, new InlineSourceBinding(workerName, source.name(), edge.key()));
        }
    }

    private static boolean inlineCapableOwner(final NodeDefinition node) {
        return node != null
                && node.kind() == GraphPlan.NodeKind.STAGE
                && node.logic() != null
                && node.batchLogic() == null
                && node.spec().batchPolicy().kind() == BatchPolicy.BatchKind.DISABLED;
    }

    private static boolean mayCarryHandle(final Class<?> type) {
        return type == Object.class
                || type.isAssignableFrom(SlabHandle.class)
                || type.isAssignableFrom(Stamped.class);
    }
}
