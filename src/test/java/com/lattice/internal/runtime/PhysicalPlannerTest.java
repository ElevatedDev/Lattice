package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.GraphPlan;
import com.lattice.graph.SourceMode;
import com.lattice.internal.graph.CompiledGraph;
import com.lattice.internal.graph.EdgeDefinition;
import com.lattice.internal.graph.NodeDefinition;
import com.lattice.placement.PinPolicy;
import com.lattice.routing.DispatchSpec;
import com.lattice.routing.JoinSpec;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.BatchStageLogic;
import com.lattice.stage.StageExceptionHandler;
import com.lattice.stage.StageLogic;
import com.lattice.stage.StageSpec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PhysicalPlannerTest {

    private static final Class<String> MESSAGE_TYPE = String.class;
    private static final String FUSION_ENABLED_PROPERTY = "lattice.fusion.enabled";
    private static final String INLINE_SOURCE_FUSION_PROPERTY = "lattice.fusion.inlineSource";
    private static final String INLINE_SOURCE_ELISION_PROPERTY = "lattice.fusion.inlineSource.elidePhysical";
    private static final StageLogic<String, String> PASS_THROUGH = (input, output, context) -> output.push(input);
    private static final BatchStageLogic<String, String> BATCH_PASS_THROUGH = (batch, output, context) -> {
        for (int i = 0; i < batch.size(); i++) {
            output.push(batch.get(i));
        }
    };

    private String previousFusionEnabled;
    private String previousInlineSourceFusion;
    private String previousInlineSourceElision;

    @BeforeEach
    void captureProperties() {
        previousFusionEnabled = System.getProperty(FUSION_ENABLED_PROPERTY);
        previousInlineSourceFusion = System.getProperty(INLINE_SOURCE_FUSION_PROPERTY);
        previousInlineSourceElision = System.getProperty(INLINE_SOURCE_ELISION_PROPERTY);
    }

    @AfterEach
    void restoreProperties() {
        restoreProperty(FUSION_ENABLED_PROPERTY, previousFusionEnabled);
        restoreProperty(INLINE_SOURCE_FUSION_PROPERTY, previousInlineSourceFusion);
        restoreProperty(INLINE_SOURCE_ELISION_PROPERTY, previousInlineSourceElision);
    }

    @Test
    void fusionDisabledLeavesLogicalWorkersAndRecordsFallback() {
        System.setProperty(FUSION_ENABLED_PROPERTY, "false");
        System.setProperty(INLINE_SOURCE_FUSION_PROPERTY, "true");

        final PhysicalPlan plan = PhysicalPlanner.plan(sourceStageSink("fusion-disabled", false));

        assertEquals(List.of("a", "sink"), plan.workerOrder());
        assertTrue(plan.fusedSinks().isEmpty());
        assertTrue(plan.fusedStages().isEmpty());
        assertTrue(plan.inlineSourceBindings().isEmpty());
        assertEquals(List.of(new PlanningFallback("fusion-disabled", FallbackReason.FUSION_DISABLED)),
                plan.fallbackReasons());
        assertEquals(WorkerKind.RUNNABLE, plan.workerDecisions().get("a").workerKind());
        assertEquals(WorkerKind.RUNNABLE, plan.workerDecisions().get("sink").workerKind());
        assertEquals(EdgeUseKind.NORMAL, plan.edgeDecision("a->sink").useKind());
    }

    @Test
    void linearStageChainPlansFusedStageSinkAndInlineSource() {
        enableFusion();

        final PhysicalPlan plan = PhysicalPlanner.plan(sourceStageStageSink("inline-chain", false));

        assertEquals(List.of("a"), plan.workerOrder());
        assertTrue(plan.fusedSinks().isEmpty());
        assertEquals(Map.of("a", "source"), plan.inlineFusedWorkerToSource());
        assertEquals(new InlineSourceBinding("a", "source", "source->a"), plan.inlineSourceBindings().get("a"));
        assertTrue(plan.fallbackReasons().isEmpty());

        final FusedStagePlan fusedStage = plan.fusedStage("a");
        assertNotNull(fusedStage);
        assertEquals("a", fusedStage.ownerName());
        assertEquals(List.of("b"), fusedStage.stageNames());
        assertEquals("sink", fusedStage.sinkPlan().sinkName());

        assertEquals(WorkerKind.RUNNABLE, plan.workerDecisions().get("a").workerKind());
        assertEquals(OutputKind.LINEAR_FUSED, plan.workerDecisions().get("a").outputKind());
        assertEquals(WorkerKind.FUSED_INTO_STAGE, plan.workerDecisions().get("b").workerKind());
        assertEquals("a", plan.workerDecisions().get("b").fusedOwnerName());
        assertEquals(WorkerKind.FUSED_INTO_STAGE, plan.workerDecisions().get("sink").workerKind());
        assertEquals("a", plan.workerDecisions().get("sink").fusedOwnerName());

        assertEquals(EdgeUseKind.NORMAL, plan.edgeDecision("source->a").useKind());
        assertEquals(EdgeUseKind.ELIDED_FUSED_STAGE, plan.edgeDecision("a->b").useKind());
        assertEquals(EdgeUseKind.ELIDED_FUSED_SINK, plan.edgeDecision("b->sink").useKind());
        assertEquals(List.of("source->a"), new ArrayList<>(plan.senderDecisions().keySet()));
    }

    @Test
    void inlineSourcePhysicalElisionRemovesOwnerWorkerAndSourceSenderWhenOptedIn() {
        enableFusion();
        System.setProperty(INLINE_SOURCE_ELISION_PROPERTY, "true");

        final PhysicalPlan plan = PhysicalPlanner.plan(sourceStageStageSink("inline-chain-elided", false));

        assertTrue(plan.workerOrder().isEmpty());
        assertEquals(1, plan.lifecycleParticipantCount());
        assertEquals(WorkerKind.INLINE_SOURCE_CHAIN, plan.workerDecisions().get("a").workerKind());
        assertEquals(OutputKind.LINEAR_FUSED, plan.workerDecisions().get("a").outputKind());
        assertEquals(EdgeUseKind.ELIDED_INLINE_SOURCE, plan.edgeDecision("source->a").useKind());
        assertEquals(EdgeUseKind.ELIDED_FUSED_STAGE, plan.edgeDecision("a->b").useKind());
        assertEquals(EdgeUseKind.ELIDED_FUSED_SINK, plan.edgeDecision("b->sink").useKind());
        assertTrue(plan.senderDecisions().isEmpty());
    }

    @Test
    void customExceptionHandlerKeepsSinkFusionButDisablesInlineSourceFusion() {
        enableFusion();

        final PhysicalPlan plan = PhysicalPlanner.plan(sourceStageSink("custom-handler", true));

        assertEquals(List.of("a"), plan.workerOrder());
        assertNotNull(plan.fusedSink("a"));
        assertEquals("sink", plan.fusedSink("a").sinkName());
        assertTrue(plan.inlineSourceBindings().isEmpty());
        assertEquals(List.of(new PlanningFallback("custom-handler", FallbackReason.CUSTOM_EXCEPTION_HANDLER)),
                plan.fallbackReasons());
        assertEquals(WorkerKind.FUSED_INTO_SINK, plan.workerDecisions().get("sink").workerKind());
        assertEquals(
                EdgeUseKind.ELIDED_FUSED_SINK,
                plan.edgeDecision("a->sink").useKind(),
                () -> "direct fused-sink edge should be classified as sink-elided: " + plan.edgeDecisions()
        );
    }

    @Test
    void conflictingExplicitPinsPreventSinkFusionAndRecordFallback() {
        enableFusion();

        final CompiledGraph compiled = compiled(
                "conflicting-pins",
                false,
                List.of(
                        source("source"),
                        stage("a", StageSpec.singleThreaded().pin(PinPolicy.cpu(0))),
                        sink("sink", StageSpec.singleThreaded().pin(PinPolicy.cpu(1)))
                ),
                List.of(
                        sourceEdge("source", "a", EdgeSpec.spscRing(16)),
                        workerEdge("a", "sink", EdgeSpec.spscRing(16))
                )
        );

        final PhysicalPlan plan = PhysicalPlanner.plan(compiled);

        assertEquals(List.of("a", "sink"), plan.workerOrder());
        assertNull(plan.fusedSink("a"));
        assertEquals(WorkerKind.RUNNABLE, plan.workerDecisions().get("sink").workerKind());
        assertEquals(EdgeUseKind.NORMAL, plan.edgeDecision("a->sink").useKind());
        assertTrue(
                hasFallback(plan, FallbackReason.CONFLICTING_EXPLICIT_PINS),
                () -> "pin conflict should be visible in planner fallbacks: " + plan.fallbackReasons()
        );
    }

    @Test
    void explicitEffectivePinPreventsInlineSourceFusion() {
        enableFusion();

        final CompiledGraph compiled = compiled(
                "pinned-inline-owner",
                false,
                List.of(
                        source("source"),
                        stage("a", StageSpec.singleThreaded().pin(PinPolicy.cpu(0))),
                        sink("sink")
                ),
                List.of(
                        sourceEdge("source", "a", EdgeSpec.spscRing(16)),
                        workerEdge("a", "sink", EdgeSpec.spscRing(16))
                )
        );

        final PhysicalPlan plan = PhysicalPlanner.plan(compiled);

        assertEquals(List.of("a"), plan.workerOrder());
        assertNotNull(plan.fusedSink("a"));
        assertTrue(plan.inlineSourceBindings().isEmpty());
        assertEquals(WorkerKind.RUNNABLE, plan.workerDecisions().get("a").workerKind());
        assertTrue(
                hasFallback(plan, FallbackReason.REQUIRES_WORKER_PLACEMENT),
                () -> "explicit pin should keep source inline fusion disabled: " + plan.fallbackReasons()
        );
    }

    @Test
    void nonFusibleSourceEdgeRecordsInlineFallbackWhenSinkFusionIsAvailable() {
        enableFusion();

        final CompiledGraph compiled = compiled(
                "non-fusible-source-edge",
                false,
                List.of(source("source"), stage("a"), sink("sink")),
                List.of(
                        sourceEdge("source", "a", EdgeSpec.spscRing(16).overflow(OverflowPolicy.failFast())),
                        workerEdge("a", "sink", EdgeSpec.spscRing(16))
                )
        );

        final PhysicalPlan plan = PhysicalPlanner.plan(compiled);

        assertEquals(List.of("a"), plan.workerOrder());
        assertNotNull(plan.fusedSink("a"));
        assertTrue(plan.inlineSourceBindings().isEmpty());
        assertEquals(List.of(new PlanningFallback("source->a", FallbackReason.NON_FUSIBLE_EDGE)),
                plan.fallbackReasons());
    }

    @Test
    void batchStageTerminalSinkFusionKeepsBatchOwnerRunnable() {
        enableFusion();

        final PhysicalPlan plan = PhysicalPlanner.plan(compiled(
                "batch-terminal-sink",
                false,
                List.of(source("source"), batchStage("batch"), sink("sink")),
                List.of(
                        sourceEdge("source", "batch", EdgeSpec.spscRing(16)),
                        workerEdge("batch", "sink", EdgeSpec.spscRing(16))
                )
        ));

        assertEquals(List.of("batch"), plan.workerOrder());
        assertNotNull(plan.fusedSink("batch"));
        assertEquals("sink", plan.fusedSink("batch").sinkName());
        assertTrue(plan.inlineSourceBindings().isEmpty());
        assertEquals(WorkerKind.RUNNABLE, plan.workerDecisions().get("batch").workerKind());
        assertEquals(OutputKind.DIRECT_SINK, plan.workerDecisions().get("batch").outputKind());
        assertEquals(WorkerKind.FUSED_INTO_SINK, plan.workerDecisions().get("sink").workerKind());
        assertEquals(EdgeUseKind.NORMAL, plan.edgeDecision("source->batch").useKind());
        assertEquals(EdgeUseKind.ELIDED_FUSED_SINK, plan.edgeDecision("batch->sink").useKind());
        assertEquals(List.of(new PlanningFallback("batch", FallbackReason.REQUIRES_WORKER_PLACEMENT)),
                plan.fallbackReasons());
    }

    @Test
    void joinTerminalSinkFusionKeepsJoinOwnerRunnable() {
        enableFusion();

        final PhysicalPlan plan = PhysicalPlanner.plan(compiled(
                "join-terminal-sink",
                false,
                List.of(source("left"), source("right"), join("join"), sink("sink")),
                List.of(
                        sourceEdge("left", "join", EdgeSpec.spscRing(16)),
                        sourceEdge("right", "join", EdgeSpec.spscRing(16)),
                        workerEdge("join", "sink", EdgeSpec.spscRing(16))
                )
        ));

        assertEquals(List.of("join"), plan.workerOrder());
        assertNotNull(plan.fusedSink("join"));
        assertEquals("sink", plan.fusedSink("join").sinkName());
        assertTrue(plan.inlineSourceBindings().isEmpty());
        assertEquals(WorkerKind.RUNNABLE, plan.workerDecisions().get("join").workerKind());
        assertEquals(OutputKind.DIRECT_SINK, plan.workerDecisions().get("join").outputKind());
        assertEquals(WorkerKind.FUSED_INTO_SINK, plan.workerDecisions().get("sink").workerKind());
        assertEquals(EdgeUseKind.ELIDED_FUSED_SINK, plan.edgeDecision("join->sink").useKind());
        assertEquals(List.of(new PlanningFallback("join", FallbackReason.REQUIRES_WORKER_PLACEMENT)),
                plan.fallbackReasons());
    }

    @Test
    void stageToDispatchFusionElidesRouterWorkerAndKeepsBranchEdgesPhysical() {
        enableFusion();

        final PhysicalPlan plan = PhysicalPlanner.plan(compiled(
                "stage-dispatch-fusion",
                false,
                List.of(source("source"), stage("a"), dispatch("router"), sink("left"), sink("right")),
                List.of(
                        sourceEdge("source", "a", EdgeSpec.spscRing(16)),
                        workerEdge("a", "router", EdgeSpec.spscRing(16)),
                        workerEdge("router", "left", EdgeSpec.spscRing(16)),
                        workerEdge("router", "right", EdgeSpec.spscRing(16))
                )
        ));

        assertEquals(List.of("a", "left", "right"), plan.workerOrder());
        assertNotNull(plan.fusedRouter("a"));
        assertEquals("router", plan.fusedRouter("a").routerName());
        assertEquals(WorkerKind.RUNNABLE, plan.workerDecisions().get("a").workerKind());
        assertEquals(OutputKind.DIRECT_ROUTER, plan.workerDecisions().get("a").outputKind());
        assertEquals(WorkerKind.FUSED_INTO_STAGE, plan.workerDecisions().get("router").workerKind());
        assertEquals(OutputKind.DIRECT_ROUTER, plan.workerDecisions().get("router").outputKind());
        assertEquals("a", plan.workerDecisions().get("router").fusedOwnerName());
        assertEquals(EdgeUseKind.ELIDED_FUSED_ROUTER, plan.edgeDecision("a->router").useKind());
        assertEquals(EdgeUseKind.NORMAL, plan.edgeDecision("router->left").useKind());
        assertEquals("a", plan.edgeDecision("router->left").allocationOwner());
        assertEquals("a", plan.edgeDecision("router->right").allocationOwner());
        assertEquals(SenderKind.DIRECT_ROUTER, plan.senderDecisions().get("router->left").senderKind());
        assertEquals("a", plan.senderDecisions().get("router->left").ownerName());
    }

    private static void enableFusion() {
        System.setProperty(FUSION_ENABLED_PROPERTY, "true");
        System.setProperty(INLINE_SOURCE_FUSION_PROPERTY, "true");
    }

    private static boolean hasFallback(final PhysicalPlan plan, final FallbackReason reason) {
        return plan.fallbackReasons().stream().anyMatch(fallback -> fallback.reason() == reason);
    }

    private static CompiledGraph sourceStageSink(final String graphName, final boolean customHandler) {
        return compiled(
                graphName,
                customHandler,
                List.of(source("source"), stage("a"), sink("sink")),
                List.of(
                        sourceEdge("source", "a", EdgeSpec.spscRing(16)),
                        workerEdge("a", "sink", EdgeSpec.spscRing(16))
                )
        );
    }

    private static CompiledGraph sourceStageStageSink(final String graphName, final boolean customHandler) {
        return compiled(
                graphName,
                customHandler,
                List.of(source("source"), stage("a"), stage("b"), sink("sink")),
                List.of(
                        sourceEdge("source", "a", EdgeSpec.spscRing(16)),
                        workerEdge("a", "b", EdgeSpec.spscRing(16)),
                        workerEdge("b", "sink", EdgeSpec.spscRing(16))
                )
        );
    }

    private static CompiledGraph compiled(
            final String graphName,
            final boolean customHandler,
            final List<NodeDefinition> nodes,
            final List<EdgeDefinition> edges
    ) {
        final Map<String, NodeDefinition> nodeMap = new LinkedHashMap<>();
        for (final NodeDefinition node : nodes) {
            nodeMap.put(node.name(), node);
        }

        final List<String> workerOrder = nodes.stream()
                .filter(node -> node.kind() != GraphPlan.NodeKind.SOURCE)
                .map(NodeDefinition::name)
                .toList();

        return new CompiledGraph(
                new GraphPlan(
                        graphName,
                        nodes.stream().map(PhysicalPlannerTest::planNode).toList(),
                        edges.stream().map(PhysicalPlannerTest::planEdge).toList(),
                        workerOrder
                ),
                nodeMap,
                List.copyOf(edges),
                groupByTarget(edges),
                groupBySource(edges),
                normalOutgoingBySource(edges),
                Map.of(),
                workerOrder,
                StageExceptionHandler.failGraph(),
                customHandler
        );
    }

    private static GraphPlan.Node planNode(final NodeDefinition node) {
        return new GraphPlan.Node(
                node.name(),
                node.kind(),
                node.inputType(),
                node.outputType(),
                node.spec(),
                node.sourceMode(),
                node.preallocationSpec() != null
        );
    }

    private static GraphPlan.Edge planEdge(final EdgeDefinition edge) {
        return new GraphPlan.Edge(
                edge.from(),
                edge.to(),
                edge.messageType(),
                edge.spec(),
                "",
                edge.branchIndex(),
                edge.redirectOnly()
        );
    }

    private static NodeDefinition source(final String name) {
        return new NodeDefinition(
                name,
                GraphPlan.NodeKind.SOURCE,
                null,
                MESSAGE_TYPE,
                null,
                null,
                null,
                null,
                0,
                SourceMode.SINGLE_PRODUCER,
                false,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private static NodeDefinition stage(final String name) {
        return stage(name, StageSpec.singleThreaded());
    }

    private static NodeDefinition stage(final String name, final StageSpec spec) {
        return new NodeDefinition(
                name,
                GraphPlan.NodeKind.STAGE,
                MESSAGE_TYPE,
                MESSAGE_TYPE,
                PASS_THROUGH,
                null,
                null,
                spec,
                0,
                null,
                false,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private static NodeDefinition batchStage(final String name) {
        return new NodeDefinition(
                name,
                GraphPlan.NodeKind.STAGE,
                MESSAGE_TYPE,
                MESSAGE_TYPE,
                null,
                BATCH_PASS_THROUGH,
                null,
                StageSpec.singleThreaded().batch(BatchPolicy.maxItems(8)),
                0,
                null,
                false,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private static NodeDefinition join(final String name) {
        return new NodeDefinition(
                name,
                GraphPlan.NodeKind.JOIN,
                Object.class,
                MESSAGE_TYPE,
                null,
                null,
                null,
                StageSpec.singleThreaded(),
                0,
                null,
                false,
                null,
                null,
                null,
                null,
                null,
                JoinSpec.allOf(group -> "joined")
        );
    }

    private static NodeDefinition dispatch(final String name) {
        return new NodeDefinition(
                name,
                GraphPlan.NodeKind.DISPATCH,
                MESSAGE_TYPE,
                MESSAGE_TYPE,
                null,
                null,
                null,
                StageSpec.singleThreaded(),
                0,
                null,
                false,
                null,
                null,
                DispatchSpec.roundRobin(),
                null,
                null,
                null
        );
    }

    private static NodeDefinition sink(final String name) {
        return sink(name, StageSpec.singleThreaded());
    }

    private static NodeDefinition sink(final String name, final StageSpec spec) {
        return new NodeDefinition(
                name,
                GraphPlan.NodeKind.SINK,
                MESSAGE_TYPE,
                null,
                null,
                null,
                ignored -> { },
                spec,
                0,
                null,
                false,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    private static EdgeDefinition sourceEdge(final String from, final String to, final EdgeSpec spec) {
        return edge(from, to, spec, true);
    }

    private static EdgeDefinition workerEdge(final String from, final String to, final EdgeSpec spec) {
        return edge(from, to, spec, false);
    }

    private static EdgeDefinition edge(
            final String from,
            final String to,
            final EdgeSpec spec,
            final boolean sourceIngress
    ) {
        return new EdgeDefinition(from, to, MESSAGE_TYPE, spec, 0, 0, false, sourceIngress);
    }

    private static Map<String, List<EdgeDefinition>> groupByTarget(final List<EdgeDefinition> edges) {
        final Map<String, List<EdgeDefinition>> byTarget = new LinkedHashMap<>();
        for (final EdgeDefinition edge : edges) {
            byTarget.computeIfAbsent(edge.to(), ignored -> new ArrayList<>()).add(edge);
        }
        return copyGrouped(byTarget);
    }

    private static Map<String, List<EdgeDefinition>> groupBySource(final List<EdgeDefinition> edges) {
        final Map<String, List<EdgeDefinition>> bySource = new LinkedHashMap<>();
        for (final EdgeDefinition edge : edges) {
            bySource.computeIfAbsent(edge.from(), ignored -> new ArrayList<>()).add(edge);
        }
        return copyGrouped(bySource);
    }

    private static Map<String, List<EdgeDefinition>> normalOutgoingBySource(final List<EdgeDefinition> edges) {
        final Map<String, List<EdgeDefinition>> bySource = new LinkedHashMap<>();
        for (final EdgeDefinition edge : edges) {
            if (!edge.redirectOnly()) {
                bySource.computeIfAbsent(edge.from(), ignored -> new ArrayList<>()).add(edge);
            }
        }
        return copyGrouped(bySource);
    }

    private static Map<String, List<EdgeDefinition>> copyGrouped(
            final Map<String, List<EdgeDefinition>> grouped
    ) {
        final Map<String, List<EdgeDefinition>> copy = new LinkedHashMap<>();
        for (final Map.Entry<String, List<EdgeDefinition>> entry : grouped.entrySet()) {
            copy.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return Map.copyOf(copy);
    }

    private static void restoreProperty(final String name, final String previousValue) {
        if (previousValue == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, previousValue);
        }
    }
}
