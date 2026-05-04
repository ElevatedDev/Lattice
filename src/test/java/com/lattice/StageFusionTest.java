package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.FusionSpec;
import com.lattice.graph.GraphState;
import com.lattice.graph.MetricsSpec;
import com.lattice.graph.SourceMode;
import com.lattice.graph.StaticGraph;
import com.lattice.metrics.PlacementStatus;
import com.lattice.placement.PinPolicy;
import com.lattice.routing.JoinSpec;
import com.lattice.routing.Stamped;
import com.lattice.slab.SlabHandle;
import com.lattice.slab.SlabPool;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.Emitter;
import com.lattice.stage.Output;
import com.lattice.stage.StageExceptionAction;
import com.lattice.stage.StageLogic;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageFusionTest {

    private static final MetricsSpec TEST_METRICS = MetricsSpec.off()
        .hotCounters(true)
        .fusedLogicalEdgeCounters(true);

    @Test
    void optInStageToSinkFusionElidesSinkWorkerThread() throws Exception {
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = graph("fusion")
                .source("ingress", Integer.class)
                .stage("validate", Integer.class, Integer.class, (value, out, ctx) -> out.push(value + 1),
                    StageSpec.singleThreaded())
                .sink("egress", Integer.class, ignored -> sinkThreads.add(Thread.currentThread().getName()),
                    StageSpec.singleThreaded())
                .edge("ingress", "validate", EdgeSpec.mpscRing(32))
                .edge("validate", "egress", EdgeSpec.spscRing(32))
                .build();

            graph.start();
            final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
            for (int i = 0; i < 8; i++) {
                ingress.emit(i);
            }
            ingress.close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(8, sinkThreads.size());
            assertTrue(sinkThreads.stream().allMatch(name -> name.endsWith("-validate")));
            assertEquals(8, graph.metrics().stage("egress").consumedCount());
            assertEquals(8, graph.metrics().edge("validate", "egress").consumedCount());
    }

    @Test
    void batchStageToSinkFusionElidesSinkWorkerThread() throws Exception {
            final List<Integer> consumed = new CopyOnWriteArrayList<>();
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = graph("batch-sink-fusion")
                .source("ingress", Integer.class)
                .batchStage("batch", Integer.class, Integer.class, (batch, out, ctx) -> {
                    for (int i = 0; i < batch.size(); i++) {
                        out.push(batch.get(i) + 1);
                    }
                }, StageSpec.singleThreaded().batch(BatchPolicy.maxItems(4)))
                .sink("egress", Integer.class, value -> {
                    consumed.add(value);
                    sinkThreads.add(Thread.currentThread().getName());
                }, StageSpec.singleThreaded())
                .edge("ingress", "batch", EdgeSpec.mpscRing(32).batch(BatchPolicy.maxItems(4)))
                .edge("batch", "egress", EdgeSpec.spscRing(32))
                .build();

            graph.start();
            final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
            for (int i = 0; i < 8; i++) {
                ingress.emit(i);
            }
            ingress.close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8), List.copyOf(consumed));
            assertTrue(sinkThreads.stream().allMatch(name -> name.endsWith("-batch")));
            assertEquals(8, graph.metrics().stage("egress").consumedCount());
            assertEquals(8, graph.metrics().edge("batch", "egress").consumedCount());
    }

    @Test
    void joinToSinkFusionElidesSinkWorkerThread() throws Exception {
            final List<String> consumed = new CopyOnWriteArrayList<>();
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final JoinSpec<String> joinSpec = JoinSpec.allOf(group -> {
                final Stamped<?> left = group.value("left", Stamped.class).orElseThrow();
                final Stamped<?> right = group.value("right", Stamped.class).orElseThrow();
                return group.longStamp() + ":" + left.value() + "/" + right.value();
            });
            final StaticGraph graph = graph("join-sink-fusion")
                .stampedSource("left", String.class)
                .stampedSource("right", String.class)
                .join("join", String.class, joinSpec, StageSpec.singleThreaded())
                .sink("egress", String.class, value -> {
                    consumed.add(value);
                    sinkThreads.add(Thread.currentThread().getName());
                }, StageSpec.singleThreaded())
                .edge("left", "join", EdgeSpec.mpscRing(32))
                .edge("right", "join", EdgeSpec.mpscRing(32))
                .edge("join", "egress", EdgeSpec.spscRing(32))
                .build();

            graph.start();
            final Emitter<String> left = graph.emitter("left", String.class);
            final Emitter<String> right = graph.emitter("right", String.class);
            left.emit("l0");
            right.emit("r0");
            left.emit("l1");
            right.emit("r1");
            left.close();
            right.close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(List.of("0:l0/r0", "1:l1/r1"), List.copyOf(consumed));
            assertTrue(sinkThreads.stream().allMatch(name -> name.endsWith("-join")));
            assertEquals(2, graph.metrics().stage("egress").consumedCount());
            assertEquals(2, graph.metrics().edge("join", "egress").consumedCount());
    }

    @Test
    void optInStageToStageFusionElidesIntermediateWorkerThread() throws Exception {
            final List<String> validateThreads = new CopyOnWriteArrayList<>();
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = graph("fusion-chain")
                .source("ingress", Integer.class)
                .stage("normalize", Integer.class, Integer.class, (value, out, ctx) -> out.push(value + 1),
                    StageSpec.singleThreaded())
                .stage("validate", Integer.class, Integer.class, (value, out, ctx) -> {
                    validateThreads.add(Thread.currentThread().getName());
                    out.push(value);
                }, StageSpec.singleThreaded())
                .sink("egress", Integer.class, ignored -> sinkThreads.add(Thread.currentThread().getName()),
                    StageSpec.singleThreaded())
                .edge("ingress", "normalize", EdgeSpec.mpscRing(32))
                .edge("normalize", "validate", EdgeSpec.spscRing(32))
                .edge("validate", "egress", EdgeSpec.spscRing(32))
                .build();

            graph.start();
            final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
            for (int i = 0; i < 8; i++) {
                ingress.emit(i);
            }
            ingress.close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(List.of("normalize", "validate", "egress"), graph.plan().workerOrder());
            assertEquals(8, validateThreads.size());
            assertTrue(validateThreads.stream().allMatch(name -> name.endsWith("-normalize")));
            assertEquals(8, sinkThreads.size());
            assertTrue(sinkThreads.stream().allMatch(name -> name.endsWith("-normalize")));
            assertEquals(8, graph.metrics().stage("validate").consumedCount());
            assertEquals(8, graph.metrics().stage("validate").emittedCount());
            assertEquals(8, graph.metrics().edge("normalize", "validate").emittedCount());
            assertEquals(8, graph.metrics().edge("normalize", "validate").consumedCount());
            assertEquals(8, graph.metrics().edge("validate", "egress").emittedCount());
            assertEquals(8, graph.metrics().edge("validate", "egress").consumedCount());
    }

    @Test
    void optInMaximalLinearFusionElidesMultipleStageAndSinkWorkers() throws Exception {
            final List<String> normalizeThreads = new CopyOnWriteArrayList<>();
            final List<String> validateThreads = new CopyOnWriteArrayList<>();
            final List<String> enrichThreads = new CopyOnWriteArrayList<>();
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = graph("fusion-long-chain")
                .source("ingress", Integer.class)
                .stage("normalize", Integer.class, Integer.class, (value, out, ctx) -> {
                    normalizeThreads.add(Thread.currentThread().getName());
                    out.push(value + 1);
                }, StageSpec.singleThreaded())
                .stage("validate", Integer.class, Integer.class, (value, out, ctx) -> {
                    validateThreads.add(Thread.currentThread().getName());
                    out.push(value + 1);
                }, StageSpec.singleThreaded())
                .stage("enrich", Integer.class, Integer.class, (value, out, ctx) -> {
                    enrichThreads.add(Thread.currentThread().getName());
                    out.push(value + 1);
                }, StageSpec.singleThreaded())
                .sink("egress", Integer.class, ignored -> sinkThreads.add(Thread.currentThread().getName()),
                    StageSpec.singleThreaded())
                .edge("ingress", "normalize", EdgeSpec.mpscRing(32))
                .edge("normalize", "validate", EdgeSpec.spscRing(32))
                .edge("validate", "enrich", EdgeSpec.spscRing(32))
                .edge("enrich", "egress", EdgeSpec.spscRing(32))
                .build();

            graph.start();
            final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
            for (int i = 0; i < 8; i++) {
                ingress.emit(i);
            }
            ingress.close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(List.of("normalize", "validate", "enrich", "egress"), graph.plan().workerOrder());
            assertTrue(normalizeThreads.stream().allMatch(name -> name.endsWith("-normalize")));
            assertTrue(validateThreads.stream().allMatch(name -> name.endsWith("-normalize")));
            assertTrue(enrichThreads.stream().allMatch(name -> name.endsWith("-normalize")));
            assertTrue(sinkThreads.stream().allMatch(name -> name.endsWith("-normalize")));
            assertEquals(8, graph.metrics().stage("validate").consumedCount());
            assertEquals(8, graph.metrics().stage("enrich").consumedCount());
            assertEquals(8, graph.metrics().stage("egress").consumedCount());
            assertEquals(8, graph.metrics().edge("normalize", "validate").consumedCount());
            assertEquals(8, graph.metrics().edge("validate", "enrich").consumedCount());
            assertEquals(8, graph.metrics().edge("enrich", "egress").consumedCount());
    }

    @Test
    void fusedStageFailureIsAttributedToChildStage() throws Exception {
            final List<String> failedStages = new CopyOnWriteArrayList<>();
            final StaticGraph graph = graph("fusion-failure")
                .exceptionHandler((graphName, stageName, failure, context) -> {
                    failedStages.add(stageName);
                    return StageExceptionAction.FAIL_GRAPH;
                })
                .source("ingress", Integer.class)
                .stage("normalize", Integer.class, Integer.class, (value, out, ctx) -> out.push(value + 1),
                    StageSpec.singleThreaded())
                .stage("validate", Integer.class, Integer.class, (value, out, ctx) -> {
                    throw new IllegalStateException("invalid");
                }, StageSpec.singleThreaded())
                .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
                .edge("ingress", "normalize", EdgeSpec.mpscRing(32))
                .edge("normalize", "validate", EdgeSpec.spscRing(32))
                .edge("validate", "egress", EdgeSpec.spscRing(32))
                .build();

            graph.start();
            graph.emitter("ingress", Integer.class).emit(1);

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(GraphState.FAILED, graph.state());
            assertEquals(List.of("validate"), failedStages);
            assertEquals(0, graph.metrics().stage("normalize").stageExceptions());
            assertEquals(1, graph.metrics().stage("validate").stageExceptions());
            assertTrue(graph.failure().orElseThrow().getMessage().contains("stage failed: validate"));
    }

    @Test
    void customExceptionHandlerDisablesInlineSourceFusion() throws Exception {
            final List<String> failedStages = new CopyOnWriteArrayList<>();
            final StaticGraph graph = inlineGraph("inline-custom-handler")
                .exceptionHandler((graphName, stageName, failure, context) -> {
                    failedStages.add(stageName);
                    return StageExceptionAction.POISON_STAGE;
                })
                .source("ingress", Integer.class, SourceMode.SINGLE_PRODUCER)
                .stage("explode", Integer.class, Integer.class, (value, out, ctx) -> {
                    throw new IllegalStateException("boom");
                }, StageSpec.singleThreaded())
                .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
                .edge("ingress", "explode", EdgeSpec.spscRing(8))
                .edge("explode", "egress", EdgeSpec.spscRing(8))
                .build();

            graph.start();
            final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
            ingress.emit(1);
            ingress.close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(GraphState.STOPPED, graph.state());
            assertEquals(List.of("explode"), failedStages);
            assertEquals(1, graph.metrics().stage("explode").stageExceptions());
            assertTrue(graph.failure().isEmpty());
    }

    @Test
    void inlineSourcePhysicalElisionTerminatesWithoutOwnerWorkerThread() throws Exception {
            final List<Integer> consumed = new CopyOnWriteArrayList<>();
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = inlineElidedGraph("inline-elided")
                .source("ingress", Integer.class, SourceMode.SINGLE_PRODUCER)
                .stage("validate", Integer.class, Integer.class, (value, out, ctx) -> out.push(value + 1),
                    StageSpec.singleThreaded())
                .sink("egress", Integer.class, value -> {
                    consumed.add(value);
                    sinkThreads.add(Thread.currentThread().getName());
                }, StageSpec.singleThreaded())
                .edge("ingress", "validate", EdgeSpec.spscRing(8))
                .edge("validate", "egress", EdgeSpec.spscRing(8))
                .build();

            graph.start();
            final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
            final String callerThread = Thread.currentThread().getName();
            ingress.emit(41);
            ingress.close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(GraphState.STOPPED, graph.state());
            assertEquals(List.of(42), consumed);
            assertEquals(List.of(callerThread), sinkThreads);
            assertEquals(1, graph.metrics().stage("validate").consumedCount());
            assertEquals(1, graph.metrics().stage("egress").consumedCount());
            assertEquals(1, graph.metrics().edge("ingress", "validate").emittedCount());
            assertEquals(1, graph.metrics().edge("ingress", "validate").consumedCount());
            assertEquals(1, graph.metrics().edge("validate", "egress").emittedCount());
            assertEquals(1, graph.metrics().edge("validate", "egress").consumedCount());
    }

    @Test
    void benignFusedChainFailureIsAttributedToDownstreamStage() throws Exception {
            final List<String> failedStages = new CopyOnWriteArrayList<>();
            final StaticGraph graph = graph("fusion-downstream-failure")
                .exceptionHandler((graphName, stageName, failure, context) -> {
                    failedStages.add(stageName);
                    return StageExceptionAction.FAIL_GRAPH;
                })
                .source("ingress", Integer.class)
                .stage("normalize", Integer.class, Integer.class, (value, out, ctx) -> out.push(value + 1),
                    StageSpec.singleThreaded())
                .stage("validate", Integer.class, Integer.class, (value, out, ctx) -> out.push(value),
                    StageSpec.singleThreaded())
                .stage("enrich", Integer.class, Integer.class, (value, out, ctx) -> {
                    throw new IllegalStateException("bad enrich");
                }, StageSpec.singleThreaded())
                .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
                .edge("ingress", "normalize", EdgeSpec.mpscRing(32))
                .edge("normalize", "validate", EdgeSpec.spscRing(32))
                .edge("validate", "enrich", EdgeSpec.spscRing(32))
                .edge("enrich", "egress", EdgeSpec.spscRing(32))
                .build();

            graph.start();
            graph.emitter("ingress", Integer.class).emit(1);
            graph.emitter("ingress", Integer.class).close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(GraphState.FAILED, graph.state());
            assertEquals(List.of("enrich"), failedStages);
            assertEquals(0, graph.metrics().stage("validate").stageExceptions());
            assertEquals(1, graph.metrics().stage("enrich").stageExceptions());
    }

    @Test
    void benignFusedSinkFailureIsAttributedToSink() throws Exception {
            final List<String> failedStages = new CopyOnWriteArrayList<>();
            final StaticGraph graph = graph("fusion-sink-failure")
                .exceptionHandler((graphName, stageName, failure, context) -> {
                    failedStages.add(stageName);
                    return StageExceptionAction.FAIL_GRAPH;
                })
                .source("ingress", Integer.class)
                .stage("validate", Integer.class, Integer.class, (value, out, ctx) -> out.push(value),
                    StageSpec.singleThreaded())
                .sink("egress", Integer.class, ignored -> {
                    throw new IllegalStateException("sink failed");
                }, StageSpec.singleThreaded())
                .edge("ingress", "validate", EdgeSpec.mpscRing(32))
                .edge("validate", "egress", EdgeSpec.spscRing(32))
                .build();

            graph.start();
            graph.emitter("ingress", Integer.class).emit(1);
            graph.emitter("ingress", Integer.class).close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(GraphState.FAILED, graph.state());
            assertEquals(List.of("egress"), failedStages);
            assertEquals(0, graph.metrics().stage("validate").stageExceptions());
            assertEquals(1, graph.metrics().stage("egress").stageExceptions());
    }

    @Test
    void fusedTypeValidationAttributesMisroutedPayloadToReceivingStage() throws Exception {
            final List<String> failedStages = new CopyOnWriteArrayList<>();
            @SuppressWarnings({"rawtypes", "unchecked"})
            final StageLogic<Integer, Integer> badProducer = (StageLogic) (value, out, ctx) ->
                ((Output) out).push("not-an-integer");
            final StaticGraph graph = StaticGraph.builder("fusion-type-validation")
                .metrics(TEST_METRICS)
                .fusion(FusionSpec.defaults().validateTypes(true))
                .exceptionHandler((graphName, stageName, failure, context) -> {
                    failedStages.add(stageName);
                    return StageExceptionAction.FAIL_GRAPH;
                })
                .source("ingress", Integer.class)
                .stage("normalize", Integer.class, Integer.class, badProducer, StageSpec.singleThreaded())
                .stage("validate", Integer.class, Integer.class, (value, out, ctx) -> out.push(value),
                    StageSpec.singleThreaded())
                .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
                .edge("ingress", "normalize", EdgeSpec.mpscRing(32))
                .edge("normalize", "validate", EdgeSpec.spscRing(32))
                .edge("validate", "egress", EdgeSpec.spscRing(32))
                .build();

            graph.start();
            graph.emitter("ingress", Integer.class).emit(1);
            graph.emitter("ingress", Integer.class).close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(GraphState.FAILED, graph.state());
            assertEquals(List.of("validate"), failedStages);
            assertEquals(0, graph.metrics().stage("normalize").stageExceptions());
            assertEquals(1, graph.metrics().stage("validate").stageExceptions());
            assertTrue(graph.failure().orElseThrow().getCause() instanceof ClassCastException);
    }

    @Test
    void elidedInlineSourceFailureThrowsFromEmitAndFailsGraph() throws Exception {
            final StaticGraph graph = inlineElidedGraph("inline-elided-failure")
                .source("ingress", Integer.class, SourceMode.SINGLE_PRODUCER)
                .stage("explode", Integer.class, Integer.class, (value, out, ctx) -> {
                    throw new IllegalStateException("boom");
                }, StageSpec.singleThreaded())
                .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
                .edge("ingress", "explode", EdgeSpec.spscRing(8))
                .edge("explode", "egress", EdgeSpec.spscRing(8))
                .build();

            graph.start();
            final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);

            assertThrows(com.lattice.graph.GraphRuntimeException.class, () -> ingress.emit(1));
            assertEquals(GraphState.FAILED, graph.state());
            assertTrue(ingress.isClosed());
            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(1, graph.metrics().stage("explode").stageExceptions());
            assertTrue(graph.failure().orElseThrow().getMessage().contains("stage failed: explode"));
    }

    @Test
    void fusedObjectTypedChainRetainsSlabHandleUntilOwnerStageReturns() throws Exception {
            final SlabPool<String> pool = new SlabPool<>("fusion-object-handle", 1);
            final List<Boolean> releasedBeforeOwnerReturned = new CopyOnWriteArrayList<>();
            final StaticGraph graph = graph("fusion-object-handle")
                .source("ingress", Object.class)
                .stage("owner", Object.class, Object.class, (value, out, ctx) -> {
                    out.push(value);
                    releasedBeforeOwnerReturned.add(((SlabHandle<?>) value).released());
                }, StageSpec.singleThreaded())
                .stage("child", Object.class, Object.class, (value, out, ctx) -> out.push(value),
                    StageSpec.singleThreaded())
                .sink("egress", Object.class, ignored -> { }, StageSpec.singleThreaded())
                .edge("ingress", "owner", EdgeSpec.mpscRing(8))
                .edge("owner", "child", EdgeSpec.spscRing(8))
                .edge("child", "egress", EdgeSpec.spscRing(8))
                .build();

            graph.start();
            graph.emitter("ingress", Object.class).emit(pool.acquire("payload"));
            graph.emitter("ingress", Object.class).close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertEquals(List.of(false), releasedBeforeOwnerReturned);
            assertFalse(graph.failure().isPresent());
            assertEquals(0, pool.leakedCount());
    }

    @Test
    void fusedSinkExplicitPinBecomesEffectiveWorkerPinWithoutMutatingPlan() throws Exception {
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = graph("fusion-sink-pin")
                .source("ingress", Integer.class)
                .stage("validate", Integer.class, Integer.class, (value, out, ctx) -> out.push(value),
                    StageSpec.singleThreaded())
                .sink("egress", Integer.class, ignored -> sinkThreads.add(Thread.currentThread().getName()),
                    StageSpec.singleThreaded().pin(PinPolicy.inheritCpuset()))
                .edge("ingress", "validate", EdgeSpec.mpscRing(32))
                .edge("validate", "egress", EdgeSpec.spscRing(32))
                .build();

            assertEquals(PinPolicy.PinKind.NONE, graph.plan().placement("validate").orElseThrow().pinPolicy().kind());
            assertEquals(PinPolicy.PinKind.INHERIT_CPUSET,
                graph.plan().placement("egress").orElseThrow().pinPolicy().kind());

            graph.start();
            graph.emitter("ingress", Integer.class).emit(1);
            graph.emitter("ingress", Integer.class).close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertTrue(sinkThreads.stream().allMatch(name -> name.endsWith("-validate")));
            assertNotEquals(PlacementStatus.NOT_REQUESTED, graph.metrics().stage("validate").placementStatus());
    }

    @Test
    void fusionDoesNotOverrideConflictingExplicitPins() throws Exception {
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = graph("fusion-conflicting-pins")
                .source("ingress", Integer.class)
                .stage("validate", Integer.class, Integer.class, (value, out, ctx) -> out.push(value),
                    StageSpec.singleThreaded().pin(PinPolicy.inheritCpuset()))
                .sink("egress", Integer.class, ignored -> sinkThreads.add(Thread.currentThread().getName()),
                    StageSpec.singleThreaded().pin(PinPolicy.inheritCpuset()))
                .edge("ingress", "validate", EdgeSpec.mpscRing(32))
                .edge("validate", "egress", EdgeSpec.spscRing(32))
                .build();

            graph.start();
            graph.emitter("ingress", Integer.class).emit(1);
            graph.emitter("ingress", Integer.class).close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertTrue(sinkThreads.stream().allMatch(name -> name.endsWith("-egress")));
    }

    private static StaticGraph.Builder graph(final String name) {
        return StaticGraph.builder(name).metrics(TEST_METRICS);
    }

    private static StaticGraph.Builder inlineGraph(final String name) {
        return graph(name).fusion(FusionSpec.defaults().inlineSources(true));
    }

    private static StaticGraph.Builder inlineElidedGraph(final String name) {
        return graph(name).fusion(FusionSpec.defaults()
            .inlineSources(true)
            .elideInlineSourcePhysicalPath(true));
    }
}
