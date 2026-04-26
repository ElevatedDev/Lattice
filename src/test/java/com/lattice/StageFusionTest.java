package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import com.lattice.metrics.PlacementStatus;
import com.lattice.placement.PinPolicy;
import com.lattice.slab.SlabHandle;
import com.lattice.slab.SlabPool;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageExceptionAction;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StageFusionTest {

    @Test
    void optInStageToSinkFusionElidesSinkWorkerThread() throws Exception {
        final String previous = System.getProperty("lattice.fusion.enabled");
        System.setProperty("lattice.fusion.enabled", "true");
        try {
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = StaticGraph.builder("fusion")
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
        } finally {
            restoreFusionProperty(previous);
        }
    }

    @Test
    void optInStageToStageFusionElidesIntermediateWorkerThread() throws Exception {
        final String previous = System.getProperty("lattice.fusion.enabled");
        System.setProperty("lattice.fusion.enabled", "true");
        try {
            final List<String> validateThreads = new CopyOnWriteArrayList<>();
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = StaticGraph.builder("fusion-chain")
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
        } finally {
            restoreFusionProperty(previous);
        }
    }

    @Test
    void optInMaximalLinearFusionElidesMultipleStageAndSinkWorkers() throws Exception {
        final String previous = System.getProperty("lattice.fusion.enabled");
        System.setProperty("lattice.fusion.enabled", "true");
        try {
            final List<String> normalizeThreads = new CopyOnWriteArrayList<>();
            final List<String> validateThreads = new CopyOnWriteArrayList<>();
            final List<String> enrichThreads = new CopyOnWriteArrayList<>();
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = StaticGraph.builder("fusion-long-chain")
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
        } finally {
            restoreFusionProperty(previous);
        }
    }

    @Test
    void fusedStageFailureIsAttributedToChildStage() throws Exception {
        final String previous = System.getProperty("lattice.fusion.enabled");
        System.setProperty("lattice.fusion.enabled", "true");
        try {
            final List<String> failedStages = new CopyOnWriteArrayList<>();
            final StaticGraph graph = StaticGraph.builder("fusion-failure")
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
        } finally {
            restoreFusionProperty(previous);
        }
    }

    @Test
    void fusedObjectTypedChainRetainsSlabHandleUntilOwnerStageReturns() throws Exception {
        final String previous = System.getProperty("lattice.fusion.enabled");
        System.setProperty("lattice.fusion.enabled", "true");
        try {
            final SlabPool<String> pool = new SlabPool<>("fusion-object-handle", 1);
            final List<Boolean> releasedBeforeOwnerReturned = new CopyOnWriteArrayList<>();
            final StaticGraph graph = StaticGraph.builder("fusion-object-handle")
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
        } finally {
            restoreFusionProperty(previous);
        }
    }

    @Test
    void fusedSinkExplicitPinBecomesEffectiveWorkerPinWithoutMutatingPlan() throws Exception {
        final String previous = System.getProperty("lattice.fusion.enabled");
        System.setProperty("lattice.fusion.enabled", "true");
        try {
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = StaticGraph.builder("fusion-sink-pin")
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
        } finally {
            restoreFusionProperty(previous);
        }
    }

    @Test
    void fusionDoesNotOverrideConflictingExplicitPins() throws Exception {
        final String previous = System.getProperty("lattice.fusion.enabled");
        System.setProperty("lattice.fusion.enabled", "true");
        try {
            final List<String> sinkThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = StaticGraph.builder("fusion-conflicting-pins")
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
        } finally {
            restoreFusionProperty(previous);
        }
    }

    private static void restoreFusionProperty(final String previous) {
        if (previous == null) {
            System.clearProperty("lattice.fusion.enabled");
        } else {
            System.setProperty("lattice.fusion.enabled", previous);
        }
    }
}
