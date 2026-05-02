package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import com.lattice.routing.BroadcastSpec;
import com.lattice.routing.DispatchSpec;
import com.lattice.routing.PartitionSpec;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RouterFusionTest {

    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(5);
    private static final StageSpec STAGE = StageSpec.singleThreaded();
    private static final String FUSION_ENABLED_PROPERTY = "lattice.fusion.enabled";
    private static final String INLINE_SOURCE_FUSION_PROPERTY = "lattice.fusion.inlineSource";
    private static final String INLINE_SOURCE_ELISION_PROPERTY = "lattice.fusion.inlineSource.elidePhysical";

    @Test
    void stageToDispatchFusionMatchesPhysicalAndKeepsLogicalMetrics() throws Exception {
        final RouterRun physical = runDispatch("router-fusion-dispatch-physical", false);
        final RouterRun fused = runDispatch("router-fusion-dispatch-fused", true);

        assertEquals(physical.firstBranch(), fused.firstBranch());
        assertEquals(physical.secondBranch(), fused.secondBranch());
        assertEquals(physical.metrics(), fused.metrics());
        assertLogicalRouterMetrics(fused, 12, true);
        assertConsumersDidNotRunOnRouter(fused, "router-fusion-dispatch-fused", "route");
    }

    @Test
    void stageToBroadcastFusionMatchesPhysicalAndKeepsLogicalMetrics() throws Exception {
        final RouterRun physical = runBroadcast("router-fusion-broadcast-physical", false);
        final RouterRun fused = runBroadcast("router-fusion-broadcast-fused", true);

        assertEquals(physical.firstBranch(), fused.firstBranch());
        assertEquals(physical.secondBranch(), fused.secondBranch());
        assertEquals(physical.metrics(), fused.metrics());
        assertLogicalRouterMetrics(fused, 12, false);
        assertConsumersDidNotRunOnRouter(fused, "router-fusion-broadcast-fused", "fanout");
    }

    @Test
    void stageToPartitionFusionMatchesPhysicalAndKeepsLogicalMetrics() throws Exception {
        final RouterRun physical = runPartition("router-fusion-partition-physical", false);
        final RouterRun fused = runPartition("router-fusion-partition-fused", true);

        assertEquals(physical.firstBranch(), fused.firstBranch());
        assertEquals(physical.secondBranch(), fused.secondBranch());
        assertEquals(physical.metrics(), fused.metrics());
        assertLogicalRouterMetrics(fused, 12, true);
        assertConsumersDidNotRunOnRouter(fused, "router-fusion-partition-fused", "partition");
    }

    private static RouterRun runDispatch(final String graphName, final boolean fusionEnabled) throws Exception {
        return withFusion(fusionEnabled, () -> {
            final List<Integer> left = new CopyOnWriteArrayList<>();
            final List<Integer> right = new CopyOnWriteArrayList<>();
            final List<String> consumerThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = StaticGraph.builder(graphName)
                .source("ingress", Integer.class)
                .stage("normalize", Integer.class, Integer.class, (value, out, ctx) -> out.push(value + 100),
                    STAGE)
                .dispatch("route", Integer.class, DispatchSpec.roundRobin(), STAGE)
                .sink("left", Integer.class, value -> {
                    left.add(value);
                    consumerThreads.add(Thread.currentThread().getName());
                }, STAGE)
                .sink("right", Integer.class, value -> {
                    right.add(value);
                    consumerThreads.add(Thread.currentThread().getName());
                }, STAGE)
                .edge("ingress", "normalize", EdgeSpec.mpscRing(32))
                .edge("normalize", "route", EdgeSpec.spscRing(32))
                .edge("route", "left", EdgeSpec.spscRing(32))
                .edge("route", "right", EdgeSpec.spscRing(32))
                .build();

            return runGraph(graph, graphName, "normalize", "route", "left", "right", fusionEnabled,
                left, right, consumerThreads, 12);
        });
    }

    private static RouterRun runBroadcast(final String graphName, final boolean fusionEnabled) throws Exception {
        return withFusion(fusionEnabled, () -> {
            final List<Integer> journal = new CopyOnWriteArrayList<>();
            final List<Integer> risk = new CopyOnWriteArrayList<>();
            final List<String> consumerThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = StaticGraph.builder(graphName)
                .source("ingress", Integer.class)
                .stage("normalize", Integer.class, Integer.class, (value, out, ctx) -> out.push(value + 100),
                    STAGE)
                .broadcast("fanout", Integer.class, BroadcastSpec.copy(), STAGE)
                .sink("journal", Integer.class, value -> {
                    journal.add(value);
                    consumerThreads.add(Thread.currentThread().getName());
                }, STAGE)
                .sink("risk", Integer.class, value -> {
                    risk.add(value);
                    consumerThreads.add(Thread.currentThread().getName());
                }, STAGE)
                .edge("ingress", "normalize", EdgeSpec.mpscRing(32))
                .edge("normalize", "fanout", EdgeSpec.spscRing(32))
                .edge("fanout", "journal", EdgeSpec.spscRing(32))
                .edge("fanout", "risk", EdgeSpec.spscRing(32))
                .build();

            return runGraph(graph, graphName, "normalize", "fanout", "journal", "risk", fusionEnabled,
                journal, risk, consumerThreads, 12);
        });
    }

    private static RouterRun runPartition(final String graphName, final boolean fusionEnabled) throws Exception {
        return withFusion(fusionEnabled, () -> {
            final List<Integer> lane0 = new CopyOnWriteArrayList<>();
            final List<Integer> lane1 = new CopyOnWriteArrayList<>();
            final List<String> consumerThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = StaticGraph.builder(graphName)
                .source("ingress", Integer.class)
                .stage("normalize", Integer.class, Integer.class, (value, out, ctx) -> out.push(value + 100),
                    STAGE)
                .partition("partition", Integer.class, PartitionSpec.byKey(value -> value & 1, 2), STAGE)
                .sink("lane0", Integer.class, value -> {
                    lane0.add(value);
                    consumerThreads.add(Thread.currentThread().getName());
                }, STAGE)
                .sink("lane1", Integer.class, value -> {
                    lane1.add(value);
                    consumerThreads.add(Thread.currentThread().getName());
                }, STAGE)
                .edge("ingress", "normalize", EdgeSpec.mpscRing(32))
                .edge("normalize", "partition", EdgeSpec.spscRing(32))
                .edge("partition", "lane0", EdgeSpec.spscRing(32))
                .edge("partition", "lane1", EdgeSpec.spscRing(32))
                .build();

            return runGraph(graph, graphName, "normalize", "partition", "lane0", "lane1", fusionEnabled,
                lane0, lane1, consumerThreads, 12);
        });
    }

    private static RouterRun runGraph(
        final StaticGraph graph,
        final String graphName,
        final String ownerName,
        final String routerName,
        final String firstBranchName,
        final String secondBranchName,
        final boolean fusionEnabled,
        final List<Integer> firstBranch,
        final List<Integer> secondBranch,
        final List<String> consumerThreads,
        final int itemCount
    ) throws Exception {
        Emitter<Integer> ingress = null;
        try {
            graph.start();
            if (fusionEnabled) {
                assertRouterWorkerElided(graphName, ownerName, routerName);
            }
            ingress = graph.emitter("ingress", Integer.class);
            for (int i = 0; i < itemCount; i++) {
                ingress.emit(i);
            }
            ingress.close();

            assertTrue(graph.awaitTermination(STOP_TIMEOUT));
            assertEquals(GraphState.STOPPED, graph.state());
            return new RouterRun(
                List.copyOf(firstBranch),
                List.copyOf(secondBranch),
                List.copyOf(consumerThreads),
                RouterMetrics.capture(graph, ownerName, routerName, firstBranchName, secondBranchName)
            );
        } finally {
            closeQuietly(graph, ingress);
        }
    }

    private static void assertRouterWorkerElided(
        final String graphName,
        final String ownerName,
        final String routerName
    ) {
        assertTrue(hasWorkerThread(graphName, ownerName),
            () -> "expected owner worker to be running: " + workerThreadName(graphName, ownerName));
        assertFalse(hasWorkerThread(graphName, routerName),
            () -> "router worker should be elided: " + workerThreadName(graphName, routerName));
    }

    private static void assertConsumersDidNotRunOnRouter(
        final RouterRun run,
        final String graphName,
        final String routerName
    ) {
        final String routerThreadName = workerThreadName(graphName, routerName);
        assertFalse(run.consumerThreads().contains(routerThreadName),
            () -> "consumer ran on router worker thread " + routerThreadName + ": " + run.consumerThreads());
    }

    private static void assertLogicalRouterMetrics(
        final RouterRun run,
        final int itemCount,
        final boolean laneSelectionsExpected
    ) {
        final RouterMetrics metrics = run.metrics();
        final int firstBranchCount = run.firstBranch().size();
        final int secondBranchCount = run.secondBranch().size();
        final int branchTotal = firstBranchCount + secondBranchCount;

        assertEquals(itemCount, metrics.ownerConsumed());
        assertEquals(itemCount, metrics.ownerEmitted());
        assertEquals(itemCount, metrics.routerConsumed());
        assertEquals(branchTotal, metrics.routerEmitted());
        assertEquals(itemCount, metrics.routingDecisions());
        assertEquals(itemCount, metrics.ownerToRouterEmitted());
        assertEquals(itemCount, metrics.ownerToRouterConsumed());
        assertEquals(firstBranchCount, metrics.firstBranchEmitted());
        assertEquals(firstBranchCount, metrics.firstBranchConsumed());
        assertEquals(secondBranchCount, metrics.secondBranchEmitted());
        assertEquals(secondBranchCount, metrics.secondBranchConsumed());
        assertEquals(laneSelectionsExpected ? firstBranchCount : 0, metrics.firstBranchLaneSelections());
        assertEquals(laneSelectionsExpected ? secondBranchCount : 0, metrics.secondBranchLaneSelections());
    }

    private static boolean hasWorkerThread(final String graphName, final String workerName) {
        final String expected = workerThreadName(graphName, workerName);
        return Thread.getAllStackTraces().keySet().stream()
            .anyMatch(thread -> thread.isAlive() && thread.getName().equals(expected));
    }

    private static String workerThreadName(final String graphName, final String workerName) {
        return "lattice-" + graphName + "-" + workerName;
    }

    private static void closeQuietly(final StaticGraph graph, final Emitter<Integer> ingress) {
        if (ingress != null && !ingress.isClosed()) {
            ingress.close();
        }
        final GraphState state = graph.state();
        if (state != GraphState.STOPPED && state != GraphState.FAILED) {
            graph.stop(STOP_TIMEOUT);
        }
    }

    private static RouterRun withFusion(final boolean enabled, final ThrowingSupplier<RouterRun> run)
        throws Exception {
        final String previousFusion = System.getProperty(FUSION_ENABLED_PROPERTY);
        final String previousInline = System.getProperty(INLINE_SOURCE_FUSION_PROPERTY);
        final String previousInlineElision = System.getProperty(INLINE_SOURCE_ELISION_PROPERTY);
        System.setProperty(FUSION_ENABLED_PROPERTY, Boolean.toString(enabled));
        System.setProperty(INLINE_SOURCE_FUSION_PROPERTY, "false");
        System.setProperty(INLINE_SOURCE_ELISION_PROPERTY, "false");
        try {
            return run.get();
        } finally {
            restoreProperty(FUSION_ENABLED_PROPERTY, previousFusion);
            restoreProperty(INLINE_SOURCE_FUSION_PROPERTY, previousInline);
            restoreProperty(INLINE_SOURCE_ELISION_PROPERTY, previousInlineElision);
        }
    }

    private static void restoreProperty(final String name, final String previousValue) {
        if (previousValue == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, previousValue);
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    private record RouterRun(
        List<Integer> firstBranch,
        List<Integer> secondBranch,
        List<String> consumerThreads,
        RouterMetrics metrics
    ) {
    }

    private record RouterMetrics(
        long ownerConsumed,
        long ownerEmitted,
        long routerConsumed,
        long routerEmitted,
        long routingDecisions,
        long ownerToRouterEmitted,
        long ownerToRouterConsumed,
        long firstBranchEmitted,
        long firstBranchConsumed,
        long secondBranchEmitted,
        long secondBranchConsumed,
        long firstBranchLaneSelections,
        long secondBranchLaneSelections
    ) {
        private static RouterMetrics capture(
            final StaticGraph graph,
            final String ownerName,
            final String routerName,
            final String firstBranchName,
            final String secondBranchName
        ) {
            return new RouterMetrics(
                graph.metrics().stage(ownerName).consumedCount(),
                graph.metrics().stage(ownerName).emittedCount(),
                graph.metrics().stage(routerName).consumedCount(),
                graph.metrics().stage(routerName).emittedCount(),
                graph.metrics().stage(routerName).routingDecisions(),
                graph.metrics().edge(ownerName, routerName).emittedCount(),
                graph.metrics().edge(ownerName, routerName).consumedCount(),
                graph.metrics().edge(routerName, firstBranchName).emittedCount(),
                graph.metrics().edge(routerName, firstBranchName).consumedCount(),
                graph.metrics().edge(routerName, secondBranchName).emittedCount(),
                graph.metrics().edge(routerName, secondBranchName).consumedCount(),
                graph.metrics().edge(routerName, firstBranchName).laneSelections(),
                graph.metrics().edge(routerName, secondBranchName).laneSelections()
            );
        }
    }
}
