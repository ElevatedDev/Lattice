package io.github.elevateddev.lattice;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.edge.OverflowPolicy;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.routing.BroadcastSpec;
import io.github.elevateddev.lattice.routing.DispatchSpec;
import io.github.elevateddev.lattice.routing.JoinGroup;
import io.github.elevateddev.lattice.routing.JoinSpec;
import io.github.elevateddev.lattice.routing.PartitionSpec;
import io.github.elevateddev.lattice.routing.Stamped;
import io.github.elevateddev.lattice.slab.SlabHandle;
import io.github.elevateddev.lattice.slab.SlabPool;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Phase4RoutingTest {
    private static final MetricsSpec TEST_METRICS = MetricsSpec.off()
        .hotCounters(true)
        .fusedLogicalEdgeCounters(true);


    @Test
    void dispatchRoundRobinDeliversEachItemToExactlyOneBranch() throws Exception {
        final List<Integer> left = Collections.synchronizedList(new ArrayList<>());
        final List<Integer> right = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("dispatch")
            .source("ingress", Integer.class)
            .dispatch("route", Integer.class, DispatchSpec.roundRobin(), StageSpec.singleThreaded())
            .sink("left", Integer.class, left::add, StageSpec.singleThreaded())
            .sink("right", Integer.class, right::add, StageSpec.singleThreaded())
            .edge("ingress", "route", EdgeSpec.mpscRing(32))
            .edge("route", "left", EdgeSpec.spscRing(32))
            .edge("route", "right", EdgeSpec.spscRing(32))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 20; i++) {
            ingress.emit(i);
        }
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(20, left.size() + right.size());
        assertEquals(10, left.size());
        assertEquals(10, right.size());
        assertEquals(20, graph.metrics().stage("route").routingDecisions());
    }

    @Test
    void dispatchKeyedAndWeightedModesUseStableBranchSelection() throws Exception {
        final List<Integer> even = Collections.synchronizedList(new ArrayList<>());
        final List<Integer> odd = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph keyed = graph("keyed-dispatch")
            .source("ingress", Integer.class)
            .dispatch("route", Integer.class, DispatchSpec.keyed(value -> value), StageSpec.singleThreaded())
            .sink("even", Integer.class, even::add, StageSpec.singleThreaded())
            .sink("odd", Integer.class, odd::add, StageSpec.singleThreaded())
            .edge("ingress", "route", EdgeSpec.mpscRing(32))
            .edge("route", "even", EdgeSpec.spscRing(32))
            .edge("route", "odd", EdgeSpec.spscRing(32))
            .build();

        keyed.start();
        final Emitter<Integer> keyedIngress = keyed.emitter("ingress", Integer.class);
        for (int i = 0; i < 10; i++) {
            keyedIngress.emit(i);
        }
        keyedIngress.close();
        assertTrue(keyed.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(List.of(0, 2, 4, 6, 8), List.copyOf(even));
        assertEquals(List.of(1, 3, 5, 7, 9), List.copyOf(odd));

        final List<Integer> light = Collections.synchronizedList(new ArrayList<>());
        final List<Integer> heavy = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph weighted = graph("weighted-dispatch")
            .source("ingress", Integer.class)
            .dispatch("route", Integer.class, DispatchSpec.weighted(1, 3), StageSpec.singleThreaded())
            .sink("light", Integer.class, light::add, StageSpec.singleThreaded())
            .sink("heavy", Integer.class, heavy::add, StageSpec.singleThreaded())
            .edge("ingress", "route", EdgeSpec.mpscRing(32))
            .edge("route", "light", EdgeSpec.spscRing(32))
            .edge("route", "heavy", EdgeSpec.spscRing(32))
            .build();

        weighted.start();
        final Emitter<Integer> weightedIngress = weighted.emitter("ingress", Integer.class);
        for (int i = 0; i < 8; i++) {
            weightedIngress.emit(i);
        }
        weightedIngress.close();
        assertTrue(weighted.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(2, light.size());
        assertEquals(6, heavy.size());
    }

    @Test
    void broadcastCopyDeliversEveryItemToEveryRequiredBranch() throws Exception {
        final List<Integer> journal = Collections.synchronizedList(new ArrayList<>());
        final List<Integer> risk = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("broadcast")
            .source("ingress", Integer.class)
            .broadcast("fanout", Integer.class, BroadcastSpec.copy(), StageSpec.singleThreaded())
            .sink("journal", Integer.class, journal::add, StageSpec.singleThreaded())
            .sink("risk", Integer.class, risk::add, StageSpec.singleThreaded())
            .edge("ingress", "fanout", EdgeSpec.mpscRing(32))
            .edge("fanout", "journal", EdgeSpec.spscRing(32))
            .edge("fanout", "risk", EdgeSpec.spscRing(32))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 16; i++) {
            ingress.emit(i);
        }
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(List.copyOf(journal), List.copyOf(risk));
        assertEquals(16, journal.size());
    }

    @Test
    void broadcastBranchIsolationRecordsSlowBranchDrops() throws Exception {
        final CountDownLatch slowEntered = new CountDownLatch(1);
        final CountDownLatch releaseSlow = new CountDownLatch(1);
        final List<Integer> fast = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("broadcast-isolation")
            .source("ingress", Integer.class)
            .broadcast("fanout", Integer.class, BroadcastSpec.<Integer>copy().withBranchIsolation(),
                StageSpec.singleThreaded())
            .sink("slow", Integer.class, ignored -> {
                slowEntered.countDown();
                try {
                    assertTrue(releaseSlow.await(5, TimeUnit.SECONDS));
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
            }, StageSpec.singleThreaded())
            .sink("fast", Integer.class, fast::add, StageSpec.singleThreaded())
            .edge("ingress", "fanout", EdgeSpec.mpscRing(256))
            .edge("fanout", "slow", EdgeSpec.spscRing(2))
            .edge("fanout", "fast", EdgeSpec.spscRing(256))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(0);
        assertTrue(slowEntered.await(5, TimeUnit.SECONDS));
        for (int i = 1; i < 128; i++) {
            ingress.emit(i);
        }
        assertEventually(() -> graph.metrics().stage("fanout").branchIsolationActions() > 0, Duration.ofSeconds(5));
        releaseSlow.countDown();
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertTrue(graph.metrics().stage("fanout").branchIsolationActions() > 0);
        assertTrue(graph.metrics().edge("fanout", "slow").branchIsolationActions() > 0);
        assertEquals(128, fast.size());
    }

    @Test
    void partitionByKeyPreservesPerKeyFifoWithinLane() throws Exception {
        final List<Integer> lane0 = Collections.synchronizedList(new ArrayList<>());
        final List<Integer> lane1 = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("partition")
            .source("ingress", Integer.class)
            .partition("partition", Integer.class, PartitionSpec.byKey(value -> value % 2, 2), StageSpec.singleThreaded())
            .sink("lane0", Integer.class, lane0::add, StageSpec.singleThreaded())
            .sink("lane1", Integer.class, lane1::add, StageSpec.singleThreaded())
            .edge("ingress", "partition", EdgeSpec.mpscRing(64))
            .edge("partition", "lane0", EdgeSpec.spscRing(64))
            .edge("partition", "lane1", EdgeSpec.spscRing(64))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 20; i++) {
            ingress.emit(i);
        }
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(List.of(0, 2, 4, 6, 8, 10, 12, 14, 16, 18), List.copyOf(lane0));
        assertEquals(List.of(1, 3, 5, 7, 9, 11, 13, 15, 17, 19), List.copyOf(lane1));
        assertEquals(10, graph.metrics().edge("partition", "lane0").laneSelections());
        assertEquals(10, graph.metrics().edge("partition", "lane1").laneSelections());
    }

    @Test
    void partitionHotLaneSignalsSkewThreshold() throws Exception {
        final List<Integer> lane = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("partition-skew")
            .source("ingress", Integer.class)
            .partition("partition", Integer.class,
                PartitionSpec.<Integer, Integer>byKey(value -> 0, 1).hotKeyThreshold(4),
                StageSpec.singleThreaded())
            .sink("lane0", Integer.class, lane::add, StageSpec.singleThreaded())
            .edge("ingress", "partition", EdgeSpec.mpscRing(16))
            .edge("partition", "lane0", EdgeSpec.spscRing(16))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 8; i++) {
            ingress.emit(i);
        }
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(1, graph.metrics().edge("partition", "lane0").hotKeySignals());
    }

    @Test
    void stampedAllOfJoinCorrelatesSourcesByStamp() throws Exception {
        final List<String> joined = Collections.synchronizedList(new ArrayList<>());
        final JoinSpec<String> joinSpec = JoinSpec.allOf(Phase4RoutingTest::formatJoin);
        final StaticGraph graph = graph("join-all")
            .stampedSource("orders", String.class)
            .stampedSource("risk", String.class)
            .join("join", String.class, joinSpec, StageSpec.singleThreaded())
            .sink("egress", String.class, joined::add, StageSpec.singleThreaded())
            .edge("orders", "join", EdgeSpec.mpscRing(32))
            .edge("risk", "join", EdgeSpec.mpscRing(32))
            .edge("join", "egress", EdgeSpec.spscRing(32))
            .build();

        graph.start();
        final Emitter<String> orders = graph.emitter("orders", String.class);
        final Emitter<String> risk = graph.emitter("risk", String.class);
        orders.emit("o0");
        orders.emit("o1");
        risk.emit("r0");
        risk.emit("r1");
        orders.close();
        risk.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(List.of("0:o0/r0", "1:o1/r1"), List.copyOf(joined));
        assertEquals(2, graph.metrics().stage("join").completedJoinGroups());
    }

    @Test
    void stampedSourceDoesNotBurnStampOnFailedTryEmit() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final List<Long> stamps = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("stable-stamps")
            .stampedSource("ingress", String.class)
            .sink("egress", Stamped.class, stamped -> {
                stamps.add(((Stamped<?>) stamped).stamp());
                entered.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(2))
            .build();

        graph.start();
        final Emitter<String> ingress = graph.emitter("ingress", String.class);
        ingress.emit("a");
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        ingress.emit("b");
        ingress.emit("c");
        assertTrue(!ingress.tryEmit("d"));
        release.countDown();
        ingress.emit("e", Duration.ofSeconds(5));
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(List.of(0L, 1L, 2L, 3L), List.copyOf(stamps));
    }

    @Test
    void joinsSupportAnyOfPartialTimeoutDuplicateFailAndHandleRelease() throws Exception {
        final List<String> any = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph anyGraph = graph("join-any")
            .stampedSource("left", String.class)
            .stampedSource("right", String.class)
            .join("join", String.class, JoinSpec.anyOf(group -> group.triggeringSource() + ":" + group.stamp()),
                StageSpec.singleThreaded())
            .sink("egress", String.class, any::add, StageSpec.singleThreaded())
            .edge("left", "join", EdgeSpec.mpscRing(8))
            .edge("right", "join", EdgeSpec.mpscRing(8))
            .edge("join", "egress", EdgeSpec.spscRing(8))
            .build();

        anyGraph.start();
        anyGraph.emitter("left", String.class).emit("l0");
        anyGraph.emitter("right", String.class).emit("r0");
        anyGraph.emitter("left", String.class).close();
        anyGraph.emitter("right", String.class).close();
        assertTrue(anyGraph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(1, any.size());
        assertTrue(any.get(0).endsWith(":0"));

        final List<String> partial = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph partialGraph = graph("join-partial")
            .stampedSource("left", String.class)
            .stampedSource("right", String.class)
            .join("join", String.class, JoinSpec.allOf(group -> "partial:" + group.stamp())
                .missingBranches(JoinSpec.MissingBranchPolicy.EMIT_PARTIAL), StageSpec.singleThreaded())
            .sink("egress", String.class, partial::add, StageSpec.singleThreaded())
            .edge("left", "join", EdgeSpec.mpscRing(8))
            .edge("right", "join", EdgeSpec.mpscRing(8))
            .edge("join", "egress", EdgeSpec.spscRing(8))
            .build();

        partialGraph.start();
        partialGraph.emitter("left", String.class).emit("l0");
        partialGraph.emitter("left", String.class).close();
        partialGraph.emitter("right", String.class).close();
        assertTrue(partialGraph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(List.of("partial:0"), List.copyOf(partial));
        assertEquals(1, partialGraph.metrics().stage("join").missingJoinBranches());

        final AtomicBoolean failed = new AtomicBoolean();
        final StaticGraph duplicateFail = graph("join-duplicate")
            .source("left", Integer.class)
            .source("right", Integer.class)
            .join("join", Integer.class, JoinSpec.<Integer>allOf(group -> 1)
                .stamp(ignored -> 1)
                .duplicates(JoinSpec.DuplicatePolicy.FAIL), StageSpec.singleThreaded())
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("left", "join", EdgeSpec.mpscRing(8))
            .edge("right", "join", EdgeSpec.mpscRing(8))
            .edge("join", "egress", EdgeSpec.spscRing(8))
            .build();
        duplicateFail.start();
        duplicateFail.emitter("left", Integer.class).emit(1);
        duplicateFail.emitter("left", Integer.class).emit(2);
        assertEventually(() -> duplicateFail.state() == GraphState.FAILED, Duration.ofSeconds(5));
        failed.set(duplicateFail.failure().isPresent());
        assertTrue(failed.get());

        final SlabPool<String> pool = new SlabPool<>("join-release", 1);
        final StaticGraph releaseGraph = graph("join-release")
            .source("left", SlabHandle.class)
            .source("right", SlabHandle.class)
            .join("join", String.class, JoinSpec.<String>allOf(group -> "unused").stamp(ignored -> 1),
                StageSpec.singleThreaded())
            .sink("egress", String.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("left", "join", EdgeSpec.mpscRing(8))
            .edge("right", "join", EdgeSpec.mpscRing(8))
            .edge("join", "egress", EdgeSpec.spscRing(8))
            .build();
        releaseGraph.start();
        releaseGraph.emitter("left", SlabHandle.class).emit(pool.acquire("payload"));
        releaseGraph.emitter("left", SlabHandle.class).close();
        releaseGraph.emitter("right", SlabHandle.class).close();
        assertTrue(releaseGraph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(0, pool.leakedCount());

        final List<String> aborted = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph abortGraph = graph("join-abort")
            .stampedSource("left", String.class)
            .stampedSource("right", String.class)
            .join("join", String.class, JoinSpec.allOf(group -> "partial")
                .missingBranches(JoinSpec.MissingBranchPolicy.EMIT_PARTIAL), StageSpec.singleThreaded())
            .sink("egress", String.class, aborted::add, StageSpec.singleThreaded())
            .edge("left", "join", EdgeSpec.mpscRing(8))
            .edge("right", "join", EdgeSpec.mpscRing(8))
            .edge("join", "egress", EdgeSpec.spscRing(8))
            .build();
        abortGraph.start();
        abortGraph.emitter("left", String.class).emit("l0");
        abortGraph.abort();
        assertTrue(abortGraph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(List.of(), List.copyOf(aborted));
    }

    @Test
    void slabHandleBroadcastReleasesEveryBranchHandle() throws Exception {
        final SlabPool<String> pool = new SlabPool<>("phase4", 4);
        final List<String> left = Collections.synchronizedList(new ArrayList<>());
        final List<String> right = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("slab-broadcast")
            .source("ingress", SlabHandle.class)
            .broadcast("fanout", SlabHandle.class, BroadcastSpec.slabHandles(), StageSpec.singleThreaded())
            .sink("left", SlabHandle.class, handle -> left.add(((SlabHandle<?>) handle).payload().toString()),
                StageSpec.singleThreaded())
            .sink("right", SlabHandle.class, handle -> right.add(((SlabHandle<?>) handle).payload().toString()),
                StageSpec.singleThreaded())
            .edge("ingress", "fanout", EdgeSpec.mpscRing(8))
            .edge("fanout", "left", EdgeSpec.spscRing(8))
            .edge("fanout", "right", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        final Emitter<SlabHandle> ingress = graph.emitter("ingress", SlabHandle.class);
        ingress.emit(pool.acquire("payload"));
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(List.of("payload"), List.copyOf(left));
        assertEquals(List.of("payload"), List.copyOf(right));
        assertEquals(0, pool.leakedCount());

        final SlabPool<String> stampedPool = new SlabPool<>("stamped-slab", 1);
        final StaticGraph stamped = graph("stamped-slab")
            .stampedSource("ingress", SlabHandle.class)
            .sink("egress", Stamped.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();
        stamped.start();
        stamped.emitter("ingress", SlabHandle.class).emit(stampedPool.acquire("payload"));
        stamped.emitter("ingress", SlabHandle.class).close();
        assertTrue(stamped.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(0, stampedPool.leakedCount());
    }

    @Test
    void dropLatestOverflowIsExplicitAndObservable() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("drop-latest")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, value -> {
                entered.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
                consumed.add(value);
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(2).overflow(OverflowPolicy.dropLatest()))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        ingress.emit(1);
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        ingress.emit(2);
        ingress.emit(3);
        ingress.emit(4);
        release.countDown();
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertTrue(graph.metrics().edge("ingress", "egress").droppedLatest() > 0);
        assertTrue(graph.metrics().droppedMessages() > 0);
    }

    @Test
    void dropOldestAndRedirectOverflowAreExplicitAndObservable() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final StaticGraph dropOldest = graph("drop-oldest")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, ignored -> {
                entered.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(2).overflow(OverflowPolicy.dropOldest()))
            .build();

        dropOldest.start();
        final Emitter<Integer> dropIngress = dropOldest.emitter("ingress", Integer.class);
        dropIngress.emit(1);
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        dropIngress.emit(2);
        dropIngress.emit(3);
        dropIngress.emit(4);
        release.countDown();
        dropIngress.close();
        assertTrue(dropOldest.awaitTermination(Duration.ofSeconds(5)));
        assertTrue(dropOldest.metrics().edge("ingress", "egress").droppedOldest() > 0);

        final CountDownLatch mainEntered = new CountDownLatch(1);
        final CountDownLatch releaseMain = new CountDownLatch(1);
        final List<Integer> dlq = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph redirect = graph("redirect")
            .source("ingress", Integer.class)
            .sink("main", Integer.class, ignored -> {
                mainEntered.countDown();
                try {
                    assertTrue(releaseMain.await(5, TimeUnit.SECONDS));
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
            }, StageSpec.singleThreaded())
            .sink("dlq", Integer.class, dlq::add, StageSpec.singleThreaded())
            .edge("ingress", "main", EdgeSpec.mpscRing(2).overflow(OverflowPolicy.redirectTo("dlq")))
            .edge("ingress", "dlq", EdgeSpec.mpscRing(8))
            .build();

        redirect.start();
        final Emitter<Integer> redirectIngress = redirect.emitter("ingress", Integer.class);
        redirectIngress.emit(1);
        assertTrue(mainEntered.await(5, TimeUnit.SECONDS));
        for (int i = 2; i < 8; i++) {
            redirectIngress.emit(i);
        }
        releaseMain.countDown();
        redirectIngress.close();
        assertTrue(redirect.awaitTermination(Duration.ofSeconds(5)));
        assertTrue(redirect.metrics().edge("ingress", "main").redirectedOffers() > 0);
        assertTrue(dlq.size() > 0);
    }

    @Test
    void coalescingOverflowIsExplicitAndObservable() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final StaticGraph graph = graph("coalesce")
            .source("ingress", Event.class)
            .sink("egress", Event.class, ignored -> {
                entered.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(ex);
                }
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(2).overflow(OverflowPolicy.coalesceBy(Event::key)))
            .build();

        graph.start();
        final Emitter<Event> ingress = graph.emitter("ingress", Event.class);
        ingress.emit(new Event(1, 1));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        ingress.emit(new Event(1, 2));
        ingress.emit(new Event(1, 3));
        ingress.emit(new Event(1, 4));
        release.countDown();
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertTrue(graph.metrics().edge("ingress", "egress").coalescedOffers() > 0);
        assertTrue(graph.metrics().coalescedMessages() > 0);
    }

    private static String formatJoin(final JoinGroup group) {
        final Stamped<?> order = group.value("orders", Stamped.class).orElseThrow();
        final Stamped<?> risk = group.value("risk", Stamped.class).orElseThrow();
        return group.stamp() + ":" + order.value() + "/" + risk.value();
    }

    private static void assertEventually(final java.util.function.BooleanSupplier condition, final Duration timeout)
        throws InterruptedException {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean());
    }

    private static StaticGraph.Builder graph(final String name) {
        return StaticGraph.builder(name).metrics(TEST_METRICS);
    }

    private record Event(int key, int value) {
    }
}
