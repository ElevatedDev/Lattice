package io.github.elevateddev.lattice.benchmark;

import io.github.elevateddev.lattice.edge.EdgeSpec;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class Phase4TopologyBenchmark {

    @Benchmark
    public void dispatchFanout(final DispatchState state) {
        final long started = System.nanoTime();
        state.emitter.emit(started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void broadcastTwoBranch(final BroadcastState state) {
        final long started = System.nanoTime();
        state.emitter.emit(started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void broadcastFourBranch(final BroadcastFourState state) {
        final long started = System.nanoTime();
        state.emitter.emit(started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void validateJournalRiskCommit(final ValidateJournalRiskCommitState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void partitionFourLanes(final PartitionState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void hotKeyPartitionSkew(final HotKeyPartitionState state) {
        final long started = System.nanoTime();
        state.emitter.emit(new Order(state.next++, started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void stampedAllOfJoin(final JoinState state) {
        final long started = System.nanoTime();
        state.left.emit(started);
        state.right.emit(started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void slabHandleBroadcast(final SlabState state) {
        final long started = System.nanoTime();
        state.emitter.emit(state.pool.acquire(started));
        state.enqueueLatency.recordElapsedSince(started);
    }

    @State(Scope.Benchmark)
    public static class DispatchState {
        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Long> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("bench-dispatch")
                .source("ingress", Long.class)
                .dispatch("dispatch", Long.class, DispatchSpec.roundRobin(), StageSpec.singleThreaded())
                .sink("a", Long.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("b", Long.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .edge("ingress", "dispatch", EdgeSpec.mpscRing(8192))
                .edge("dispatch", "a", EdgeSpec.spscRing(8192))
                .edge("dispatch", "b", EdgeSpec.spscRing(8192))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Long.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("dispatchFanout", "enqueue");
        }
    }

    @State(Scope.Benchmark)
    public static class BroadcastState {
        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Long> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("bench-broadcast")
                .source("ingress", Long.class)
                .broadcast("fanout", Long.class, BroadcastSpec.copy(), StageSpec.singleThreaded())
                .sink("journal", Long.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("risk", Long.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .edge("ingress", "fanout", EdgeSpec.mpscRing(8192))
                .edge("fanout", "journal", EdgeSpec.spscRing(8192))
                .edge("fanout", "risk", EdgeSpec.spscRing(8192))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Long.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("broadcastTwoBranch", "enqueue");
        }
    }

    @State(Scope.Benchmark)
    public static class BroadcastFourState {
        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Long> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("bench-broadcast-four")
                .source("ingress", Long.class)
                .broadcast("fanout", Long.class, BroadcastSpec.copy(), StageSpec.singleThreaded())
                .sink("a", Long.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("b", Long.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("c", Long.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("d", Long.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .edge("ingress", "fanout", EdgeSpec.mpscRing(8192))
                .edge("fanout", "a", EdgeSpec.spscRing(8192))
                .edge("fanout", "b", EdgeSpec.spscRing(8192))
                .edge("fanout", "c", EdgeSpec.spscRing(8192))
                .edge("fanout", "d", EdgeSpec.spscRing(8192))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Long.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("broadcastFourBranch", "enqueue");
        }
    }

    @State(Scope.Benchmark)
    public static class ValidateJournalRiskCommitState {
        final AtomicLong committed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Order> emitter;
        long next;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("bench-validate-journal-risk-commit")
                .source("ingress", Order.class)
                .stage("validate", Order.class, Order.class, (order, out, ctx) -> out.push(order),
                    StageSpec.singleThreaded())
                .broadcast("fanout", Order.class, BroadcastSpec.copy(order -> order), StageSpec.singleThreaded())
                .sink("journal", Order.class, ignored -> { }, StageSpec.singleThreaded())
                .stage("risk", Order.class, Order.class, (order, out, ctx) -> out.push(order),
                    StageSpec.singleThreaded())
                .sink("commit", Order.class, order -> {
                    committed.incrementAndGet();
                    endToEndLatency.recordElapsedSince(order.emittedAtNanos());
                }, StageSpec.singleThreaded())
                .edge("ingress", "validate", EdgeSpec.mpscRing(8192))
                .edge("validate", "fanout", EdgeSpec.spscRing(8192))
                .edge("fanout", "journal", EdgeSpec.spscRing(8192))
                .edge("fanout", "risk", EdgeSpec.spscRing(8192))
                .edge("risk", "commit", EdgeSpec.spscRing(8192))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Order.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("validateJournalRiskCommit", "enqueue");
            endToEndLatency.print("validateJournalRiskCommit", "endToEnd");
        }
    }

    @State(Scope.Benchmark)
    public static class PartitionState {
        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Order> emitter;
        long next;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("bench-partition")
                .source("ingress", Order.class)
                .partition("partition", Order.class, PartitionSpec.byKey(Order::id, 4), StageSpec.singleThreaded())
                .sink("lane0", Order.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("lane1", Order.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("lane2", Order.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("lane3", Order.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .edge("ingress", "partition", EdgeSpec.mpscRing(8192))
                .edge("partition", "lane0", EdgeSpec.spscRing(8192))
                .edge("partition", "lane1", EdgeSpec.spscRing(8192))
                .edge("partition", "lane2", EdgeSpec.spscRing(8192))
                .edge("partition", "lane3", EdgeSpec.spscRing(8192))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Order.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("partitionFourLanes", "enqueue");
        }
    }

    @State(Scope.Benchmark)
    public static class HotKeyPartitionState {
        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Order> emitter;
        long next;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("bench-hot-key-partition")
                .source("ingress", Order.class)
                .partition("partition", Order.class,
                    PartitionSpec.<Order, Long>byKey(ignored -> 0L, 4).hotKeyThreshold(1000),
                    StageSpec.singleThreaded())
                .sink("lane0", Order.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("lane1", Order.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("lane2", Order.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("lane3", Order.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .edge("ingress", "partition", EdgeSpec.mpscRing(8192))
                .edge("partition", "lane0", EdgeSpec.spscRing(8192))
                .edge("partition", "lane1", EdgeSpec.spscRing(8192))
                .edge("partition", "lane2", EdgeSpec.spscRing(8192))
                .edge("partition", "lane3", EdgeSpec.spscRing(8192))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Order.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("hotKeyPartitionSkew", "enqueue");
        }
    }

    @State(Scope.Benchmark)
    public static class JoinState {
        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        StaticGraph graph;
        Emitter<Long> left;
        Emitter<Long> right;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("bench-join")
                .stampedSource("left", Long.class)
                .stampedSource("right", Long.class)
                .join("join", Long.class, JoinSpec.allOf(Phase4TopologyBenchmark::joinedStamp),
                    StageSpec.singleThreaded())
                .sink("egress", Long.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .edge("left", "join", EdgeSpec.mpscRing(8192))
                .edge("right", "join", EdgeSpec.mpscRing(8192))
                .edge("join", "egress", EdgeSpec.spscRing(8192))
                .build();
            graph.start();
            left = graph.emitter("left", Long.class);
            right = graph.emitter("right", Long.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            left.close();
            right.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("stampedAllOfJoin", "enqueue");
        }
    }

    @State(Scope.Benchmark)
    public static class SlabState {
        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final SlabPool<Long> pool = new SlabPool<>("bench-slab", 8192);
        StaticGraph graph;
        Emitter<SlabHandle> emitter;

        @Setup(Level.Trial)
        public void setup() {
            graph = StaticGraph.builder("bench-slab")
                .source("ingress", SlabHandle.class)
                .broadcast("fanout", SlabHandle.class, BroadcastSpec.slabHandles(), StageSpec.singleThreaded())
                .sink("a", SlabHandle.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .sink("b", SlabHandle.class, ignored -> consumed.incrementAndGet(), StageSpec.singleThreaded())
                .edge("ingress", "fanout", EdgeSpec.mpscRing(8192))
                .edge("fanout", "a", EdgeSpec.spscRing(8192))
                .edge("fanout", "b", EdgeSpec.spscRing(8192))
                .build();
            graph.start();
            emitter = graph.emitter("ingress", SlabHandle.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(Duration.ofSeconds(10));
            enqueueLatency.print("slabHandleBroadcast", "enqueue");
        }
    }

    private static Long joinedStamp(final JoinGroup group) {
        return ((Number) group.stamp()).longValue();
    }

    public record Order(long id, long emittedAtNanos) {
    }
}
