package com.lattice.benchmark;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.SourceMode;
import com.lattice.graph.StaticGraph;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import com.lattice.wait.WaitSpec;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
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

/**
 * Realistic-workload pipeline benchmark designed to model a low-latency trading-style
 * pipeline rather than the synthetic "increment one int" stages of
 * {@link ApplesToApplesDisruptorBenchmark}. Each stage performs ~50–100 ns of
 * representative CPU work:
 *
 * <ol>
 *   <li><b>parse</b> — extracts a synthetic key from the payload (multiply-and-mix);</li>
 *   <li><b>enrich</b> — looks up a side-table by key, accumulates a running checksum;</li>
 *   <li><b>risk</b> — runs a small set of branchy threshold tests against the enriched value;</li>
 *   <li><b>serialize</b> — writes a fixed-size representation into a per-event scratch buffer.</li>
 * </ol>
 *
 * <p>Side data and scratch buffers live on the {@link Order} payload so the steady-state
 * allocation is zero (events are pooled, the same as the apples-to-apples benchmark). The
 * Lattice and Disruptor variants run identical work; the only difference is the dispatch
 * runtime. This measures how much of the dispatch advantage carries over once each event
 * spends meaningful CPU time inside the user code — i.e., a realistic workload rather than
 * a microbenchmark of the queue itself.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class RealisticWorkloadBenchmark {

    private static final int RING_SIZE = 8192;
    private static final int POOL_SIZE = 1 << 18;
    private static final int SIDE_TABLE_SIZE = 1024;
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final WaitSpec WAIT = WaitSpec.phased(100, 1_000_000_000, Duration.ZERO);
    private static final StageSpec STAGE = StageSpec.singleThreaded().wait(WAIT);
    private static final EdgeSpec SPSC_FUSIBLE = EdgeSpec.spscRing(RING_SIZE).wait(WAIT);

    /** Side table shared by both runtimes; immutable after setup. */
    private static final long[] SIDE_TABLE;

    static {
        SIDE_TABLE = new long[SIDE_TABLE_SIZE];
        long mix = 0x9E3779B97F4A7C15L;
        for (int i = 0; i < SIDE_TABLE_SIZE; i++) {
            mix ^= (long) i * 0x100000001B3L;
            SIDE_TABLE[i] = mix;
        }
    }

    @Benchmark
    public void latticeRealisticPipeline(final LatticeRealisticState state) {
        state.emitter.emit(state.nextOrder());
    }

    @Benchmark
    public void disruptorRealisticPipeline(final DisruptorRealisticState state) {
        final Order order = state.nextOrder();
        final long sequence = state.ringBuffer.next();
        try {
            final OrderEvent event = state.ringBuffer.get(sequence);
            event.copyFrom(order);
        } finally {
            state.ringBuffer.publish(sequence);
        }
    }

    static void parse(final Order order) {
        // Synthetic key extraction: nonlinear bit mix on the raw payload field.
        long mix = order.raw;
        mix ^= mix >>> 33;
        mix *= 0xFF51AFD7ED558CCDL;
        mix ^= mix >>> 33;
        mix *= 0xC4CEB9FE1A85EC53L;
        mix ^= mix >>> 33;
        order.key = mix;
    }

    static void enrich(final Order order) {
        // Side-table lookup keyed off the parsed mix; accumulate into a running checksum.
        final int slot = (int) (order.key & (SIDE_TABLE_SIZE - 1));
        final long enriched = SIDE_TABLE[slot] ^ order.key;
        order.enriched = enriched;
        order.checksum = order.checksum * 31L + enriched;
    }

    static void risk(final Order order) {
        // Branchy threshold sweep that defeats trivial elimination by the JIT.
        final long v = order.enriched;
        int flags = 0;
        if (v > 0L) flags |= 1;
        if ((v & 0xFFFFL) > 0x8000L) flags |= 2;
        if ((v >>> 16) % 7L == 0L) flags |= 4;
        if ((v >>> 24) % 13L == 0L) flags |= 8;
        if (Long.bitCount(v) > 32) flags |= 16;
        order.riskFlags = flags;
        order.passed = (flags & 0b11000) == 0;
    }

    static void serialize(final Order order) {
        // Fixed-size writeout into the per-event scratch buffer (zero allocation).
        final byte[] buf = order.scratch;
        final long v = order.enriched;
        buf[0] = (byte) (v);
        buf[1] = (byte) (v >>> 8);
        buf[2] = (byte) (v >>> 16);
        buf[3] = (byte) (v >>> 24);
        buf[4] = (byte) (v >>> 32);
        buf[5] = (byte) (v >>> 40);
        buf[6] = (byte) (v >>> 48);
        buf[7] = (byte) (v >>> 56);
        buf[8] = (byte) order.riskFlags;
        buf[9] = order.passed ? (byte) 1 : (byte) 0;
    }

    @State(Scope.Benchmark)
    public static class LatticeRealisticState extends PooledOrders {
        final AtomicLong consumed = new AtomicLong();
        StaticGraph graph;
        Emitter<Order> emitter;

        @Setup(Level.Trial)
        public void setup() {
            // Inline-source fusion is on by default; the entire chain runs on the producer.
            System.setProperty("lattice.fusion.enabled", "true");
            graph = StaticGraph.builder("realistic-lattice")
                .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
                .stage("parse", Order.class, Order.class, (order, out, ctx) -> {
                    parse(order);
                    out.push(order);
                }, STAGE)
                .stage("enrich", Order.class, Order.class, (order, out, ctx) -> {
                    enrich(order);
                    out.push(order);
                }, STAGE)
                .stage("risk", Order.class, Order.class, (order, out, ctx) -> {
                    risk(order);
                    out.push(order);
                }, STAGE)
                .sink("egress", Order.class, order -> {
                    serialize(order);
                    consumed.incrementAndGet();
                }, STAGE)
                .edge("ingress", "parse", SPSC_FUSIBLE)
                .edge("parse", "enrich", SPSC_FUSIBLE)
                .edge("enrich", "risk", SPSC_FUSIBLE)
                .edge("risk", "egress", SPSC_FUSIBLE)
                .build();
            graph.start();
            emitter = graph.emitter("ingress", Order.class);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            emitter.close();
            graph.stop(STOP_TIMEOUT);
            if (consumed.get() == 0L) {
                throw new IllegalStateException("realistic-lattice consumed nothing");
            }
        }
    }

    @State(Scope.Benchmark)
    public static class DisruptorRealisticState extends PooledOrders {
        final AtomicLong consumed = new AtomicLong();
        Disruptor<OrderEvent> disruptor;
        RingBuffer<OrderEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            final EventFactory<OrderEvent> factory = OrderEvent::new;
            disruptor = new Disruptor<>(
                factory,
                RING_SIZE,
                namedThreadFactory("realistic-disruptor"),
                ProducerType.SINGLE,
                new YieldingWaitStrategy()
            );
            // Disruptor's natural fused arrangement: a single handler does all four stages.
            // This is the most favourable shape for Disruptor (no inter-stage handoff).
            final EventHandler<OrderEvent> fused = (event, sequence, endOfBatch) -> {
                final Order o = event.order;
                parse(o);
                enrich(o);
                risk(o);
                serialize(o);
                consumed.incrementAndGet();
            };
            disruptor.handleEventsWith(fused);
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            if (consumed.get() == 0L) {
                throw new IllegalStateException("realistic-disruptor consumed nothing");
            }
        }
    }

    // ----- Pooled events -----

    public static class PooledOrders {
        private final Order[] orders = new Order[POOL_SIZE];
        private int cursor;

        public PooledOrders() {
            for (int i = 0; i < orders.length; i++) {
                final Order o = new Order();
                o.id = i;
                o.raw = (long) i * 0x9E3779B97F4A7C15L;
                orders[i] = o;
            }
        }

        Order nextOrder() {
            final Order o = orders[cursor++ & (orders.length - 1)];
            o.checksum = 0L;
            return o;
        }
    }

    public static final class Order {
        public long id;
        public long raw;
        public long key;
        public long enriched;
        public long checksum;
        public int riskFlags;
        public boolean passed;
        public final byte[] scratch = new byte[16];
    }

    /** Disruptor event slot referencing the same {@link Order} produced by the benchmark. */
    public static final class OrderEvent {
        public Order order;

        public void copyFrom(final Order source) {
            this.order = source;
        }
    }

    private static ThreadFactory namedThreadFactory(final String name) {
        return runnable -> {
            final Thread thread = Thread.ofPlatform().unstarted(runnable);
            thread.setName(name);
            thread.setDaemon(true);
            return thread;
        };
    }
}

