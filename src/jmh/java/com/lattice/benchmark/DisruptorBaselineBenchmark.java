package com.lattice.benchmark;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class DisruptorBaselineBenchmark {

    private static final int RING_SIZE = 8192;
    private static final int BATCH_SIZE = 32;

    @Benchmark
    public void disruptorSingleProducer(final SingleProducerState state) {
        final long started = System.nanoTime();
        publish(state.ringBuffer, started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    @Group("disruptorMultiProducer")
    @GroupThreads(4)
    public void disruptorMultiProducer(final MultiProducerState state) {
        final long started = System.nanoTime();
        publish(state.ringBuffer, started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void disruptorBatchedSingleProducer(final BatchedSingleProducerState state) {
        final long started = System.nanoTime();
        final long hi = state.ringBuffer.next(BATCH_SIZE);
        final long lo = hi - BATCH_SIZE + 1L;
        try {
            for (long sequence = lo; sequence <= hi; sequence++) {
                state.ringBuffer.get(sequence).value = started;
            }
        } finally {
            state.ringBuffer.publish(lo, hi);
        }
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void disruptorTwoConsumerMulticast(final MulticastState state) {
        final long started = System.nanoTime();
        publish(state.ringBuffer, started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void disruptorFourConsumerMulticast(final FourConsumerMulticastState state) {
        final long started = System.nanoTime();
        publish(state.ringBuffer, started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    @Benchmark
    public void disruptorValidateJournalRiskCommit(final ValidateJournalRiskCommitState state) {
        final long started = System.nanoTime();
        publish(state.ringBuffer, started);
        state.enqueueLatency.recordElapsedSince(started);
    }

    private static void publish(final RingBuffer<ValueEvent> ringBuffer, final long value) {
        final long sequence = ringBuffer.next();
        try {
            ringBuffer.get(sequence).value = value;
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    @State(Scope.Benchmark)
    public static class SingleProducerState {

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = createDisruptor(ProducerType.SINGLE, consumed, endToEndLatency);
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            enqueueLatency.print("disruptorSingleProducer", "enqueue");
            endToEndLatency.print("disruptorSingleProducer", "endToEnd");
        }
    }

    @State(Scope.Benchmark)
    public static class MultiProducerState {

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = createDisruptor(ProducerType.MULTI, consumed, endToEndLatency);
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            enqueueLatency.print("disruptorMultiProducer", "enqueue");
            endToEndLatency.print("disruptorMultiProducer", "endToEnd");
        }
    }

    @State(Scope.Benchmark)
    public static class BatchedSingleProducerState {

        final AtomicLong consumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            disruptor = createDisruptor(ProducerType.SINGLE, consumed, endToEndLatency);
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            enqueueLatency.print("disruptorBatchedSingleProducer", "enqueue");
            endToEndLatency.print("disruptorBatchedSingleProducer", "endToEnd");
        }
    }

    @State(Scope.Benchmark)
    public static class MulticastState {

        final AtomicLong journalConsumed = new AtomicLong();
        final AtomicLong riskConsumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            final EventFactory<ValueEvent> factory = ValueEvent::new;
            final ThreadFactory threadFactory = runnable -> {
                final Thread thread = Thread.ofPlatform().unstarted(runnable);
                thread.setName("disruptor-multicast-baseline");
                thread.setDaemon(true);
                return thread;
            };
            disruptor = new Disruptor<>(
                factory,
                RING_SIZE,
                threadFactory,
                ProducerType.SINGLE,
                new YieldingWaitStrategy()
            );
            disruptor.handleEventsWith(
                (event, sequence, endOfBatch) -> {
                    journalConsumed.incrementAndGet();
                    endToEndLatency.recordElapsedSince(event.value);
                },
                (event, sequence, endOfBatch) -> riskConsumed.incrementAndGet()
            );
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            enqueueLatency.print("disruptorTwoConsumerMulticast", "enqueue");
            endToEndLatency.print("disruptorTwoConsumerMulticast", "endToEnd");
        }
    }

    @State(Scope.Benchmark)
    public static class FourConsumerMulticastState {

        final AtomicLong firstConsumed = new AtomicLong();
        final AtomicLong secondConsumed = new AtomicLong();
        final AtomicLong thirdConsumed = new AtomicLong();
        final AtomicLong fourthConsumed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            final EventFactory<ValueEvent> factory = ValueEvent::new;
            final ThreadFactory threadFactory = namedThreadFactory("disruptor-four-consumer-baseline");
            disruptor = new Disruptor<>(
                factory,
                RING_SIZE,
                threadFactory,
                ProducerType.SINGLE,
                new YieldingWaitStrategy()
            );
            disruptor.handleEventsWith(
                countingHandler(firstConsumed, endToEndLatency),
                countingHandler(secondConsumed),
                countingHandler(thirdConsumed),
                countingHandler(fourthConsumed)
            );
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            enqueueLatency.print("disruptorFourConsumerMulticast", "enqueue");
            endToEndLatency.print("disruptorFourConsumerMulticast", "endToEnd");
        }
    }

    @State(Scope.Benchmark)
    public static class ValidateJournalRiskCommitState {

        final AtomicLong validated = new AtomicLong();
        final AtomicLong journaled = new AtomicLong();
        final AtomicLong riskChecked = new AtomicLong();
        final AtomicLong committed = new AtomicLong();
        final LatencyRecorder enqueueLatency = new LatencyRecorder();
        final LatencyRecorder endToEndLatency = new LatencyRecorder();
        Disruptor<ValueEvent> disruptor;
        RingBuffer<ValueEvent> ringBuffer;

        @Setup(Level.Trial)
        public void setup() {
            final EventFactory<ValueEvent> factory = ValueEvent::new;
            final ThreadFactory threadFactory = namedThreadFactory("disruptor-dependency-baseline");
            disruptor = new Disruptor<>(
                factory,
                RING_SIZE,
                threadFactory,
                ProducerType.SINGLE,
                new YieldingWaitStrategy()
            );

            final EventHandlerGroup<ValueEvent> validate = disruptor.handleEventsWith(countingHandler(validated));
            validate
                .then(countingHandler(journaled), countingHandler(riskChecked))
                .then(countingHandler(committed, endToEndLatency));
            ringBuffer = disruptor.start();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            disruptor.shutdown();
            enqueueLatency.print("disruptorValidateJournalRiskCommit", "enqueue");
            endToEndLatency.print("disruptorValidateJournalRiskCommit", "endToEnd");
        }
    }

    private static Disruptor<ValueEvent> createDisruptor(
        final ProducerType producerType,
        final AtomicLong consumed,
        final LatencyRecorder endToEndLatency
    ) {
        final EventFactory<ValueEvent> factory = ValueEvent::new;
        final ThreadFactory threadFactory = namedThreadFactory("disruptor-baseline");
        final Disruptor<ValueEvent> disruptor = new Disruptor<>(
            factory,
            RING_SIZE,
            threadFactory,
            producerType,
            new YieldingWaitStrategy()
        );
        disruptor.handleEventsWith(countingHandler(consumed, endToEndLatency));
        return disruptor;
    }

    private static ThreadFactory namedThreadFactory(final String name) {
        return runnable -> {
            final Thread thread = Thread.ofPlatform().unstarted(runnable);
            thread.setName(name);
            thread.setDaemon(true);
            return thread;
        };
    }

    private static EventHandler<ValueEvent> countingHandler(final AtomicLong counter) {
        return (event, sequence, endOfBatch) -> counter.incrementAndGet();
    }

    private static EventHandler<ValueEvent> countingHandler(
        final AtomicLong counter,
        final LatencyRecorder endToEndLatency
    ) {
        return (event, sequence, endOfBatch) -> {
            counter.incrementAndGet();
            endToEndLatency.recordElapsedSince(event.value);
        };
    }

    public static final class ValueEvent {
        long value;
    }
}
