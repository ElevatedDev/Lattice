package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.PreallocationSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.routing.Stamped;
import com.lattice.stage.Emitter;
import com.lattice.stage.PreallocatedEmitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphConcurrencyTest {

    @Test
    void graphLevelMpscSourceDeliversConcurrentEmittersExactlyOnceAndPerProducerFifo() throws Exception {
        final int producers = 6;
        final int perProducer = 750;
        final int total = producers * perProducer;
        final Set<Integer> seen = Collections.synchronizedSet(new HashSet<>());
        final int[] nextExpectedByProducer = new int[producers];
        final AtomicReference<String> orderingFailure = new AtomicReference<>();

        final StaticGraph graph = StaticGraph.builder("graph-mpsc-contention")
            .source("ingress", EncodedMessage.class)
            .stage("identity", EncodedMessage.class, EncodedMessage.class, (message, out, ctx) -> out.push(message),
                StageSpec.singleThreaded())
            .sink("egress", EncodedMessage.class, message -> {
                final int encoded = message.encoded();
                final int producer = producer(encoded);
                final int sequence = sequence(encoded);
                if (sequence != nextExpectedByProducer[producer]) {
                    orderingFailure.compareAndSet(null,
                        "producer " + producer + " expected " + nextExpectedByProducer[producer]
                            + " but saw " + sequence);
                }
                nextExpectedByProducer[producer]++;
                seen.add(encoded);
            }, StageSpec.singleThreaded())
            .edge("ingress", "identity", EdgeSpec.mpscRing(256))
            .edge("identity", "egress", EdgeSpec.spscRing(256))
            .build();

        graph.start();
        final Emitter<EncodedMessage> ingress = graph.emitter("ingress", EncodedMessage.class);
        runConcurrentProducers(producers, producer -> {
            for (int sequence = 0; sequence < perProducer; sequence++) {
                ingress.emit(new EncodedMessage(encode(producer, sequence)));
            }
        });
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(10)));
        assertEquals(total, seen.size());
        assertEquals(null, orderingFailure.get());
        for (int producer = 0; producer < producers; producer++) {
            assertEquals(perProducer, nextExpectedByProducer[producer], "producer " + producer);
            for (int sequence = 0; sequence < perProducer; sequence++) {
                assertTrue(seen.contains(encode(producer, sequence)));
            }
        }
    }

    @Test
    void multiProducerStampedSourceAssignsUniqueContiguousStampsUnderContention() throws Exception {
        final int producers = 5;
        final int perProducer = 400;
        final int total = producers * perProducer;
        final List<Long> stamps = Collections.synchronizedList(new ArrayList<>());

        final StaticGraph graph = StaticGraph.builder("stamped-source-contention")
            .stampedSource("ingress", EncodedMessage.class)
            .sink("egress", Stamped.class, stamped -> stamps.add(((Stamped<?>) stamped).stamp()),
                StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(512))
            .build();

        graph.start();
        final Emitter<EncodedMessage> ingress = graph.emitter("ingress", EncodedMessage.class);
        runConcurrentProducers(producers, producer -> {
            for (int sequence = 0; sequence < perProducer; sequence++) {
                ingress.emit(new EncodedMessage(encode(producer, sequence)));
            }
        });
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(10)));
        final List<Long> sorted = new ArrayList<>(stamps);
        Collections.sort(sorted);
        assertEquals(total, sorted.size());
        for (int i = 0; i < total; i++) {
            assertEquals(i, sorted.get(i));
        }
    }

    @Test
    void preallocatedTryEmitRetainsOutstandingClaimAcrossBackpressureUntilRetrySucceeds() throws Exception {
        final CountDownLatch sinkEntered = new CountDownLatch(1);
        final CountDownLatch releaseSink = new CountDownLatch(1);
        final List<Long> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = StaticGraph.builder("preallocated-backpressure-retry")
            .preallocatedSource("ingress", MutableSignal.class,
                PreallocationSpec.pool(ignored -> new MutableSignal()).poolSize(128))
            .sink("egress", MutableSignal.class, signal -> {
                sinkEntered.countDown();
                await(releaseSink);
                consumed.add(signal.sequence);
            }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.spscRing(1))
            .build();

        graph.start();
        final PreallocatedEmitter<MutableSignal> ingress =
            graph.preallocatedEmitter("ingress", MutableSignal.class);
        final MutableSignal first = ingress.claim();
        first.sequence = 1L;
        ingress.emit(first);
        assertTrue(sinkEntered.await(5, TimeUnit.SECONDS));

        final MutableSignal second = ingress.claim();
        second.sequence = 2L;
        assertFalse(ingress.tryEmit(second));
        assertThrows(RuntimeException.class, ingress::claim);

        releaseSink.countDown();
        assertEventually(() -> ingress.tryEmit(second), Duration.ofSeconds(5));
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(10)));
        assertEquals(List.of(1L, 2L), List.copyOf(consumed));
    }

    @Test
    void manyIndependentGraphsCanRunInParallelWithoutCrossGraphStateLeakage() throws Exception {
        final int graphs = 8;
        final ExecutorService executor = Executors.newFixedThreadPool(graphs);
        final List<Future<List<Integer>>> futures = new ArrayList<>();
        for (int graphIndex = 0; graphIndex < graphs; graphIndex++) {
            final int index = graphIndex;
            futures.add(executor.submit(() -> runIndependentGraph(index)));
        }

        for (int graphIndex = 0; graphIndex < graphs; graphIndex++) {
            final List<Integer> outputs = futures.get(graphIndex).get(10, TimeUnit.SECONDS);
            assertEquals(100, outputs.size());
            for (int i = 0; i < outputs.size(); i++) {
                assertEquals((graphIndex * 1_000) + i + 1, outputs.get(i));
            }
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }

    private static List<Integer> runIndependentGraph(final int graphIndex) throws Exception {
        final List<Integer> outputs = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = StaticGraph.builder("parallel-graph-" + graphIndex)
            .source("ingress", Integer.class)
            .stage("plusOne", Integer.class, Integer.class, (value, out, ctx) -> out.push(value + 1),
                StageSpec.singleThreaded())
            .sink("egress", Integer.class, outputs::add, StageSpec.singleThreaded())
            .edge("ingress", "plusOne", EdgeSpec.mpscRing(64))
            .edge("plusOne", "egress", EdgeSpec.spscRing(64))
            .build();
        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 100; i++) {
            ingress.emit((graphIndex * 1_000) + i);
        }
        ingress.close();
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        return List.copyOf(outputs);
    }

    private static void runConcurrentProducers(final int producers, final ProducerAction action) throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(producers);
        final CountDownLatch ready = new CountDownLatch(producers);
        final CountDownLatch start = new CountDownLatch(1);
        final List<Future<?>> futures = new ArrayList<>();
        for (int producer = 0; producer < producers; producer++) {
            final int producerId = producer;
            futures.add(executor.submit(() -> {
                ready.countDown();
                assertTrue(start.await(5, TimeUnit.SECONDS));
                action.run(producerId);
                return null;
            }));
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        start.countDown();
        for (final Future<?> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }

    private static void await(final CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError(ex);
        }
    }

    private static void assertEventually(final CheckedBooleanSupplier condition, final Duration timeout)
        throws Exception {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean());
    }

    private static int encode(final int producer, final int sequence) {
        return (producer << 20) | sequence;
    }

    private static int producer(final int encoded) {
        return encoded >>> 20;
    }

    private static int sequence(final int encoded) {
        return encoded & ((1 << 20) - 1);
    }

    @FunctionalInterface
    private interface ProducerAction {
        void run(int producer) throws Exception;
    }

    @FunctionalInterface
    private interface CheckedBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    private record EncodedMessage(int encoded) {
    }

    public static final class MutableSignal {
        long sequence;
    }
}
