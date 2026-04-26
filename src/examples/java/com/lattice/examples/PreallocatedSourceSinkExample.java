package com.lattice.examples;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.PreallocationSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.stage.PreallocatedEmitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class PreallocatedSourceSinkExample {

    private static final int RING_SIZE = 64;
    private static final int POOL_SIZE = RING_SIZE << 1;

    private PreallocatedSourceSinkExample() {
    }

    public static void main(final String[] args) throws Exception {
        final CountDownLatch received = new CountDownLatch(3);
        final AtomicLong totalCents = new AtomicLong();

        final StaticGraph graph = StaticGraph.builder("example-preallocated-source-sink")
            .preallocatedSource(
                "ingress",
                ReusablePayment.class,
                PreallocationSpec.pool(ignored -> new ReusablePayment()).poolSize(POOL_SIZE)
            )
            .sink("settled", ReusablePayment.class, payment -> {
                totalCents.addAndGet(payment.amountCents);
                received.countDown();
            }, StageSpec.singleThreaded())
            // Preallocated sources intentionally use a single linear SPSC path so object reuse is bounded.
            .edge("ingress", "settled", EdgeSpec.spscRing(RING_SIZE))
            .build();

        graph.start();
        final PreallocatedEmitter<ReusablePayment> ingress =
            graph.preallocatedEmitter("ingress", ReusablePayment.class);
        for (int i = 0; i < 3; i++) {
            final ReusablePayment payment = ingress.claim();
            payment.paymentId = 10_000L + i;
            payment.amountCents = 1_250L + i;
            ingress.emit(payment);
        }
        ingress.close();

        await(received, graph, "payments");
        awaitTermination(graph);

        System.out.printf("settled=%d totalCents=%d poolSize=%d reuseBound=%d%n",
            3, totalCents.get(), ingress.poolSize(), ingress.reuseBound());
    }

    public static final class ReusablePayment {
        long paymentId;
        long amountCents;
    }

    private static void await(
        final CountDownLatch latch,
        final StaticGraph graph,
        final String name
    ) throws InterruptedException {
        if (!latch.await(5, TimeUnit.SECONDS)) {
            graph.abort();
            throw new IllegalStateException("timed out waiting for " + name);
        }
    }

    private static void awaitTermination(final StaticGraph graph) throws InterruptedException {
        if (!graph.awaitTermination(Duration.ofSeconds(5))) {
            graph.abort();
            throw new IllegalStateException("graph did not stop cleanly");
        }
    }
}
