package com.lattice.examples;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.StaticGraph;
import com.lattice.placement.MemoryMode;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import com.lattice.wait.WaitSpec;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class OrdersPipelineExample {

    private OrdersPipelineExample() {
    }

    public static void main(final String[] args) throws Exception {
        final CountDownLatch consumed = new CountDownLatch(1);
        final StaticGraph graph = StaticGraph.builder("orders-example")
            .source("ingress", Order.class)
            .stage(
                "validate",
                Order.class,
                ValidOrder.class,
                (order, out, ctx) -> {
                    if (order.valid()) {
                        out.push(new ValidOrder(order.id()));
                    }
                },
                StageSpec.singleThreaded()
            )
            .sink("egress", ValidOrder.class, ignored -> consumed.countDown(), StageSpec.singleThreaded())
            .edge(
                "ingress",
                "validate",
                EdgeSpec.mpscRing(1024)
                    .wait(WaitSpec.phased(10_000, 50, Duration.ofNanos(500)))
                    .overflow(OverflowPolicy.block())
                    .memory(MemoryMode.onHeapSlots())
            )
            .edge(
                "validate",
                "egress",
                EdgeSpec.spscRing(1024)
                    .wait(WaitSpec.phased(10_000, 50, Duration.ofNanos(500)))
                    .overflow(OverflowPolicy.block())
                    .memory(MemoryMode.onHeapSlots())
            )
            .build();

        graph.start();
        final Emitter<Order> ingress = graph.emitter("ingress", Order.class);
        ingress.emit(new Order(1L, true));
        ingress.close();

        if (!consumed.await(5, TimeUnit.SECONDS)) {
            graph.abort();
            throw new IllegalStateException("example timed out waiting for the sink");
        }
        if (!graph.awaitTermination(Duration.ofSeconds(5))) {
            graph.abort();
            throw new IllegalStateException("example graph did not stop cleanly");
        }
    }

    public record Order(long id, boolean valid) {
    }

    public record ValidOrder(long id) {
    }
}
