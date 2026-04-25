package com.lattice.core;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.StaticGraph;
import com.lattice.placement.MemoryMode;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import com.lattice.wait.WaitSpec;
import java.time.Duration;

public class Main {

    public static void main(final String[] args) {
        final StaticGraph graph = StaticGraph.builder("phase1-orders")
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
                    .batch(BatchPolicy.disabled())
            )
            .sink("egress", ValidOrder.class, Main::consume, StageSpec.singleThreaded())
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
        ingress.emit(new Order(1, true));
        ingress.close();
        graph.stop(Duration.ofSeconds(5));
    }

    private static void consume(final ValidOrder order) {
        // Example sink intentionally empty.
    }

    public record Order(long id, boolean valid) {
    }

    public record ValidOrder(long id) {
    }
}
