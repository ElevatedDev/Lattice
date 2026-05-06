package io.github.elevateddev.lattice.core;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.edge.OverflowPolicy;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.placement.MemoryMode;
import io.github.elevateddev.lattice.stage.BatchPolicy;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.StageSpec;
import io.github.elevateddev.lattice.wait.WaitSpec;
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
