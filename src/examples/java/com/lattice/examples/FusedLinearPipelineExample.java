package com.lattice.examples;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.SourceMode;
import com.lattice.graph.StaticGraph;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class FusedLinearPipelineExample {

    private static final int RING_SIZE = 256;

    private FusedLinearPipelineExample() {
    }

    public static void main(final String[] args) throws Exception {
        final CountDownLatch captured = new CountDownLatch(1);
        final AtomicReference<CapturedOrder> result = new AtomicReference<>();

        final StaticGraph graph = buildWithFusion(captured, result);
        graph.start();
        final Emitter<Order> ingress = graph.emitter("ingress", Order.class);
        ingress.emit(new Order(42L, "  buy 100 shares  ", true));
        ingress.close();

        await(captured, graph, "captured order");
        awaitTermination(graph);

        System.out.println(result.get());
    }

    private static StaticGraph buildWithFusion(
        final CountDownLatch captured,
        final AtomicReference<CapturedOrder> result
    ) {
        final String previousFusion = System.getProperty("lattice.fusion.enabled");
        System.setProperty("lattice.fusion.enabled", "true");
        try {
            return StaticGraph.builder("example-fused-linear-pipeline")
                .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
                .stage("normalize", Order.class, NormalizedOrder.class, (order, out, ctx) ->
                    out.push(new NormalizedOrder(order.id(), order.text().trim().toUpperCase(), order.valid())),
                    StageSpec.singleThreaded())
                .stage("risk", NormalizedOrder.class, RiskCheckedOrder.class, (order, out, ctx) ->
                    out.push(new RiskCheckedOrder(order.id(), order.text(), order.valid() && !order.text().isBlank())),
                    StageSpec.singleThreaded())
                .stage("capture", RiskCheckedOrder.class, CapturedOrder.class, (order, out, ctx) -> {
                    if (order.accepted()) {
                        out.push(new CapturedOrder(order.id(), order.text()));
                    }
                }, StageSpec.singleThreaded())
                .sink("egress", CapturedOrder.class, order -> {
                    result.set(order);
                    captured.countDown();
                }, StageSpec.singleThreaded())
                // A linear chain of single-threaded stages is the shape fusion can collapse.
                .edge("ingress", "normalize", EdgeSpec.spscRing(RING_SIZE))
                .edge("normalize", "risk", EdgeSpec.spscRing(RING_SIZE))
                .edge("risk", "capture", EdgeSpec.spscRing(RING_SIZE))
                .edge("capture", "egress", EdgeSpec.spscRing(RING_SIZE))
                .build();
        } finally {
            restoreFusionProperty(previousFusion);
        }
    }

    private static void restoreFusionProperty(final String previousFusion) {
        if (previousFusion == null) {
            System.clearProperty("lattice.fusion.enabled");
        } else {
            System.setProperty("lattice.fusion.enabled", previousFusion);
        }
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

    public record Order(long id, String text, boolean valid) {
    }

    public record NormalizedOrder(long id, String text, boolean valid) {
    }

    public record RiskCheckedOrder(long id, String text, boolean accepted) {
    }

    public record CapturedOrder(long id, String text) {
    }
}
