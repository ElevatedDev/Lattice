package io.github.elevateddev.lattice.examples;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.routing.BroadcastSpec;
import io.github.elevateddev.lattice.routing.JoinGroup;
import io.github.elevateddev.lattice.routing.JoinSpec;
import io.github.elevateddev.lattice.routing.Stamped;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class RoutingJoinExample {

    private RoutingJoinExample() {
    }

    public static void main(final String[] args) throws Exception {
        final CountDownLatch committed = new CountDownLatch(1);
        final AtomicReference<CommitDecision> decision = new AtomicReference<>();

        final StaticGraph graph = StaticGraph.builder("example-routing-join")
            .stampedSource("orders", Order.class)
            .broadcast("fanout", Stamped.class, BroadcastSpec.copy(stamped -> stamped), StageSpec.singleThreaded())
            .stage("journal", Stamped.class, Stamped.class, (stamped, out, ctx) -> {
                // Replace this pass-through with a durable write, then emit only after the write succeeds.
                out.push(stamped);
            }, StageSpec.singleThreaded())
            .stage("risk", Stamped.class, Stamped.class, (stamped, out, ctx) -> {
                final Stamped<?> raw = (Stamped<?>) stamped;
                final Order order = (Order) raw.value();
                out.push(Stamped.of(raw.stamp(), new RiskDecision(order.id(), order.amountCents() < 100_000L)));
            }, StageSpec.singleThreaded())
            .join("join", CommitDecision.class, JoinSpec.allOf(RoutingJoinExample::commitDecision),
                StageSpec.singleThreaded())
            .sink("commit", CommitDecision.class, item -> {
                decision.set(item);
                committed.countDown();
            }, StageSpec.singleThreaded())
            // The same source stamp reaches the risk branch and the journal dependency branch.
            .edge("orders", "fanout", EdgeSpec.mpscRing(128))
            .edge("fanout", "journal", EdgeSpec.spscRing(128))
            .edge("fanout", "risk", EdgeSpec.spscRing(128))
            .edge("journal", "join", EdgeSpec.spscRing(128))
            .edge("risk", "join", EdgeSpec.spscRing(128))
            .edge("join", "commit", EdgeSpec.spscRing(128))
            .build();

        graph.start();
        final Emitter<Order> orders = graph.emitter("orders", Order.class);
        orders.emit(new Order(7L, 25_00L));
        orders.close();

        await(committed, graph, "commit decision");
        awaitTermination(graph);

        System.out.printf("%s completedJoinGroups=%d%n",
            decision.get(), graph.metrics().stage("join").completedJoinGroups());
    }

    private static CommitDecision commitDecision(final JoinGroup group) {
        final Stamped<?> journaled = group.value("journal", Stamped.class).orElseThrow();
        final Stamped<?> checked = group.value("risk", Stamped.class).orElseThrow();
        final Order order = (Order) journaled.value();
        final RiskDecision risk = (RiskDecision) checked.value();
        return new CommitDecision(group.longStamp(), order.id(), risk.accepted());
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

    public record Order(long id, long amountCents) {
    }

    public record RiskDecision(long orderId, boolean accepted) {
    }

    public record CommitDecision(long stamp, long orderId, boolean accepted) {
    }
}
