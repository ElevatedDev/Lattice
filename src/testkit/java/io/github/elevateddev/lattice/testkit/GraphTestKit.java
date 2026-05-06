package io.github.elevateddev.lattice.testkit;

import io.github.elevateddev.lattice.graph.StaticGraph;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BooleanSupplier;

public final class GraphTestKit {

    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(1);

    private GraphTestKit() {
    }

    public static void await(final BooleanSupplier condition, final Duration timeout) {
        Objects.requireNonNull(condition, "condition");
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            park();
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("condition was not satisfied within " + timeout);
        }
    }

    public static void stopOrAbort(final StaticGraph graph, final Duration timeout) {
        Objects.requireNonNull(graph, "graph");
        try {
            if (!graph.stop(timeout)) {
                graph.abort();
                throw new AssertionError("graph did not stop within " + timeout);
            }
        } catch (final RuntimeException ex) {
            graph.abort();
            throw ex;
        }
    }

    private static void park() {
        try {
            Thread.sleep(DEFAULT_POLL_INTERVAL.toMillis());
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting", ex);
        }
    }
}
