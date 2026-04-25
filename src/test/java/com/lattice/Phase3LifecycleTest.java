package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.GraphState;
import com.lattice.graph.StaticGraph;
import com.lattice.placement.PinPolicy;
import com.lattice.stage.StageSpec;
import com.staticgraph.runtime.nativeaccess.NativeTopology;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class Phase3LifecycleTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty("lattice.placement.bootstrapDelayMillis");
        System.clearProperty("lattice.placement.strict");
    }

    @Test
    void abortDuringBootstrapDoesNotResurrectRunningState() throws Exception {
        System.setProperty("lattice.placement.bootstrapDelayMillis", "250");
        final StaticGraph graph = sourceSink("abort-bootstrap", StageSpec.singleThreaded());
        final AtomicReference<Throwable> startFailure = new AtomicReference<>();

        final Thread starter = new Thread(() -> {
            try {
                graph.start();
            } catch (final Throwable ex) {
                startFailure.set(ex);
            }
        });
        starter.start();

        waitForState(graph, GraphState.STARTING);
        graph.abort();
        starter.join(5_000L);

        assertTrue(startFailure.get() instanceof GraphRuntimeException);
        assertEquals(GraphState.STOPPED, graph.state());
        assertTrue(graph.awaitTermination(Duration.ofSeconds(1)));
    }

    @Test
    void stopDuringBootstrapDrainsWithoutPublishingRunning() throws Exception {
        System.setProperty("lattice.placement.bootstrapDelayMillis", "150");
        final StaticGraph graph = sourceSink("stop-bootstrap", StageSpec.singleThreaded());
        final AtomicReference<Throwable> startFailure = new AtomicReference<>();

        final Thread starter = new Thread(() -> {
            try {
                graph.start();
            } catch (final Throwable ex) {
                startFailure.set(ex);
            }
        });
        starter.start();

        waitForState(graph, GraphState.STARTING);
        assertTrue(graph.stop(Duration.ofSeconds(5)));
        starter.join(5_000L);

        assertTrue(startFailure.get() instanceof GraphRuntimeException);
        assertEquals(GraphState.STOPPED, graph.state());
    }

    @Test
    void strictPlacementFailsWhenNativeLibraryIsUnavailable() throws Exception {
        assumeFalse(NativeTopology.isLoaded(), "native topology library is available in this environment");
        System.setProperty("lattice.placement.strict", "true");
        final StaticGraph graph = sourceSink(
            "strict-native",
            StageSpec.singleThreaded().pin(PinPolicy.cpu(0))
        );

        assertThrows(GraphRuntimeException.class, graph::start);
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.FAILED, graph.state());
    }

    private static StaticGraph sourceSink(final String name, final StageSpec sinkSpec) {
        return StaticGraph.builder(name)
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, ignored -> { }, sinkSpec)
            .edge("ingress", "egress", EdgeSpec.mpscRing(32))
            .build();
    }

    private static void waitForState(final StaticGraph graph, final GraphState expected) throws InterruptedException {
        final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (graph.state() == expected) {
                return;
            }
            Thread.sleep(5L);
        }
        assertEquals(expected, graph.state());
    }
}
