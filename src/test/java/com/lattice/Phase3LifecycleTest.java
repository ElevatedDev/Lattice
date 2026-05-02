package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.GraphState;
import com.lattice.graph.GraphPlacementSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.internal.placement.PlacementBootstrap;
import com.lattice.placement.PinPolicy;
import com.lattice.stage.StageSpec;
import com.lattice.nativeaccess.NativeTopology;
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
        PlacementBootstrap.clearBootstrapDelayMillisForTests();
    }

    @Test
    void abortDuringBootstrapDoesNotResurrectRunningState() throws Exception {
        PlacementBootstrap.bootstrapDelayMillisForTests(250L);
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
        PlacementBootstrap.bootstrapDelayMillisForTests(150L);
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
        final StaticGraph graph = sourceSink(
            "strict-native",
            StageSpec.singleThreaded().pin(PinPolicy.cpu(0)),
            GraphPlacementSpec.off().strict(true)
        );

        assertThrows(GraphRuntimeException.class, graph::start);
        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.FAILED, graph.state());
    }

    private static StaticGraph sourceSink(final String name, final StageSpec sinkSpec) {
        return sourceSink(name, sinkSpec, GraphPlacementSpec.off());
    }

    private static StaticGraph sourceSink(
        final String name,
        final StageSpec sinkSpec,
        final GraphPlacementSpec placementSpec
    ) {
        return StaticGraph.builder(name)
            .placement(placementSpec)
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
