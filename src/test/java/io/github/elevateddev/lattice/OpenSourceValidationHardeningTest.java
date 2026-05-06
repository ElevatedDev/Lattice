package io.github.elevateddev.lattice;

import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.FusionSpec;
import io.github.elevateddev.lattice.graph.GraphBuildException;
import io.github.elevateddev.lattice.graph.GraphRuntimeException;
import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.graph.MetricsSpec;
import io.github.elevateddev.lattice.graph.PreallocationSpec;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.metrics.PlacementStatus;
import io.github.elevateddev.lattice.placement.PinPolicy;
import io.github.elevateddev.lattice.routing.DispatchSpec;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.PreallocatedEmitter;
import io.github.elevateddev.lattice.stage.StageSpec;
import io.github.elevateddev.lattice.nativeaccess.NativeTopology;
import jdk.jfr.EventType;
import jdk.jfr.FlightRecorder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class OpenSourceValidationHardeningTest {
    private static final MetricsSpec TEST_METRICS = MetricsSpec.off()
        .hotCounters(true)
        .fusedLogicalEdgeCounters(true);


    @Test
    void closeDrainsSourcesAndGraphCannotBeRestarted() throws Exception {
        final List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("lifecycle-close")
            .source("ingress", Integer.class)
            .sink("egress", Integer.class, consumed::add, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(16))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 8; i++) {
            ingress.emit(i);
        }

        graph.close();

        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7), List.copyOf(consumed));
        assertTrue(graph.awaitTermination(Duration.ofMillis(10)));
        assertThrows(GraphRuntimeException.class, graph::start);
        assertFalse(ingress.tryEmit(8));
        assertThrows(GraphRuntimeException.class, () -> ingress.emit(8));
    }

    @Test
    void keyedDispatchTreatsNullKeyAsStableBranchZero() throws Exception {
        final List<Integer> left = Collections.synchronizedList(new ArrayList<>());
        final List<Integer> right = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = graph("dispatch-null-key")
            .source("ingress", Integer.class)
            .dispatch("route", Integer.class, DispatchSpec.keyed(ignored -> null), StageSpec.singleThreaded())
            .sink("left", Integer.class, left::add, StageSpec.singleThreaded())
            .sink("right", Integer.class, right::add, StageSpec.singleThreaded())
            .edge("ingress", "route", EdgeSpec.mpscRing(16))
            .edge("route", "left", EdgeSpec.spscRing(16))
            .edge("route", "right", EdgeSpec.spscRing(16))
            .build();

        graph.start();
        final Emitter<Integer> ingress = graph.emitter("ingress", Integer.class);
        for (int i = 0; i < 6; i++) {
            ingress.emit(i);
        }
        ingress.close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(List.of(0, 1, 2, 3, 4, 5), List.copyOf(left));
        assertEquals(List.of(), List.copyOf(right));
        assertEquals(6, graph.metrics().stage("route").routingDecisions());
        assertEquals(6, graph.metrics().edge("route", "left").consumedCount());
        assertEquals(0, graph.metrics().edge("route", "right").consumedCount());
    }

    @Test
    void preallocatedFixedPoolIsSnapshottedAndRejectsForeignItems() {
        final ReusableMessage first = new ReusableMessage();
        final ReusableMessage replacement = new ReusableMessage();
        final ReusableMessage[] pool = new ReusableMessage[128];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new ReusableMessage();
        }
        pool[0] = first;
        final PreallocationSpec<ReusableMessage> spec = PreallocationSpec.fixedPool(pool);
        pool[0] = replacement;

        final StaticGraph graph = graph("preallocated-fixed-pool")
            .preallocatedSource("ingress", ReusableMessage.class, spec)
            .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build();
        final PreallocatedEmitter<ReusableMessage> ingress =
            graph.preallocatedEmitter("ingress", ReusableMessage.class);

        final ReusableMessage claimed = ingress.claim();

        assertSame(first, claimed);
        assertThrows(GraphRuntimeException.class, () -> ingress.emit(replacement));
        assertThrows(GraphRuntimeException.class, () -> ingress.discard(replacement));
        ingress.discard(claimed);
        ingress.close();
        assertTrue(ingress.isClosed());
        assertTrue(graph.stop(Duration.ofSeconds(1)));
    }

    @Test
    void preallocatedFactoryPoolRejectsNullItemsWithBuildException() {
        assertThrows(GraphBuildException.class, () -> graph("preallocated-null-pool")
            .preallocatedSource("ingress", ReusableMessage.class,
                PreallocationSpec.<ReusableMessage>pool(ignored -> null).poolSize(128))
            .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build());
    }

    @Test
    void fusionDoesNotElideRoutingFanoutOrSinkWorkers() throws Exception {
        final List<String> leftThreads = new CopyOnWriteArrayList<>();
        final List<String> rightThreads = new CopyOnWriteArrayList<>();
        final StaticGraph graph = graph("fusion-fanout-boundary")
                .fusion(FusionSpec.defaults())
                .source("ingress", Integer.class)
                .dispatch("route", Integer.class, DispatchSpec.roundRobin(), StageSpec.singleThreaded())
                .sink("left", Integer.class, ignored -> leftThreads.add(Thread.currentThread().getName()),
                    StageSpec.singleThreaded())
                .sink("right", Integer.class, ignored -> rightThreads.add(Thread.currentThread().getName()),
                    StageSpec.singleThreaded())
                .edge("ingress", "route", EdgeSpec.mpscRing(16))
                .edge("route", "left", EdgeSpec.spscRing(16))
                .edge("route", "right", EdgeSpec.spscRing(16))
                .build();

            graph.start();
            graph.emitter("ingress", Integer.class).emit(1);
            graph.emitter("ingress", Integer.class).emit(2);
            graph.emitter("ingress", Integer.class).close();

            assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
            assertTrue(leftThreads.stream().allMatch(name -> name.endsWith("-left")));
            assertTrue(rightThreads.stream().allMatch(name -> name.endsWith("-right")));
        assertEquals(List.of("route", "left", "right"), graph.plan().workerOrder());
    }

    @Test
    void placementFallbackRecordsUnavailableStatusWhenNativeIsAbsent() throws Exception {
        assumeFalse(NativeTopology.isLoaded(), "native topology library is available in this environment");
        final StaticGraph graph = graph("placement-fallback-diagnostics")
            .source("ingress", Integer.class, SourceMode.SINGLE_PRODUCER)
            .sink("egress", Integer.class, ignored -> { }, StageSpec.singleThreaded().pin(PinPolicy.cpu(0)))
            .edge("ingress", "egress", EdgeSpec.spscRing(8))
            .build();

        graph.start();
        graph.emitter("ingress", Integer.class).emit(1);
        graph.emitter("ingress", Integer.class).close();

        assertTrue(graph.awaitTermination(Duration.ofSeconds(5)));
        assertEquals(GraphState.STOPPED, graph.state());
        assertEquals(PlacementStatus.UNAVAILABLE, graph.metrics().stage("egress").placementStatus());
        assertTrue(graph.metrics().stage("egress").placementMessage().toLowerCase().contains("native topology library"));
        assertTrue(graph.metrics().placementReport().stream()
            .anyMatch(placement -> placement.stageName().equals("egress")
                && placement.status() == PlacementStatus.UNAVAILABLE));
    }

    @Test
    void jfrRuntimeEventCatalogIsRegistered() throws Exception {
        for (final String eventClass : List.of(
            "GraphStarted",
            "GraphStopped",
            "StageException",
            "EdgeBackpressure",
            "EdgeStall",
            "BatchProcessed",
            "WorkerBlocked",
            "WorkerParked",
            "WorkerPlacement",
            "AffinityMismatch",
            "NumaMismatch"
        )) {
            Class.forName("io.github.elevateddev.lattice.internal.jfr.JfrEvents$" + eventClass);
        }

        final Set<String> eventNames = FlightRecorder.getFlightRecorder().getEventTypes().stream()
            .map(EventType::getName)
            .collect(java.util.stream.Collectors.toSet());

        assertTrue(eventNames.contains("io.github.elevateddev.lattice.GraphStarted"));
        assertTrue(eventNames.contains("io.github.elevateddev.lattice.GraphStopped"));
        assertTrue(eventNames.contains("io.github.elevateddev.lattice.StageException"));
        assertTrue(eventNames.contains("io.github.elevateddev.lattice.EdgeBackpressure"));
        assertTrue(eventNames.contains("io.github.elevateddev.lattice.EdgeStall"));
        assertTrue(eventNames.contains("io.github.elevateddev.lattice.BatchProcessed"));
        assertTrue(eventNames.contains("io.github.elevateddev.lattice.WorkerBlocked"));
        assertTrue(eventNames.contains("io.github.elevateddev.lattice.WorkerParked"));
        assertTrue(eventNames.contains("io.github.elevateddev.lattice.WorkerPlacement"));
        assertTrue(eventNames.contains("io.github.elevateddev.lattice.AffinityMismatch"));
        assertTrue(eventNames.contains("io.github.elevateddev.lattice.NumaMismatch"));
    }

    static final class ReusableMessage {
    }

    private static StaticGraph.Builder graph(final String name) {
        return StaticGraph.builder(name).metrics(TEST_METRICS);
    }
}
