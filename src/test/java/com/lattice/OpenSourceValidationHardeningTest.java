package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphBuildException;
import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.GraphState;
import com.lattice.graph.PreallocationSpec;
import com.lattice.graph.SourceMode;
import com.lattice.graph.StaticGraph;
import com.lattice.metrics.PlacementStatus;
import com.lattice.placement.PinPolicy;
import com.lattice.routing.DispatchSpec;
import com.lattice.stage.Emitter;
import com.lattice.stage.PreallocatedEmitter;
import com.lattice.stage.StageSpec;
import com.lattice.nativeaccess.NativeTopology;
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

    @Test
    void closeDrainsSourcesAndGraphCannotBeRestarted() throws Exception {
        final List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        final StaticGraph graph = StaticGraph.builder("lifecycle-close")
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
        final StaticGraph graph = StaticGraph.builder("dispatch-null-key")
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

        final StaticGraph graph = StaticGraph.builder("preallocated-fixed-pool")
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
        assertThrows(GraphBuildException.class, () -> StaticGraph.builder("preallocated-null-pool")
            .preallocatedSource("ingress", ReusableMessage.class,
                PreallocationSpec.<ReusableMessage>pool(ignored -> null).poolSize(128))
            .sink("egress", ReusableMessage.class, ignored -> { }, StageSpec.singleThreaded())
            .edge("ingress", "egress", EdgeSpec.mpscRing(8))
            .build());
    }

    @Test
    void fusionDoesNotElideRoutingFanoutOrSinkWorkers() throws Exception {
        final String previous = System.getProperty("lattice.fusion.enabled");
        System.setProperty("lattice.fusion.enabled", "true");
        try {
            final List<String> leftThreads = new CopyOnWriteArrayList<>();
            final List<String> rightThreads = new CopyOnWriteArrayList<>();
            final StaticGraph graph = StaticGraph.builder("fusion-fanout-boundary")
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
        } finally {
            restoreFusionProperty(previous);
        }
    }

    @Test
    void placementFallbackRecordsUnavailableStatusWhenNativeIsAbsent() throws Exception {
        assumeFalse(NativeTopology.isLoaded(), "native topology library is available in this environment");
        final StaticGraph graph = StaticGraph.builder("placement-fallback-diagnostics")
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
            Class.forName("com.lattice.internal.jfr.JfrEvents$" + eventClass);
        }

        final Set<String> eventNames = FlightRecorder.getFlightRecorder().getEventTypes().stream()
            .map(EventType::getName)
            .collect(java.util.stream.Collectors.toSet());

        assertTrue(eventNames.contains("com.lattice.GraphStarted"));
        assertTrue(eventNames.contains("com.lattice.GraphStopped"));
        assertTrue(eventNames.contains("com.lattice.StageException"));
        assertTrue(eventNames.contains("com.lattice.EdgeBackpressure"));
        assertTrue(eventNames.contains("com.lattice.EdgeStall"));
        assertTrue(eventNames.contains("com.lattice.BatchProcessed"));
        assertTrue(eventNames.contains("com.lattice.WorkerBlocked"));
        assertTrue(eventNames.contains("com.lattice.WorkerParked"));
        assertTrue(eventNames.contains("com.lattice.WorkerPlacement"));
        assertTrue(eventNames.contains("com.lattice.AffinityMismatch"));
        assertTrue(eventNames.contains("com.lattice.NumaMismatch"));
    }

    private static void restoreFusionProperty(final String previous) {
        if (previous == null) {
            System.clearProperty("lattice.fusion.enabled");
        } else {
            System.setProperty("lattice.fusion.enabled", previous);
        }
    }

    static final class ReusableMessage {
    }
}
