package com.lattice.internal.jfr;

import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.EventType;
import jdk.jfr.Label;
import jdk.jfr.Name;

public final class JfrEvents {

    private JfrEvents() {
    }

    public static boolean enabled() {
        return false;
    }

    public static void graphStarted(final String graphName) {
        if (!GraphStarted.EVENT_TYPE.isEnabled()) {
            return;
        }
        final GraphStarted event = new GraphStarted();
        event.graphName = graphName;
        event.commit();
    }

    public static void graphStopped(final String graphName, final String state) {
        if (!GraphStopped.EVENT_TYPE.isEnabled()) {
            return;
        }
        final GraphStopped event = new GraphStopped();
        event.graphName = graphName;
        event.state = state;
        event.commit();
    }

    public static void stageException(final String graphName, final String stageName, final Throwable failure) {
        if (!StageException.EVENT_TYPE.isEnabled()) {
            return;
        }
        final StageException event = new StageException();
        event.graphName = graphName;
        event.stageName = stageName;
        event.failureType = failure.getClass().getName();
        event.message = failure.getMessage();
        event.commit();
    }

    public static void edgeBackpressure(final String graphName, final String from, final String to) {
        if (!EdgeBackpressure.EVENT_TYPE.isEnabled()) {
            return;
        }
        final EdgeBackpressure event = new EdgeBackpressure();
        event.graphName = graphName;
        event.from = from;
        event.to = to;
        event.commit();
    }

    public static void edgeStall(final String graphName, final String from, final String to, final long nanos) {
        if (!EdgeStall.EVENT_TYPE.isEnabled()) {
            return;
        }
        final EdgeStall event = new EdgeStall();
        event.graphName = graphName;
        event.from = from;
        event.to = to;
        event.nanos = nanos;
        event.commit();
    }

    public static void batchProcessed(final String graphName, final String stageName, final int size, final long nanos) {
        if (!BatchProcessed.EVENT_TYPE.isEnabled()) {
            return;
        }
        final BatchProcessed event = new BatchProcessed();
        event.graphName = graphName;
        event.stageName = stageName;
        event.size = size;
        event.nanos = nanos;
        event.commit();
    }

    public static void workerBlocked(final String graphName, final String stageName) {
        if (!WorkerBlocked.EVENT_TYPE.isEnabled()) {
            return;
        }
        final WorkerBlocked event = new WorkerBlocked();
        event.graphName = graphName;
        event.stageName = stageName;
        event.commit();
    }

    public static void workerParked(final String graphName, final String stageName) {
        if (!WorkerParked.EVENT_TYPE.isEnabled()) {
            return;
        }
        final WorkerParked event = new WorkerParked();
        event.graphName = graphName;
        event.stageName = stageName;
        event.commit();
    }

    public static void workerPlacement(
        final String graphName,
        final String stageName,
        final int expectedCpu,
        final int observedCpu,
        final int expectedNumaNode,
        final int observedNumaNode,
        final String status
    ) {
        if (!WorkerPlacement.EVENT_TYPE.isEnabled()) {
            return;
        }
        final WorkerPlacement event = new WorkerPlacement();
        event.graphName = graphName;
        event.stageName = stageName;
        event.expectedCpu = expectedCpu;
        event.observedCpu = observedCpu;
        event.expectedNumaNode = expectedNumaNode;
        event.observedNumaNode = observedNumaNode;
        event.status = status;
        event.commit();
    }

    public static void affinityMismatch(
        final String graphName,
        final String stageName,
        final int expectedCpu,
        final int observedCpu
    ) {
        if (!AffinityMismatch.EVENT_TYPE.isEnabled()) {
            return;
        }
        final AffinityMismatch event = new AffinityMismatch();
        event.graphName = graphName;
        event.stageName = stageName;
        event.expectedCpu = expectedCpu;
        event.observedCpu = observedCpu;
        event.commit();
    }

    public static void numaMismatch(
        final String graphName,
        final String stageName,
        final int expectedNumaNode,
        final int observedNumaNode
    ) {
        if (!NumaMismatch.EVENT_TYPE.isEnabled()) {
            return;
        }
        final NumaMismatch event = new NumaMismatch();
        event.graphName = graphName;
        event.stageName = stageName;
        event.expectedNumaNode = expectedNumaNode;
        event.observedNumaNode = observedNumaNode;
        event.commit();
    }

    @Name("com.lattice.GraphStarted")
    @Label("Lattice Graph Started")
    @Category({"Lattice", "Runtime"})
    static final class GraphStarted extends Event {
        private static final EventType EVENT_TYPE = EventType.getEventType(GraphStarted.class);

        String graphName;
    }

    @Name("com.lattice.GraphStopped")
    @Label("Lattice Graph Stopped")
    @Category({"Lattice", "Runtime"})
    static final class GraphStopped extends Event {
        private static final EventType EVENT_TYPE = EventType.getEventType(GraphStopped.class);

        String graphName;
        String state;
    }

    @Name("com.lattice.StageException")
    @Label("Lattice Stage Exception")
    @Category({"Lattice", "Runtime"})
    static final class StageException extends Event {
        private static final EventType EVENT_TYPE = EventType.getEventType(StageException.class);

        String graphName;
        String stageName;
        String failureType;
        String message;
    }

    @Name("com.lattice.EdgeBackpressure")
    @Label("Lattice Edge Backpressure")
    @Category({"Lattice", "Runtime"})
    static final class EdgeBackpressure extends Event {
        private static final EventType EVENT_TYPE = EventType.getEventType(EdgeBackpressure.class);

        String graphName;
        String from;
        String to;
    }

    @Name("com.lattice.EdgeStall")
    @Label("Lattice Edge Stall")
    @Category({"Lattice", "Runtime"})
    static final class EdgeStall extends Event {
        private static final EventType EVENT_TYPE = EventType.getEventType(EdgeStall.class);

        String graphName;
        String from;
        String to;
        long nanos;
    }

    @Name("com.lattice.BatchProcessed")
    @Label("Lattice Batch Processed")
    @Category({"Lattice", "Runtime"})
    static final class BatchProcessed extends Event {
        private static final EventType EVENT_TYPE = EventType.getEventType(BatchProcessed.class);

        String graphName;
        String stageName;
        int size;
        long nanos;
    }

    @Name("com.lattice.WorkerBlocked")
    @Label("Lattice Worker Blocked")
    @Category({"Lattice", "Runtime"})
    static final class WorkerBlocked extends Event {
        private static final EventType EVENT_TYPE = EventType.getEventType(WorkerBlocked.class);

        String graphName;
        String stageName;
    }

    @Name("com.lattice.WorkerParked")
    @Label("Lattice Worker Parked")
    @Category({"Lattice", "Runtime"})
    static final class WorkerParked extends Event {
        private static final EventType EVENT_TYPE = EventType.getEventType(WorkerParked.class);

        String graphName;
        String stageName;
    }

    @Name("com.lattice.WorkerPlacement")
    @Label("Lattice Worker Placement")
    @Category({"Lattice", "Runtime", "Placement"})
    static final class WorkerPlacement extends Event {
        private static final EventType EVENT_TYPE = EventType.getEventType(WorkerPlacement.class);

        String graphName;
        String stageName;
        int expectedCpu;
        int observedCpu;
        int expectedNumaNode;
        int observedNumaNode;
        String status;
    }

    @Name("com.lattice.AffinityMismatch")
    @Label("Lattice Affinity Mismatch")
    @Category({"Lattice", "Runtime", "Placement"})
    static final class AffinityMismatch extends Event {
        private static final EventType EVENT_TYPE = EventType.getEventType(AffinityMismatch.class);

        String graphName;
        String stageName;
        int expectedCpu;
        int observedCpu;
    }

    @Name("com.lattice.NumaMismatch")
    @Label("Lattice NUMA Mismatch")
    @Category({"Lattice", "Runtime", "Placement"})
    static final class NumaMismatch extends Event {
        private static final EventType EVENT_TYPE = EventType.getEventType(NumaMismatch.class);

        String graphName;
        String stageName;
        int expectedNumaNode;
        int observedNumaNode;
    }
}
