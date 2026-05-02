package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.GraphBuildException;
import com.lattice.graph.GraphPlan;
import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.GraphState;
import com.lattice.internal.edge.MessageEdge;
import com.lattice.internal.graph.NodeDefinition;
import com.lattice.internal.jfr.JfrEvents;
import com.lattice.internal.placement.PlacementBootstrap;
import com.lattice.internal.placement.PlacementResult;
import com.lattice.internal.wait.WaitStrategies;
import com.lattice.internal.wait.WaitStrategy;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.metrics.WaitMetrics;
import com.lattice.metrics.WorkerState;
import com.lattice.placement.PinPolicy;
import com.lattice.routing.BroadcastSpec;
import com.lattice.routing.DispatchSpec;
import com.lattice.routing.JoinGroup;
import com.lattice.routing.JoinSpec;
import com.lattice.routing.PartitionSpec;
import com.lattice.routing.Stamped;
import com.lattice.slab.SlabHandle;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.BatchStageLogic;
import com.lattice.stage.Output;
import com.lattice.stage.StageExceptionAction;
import com.lattice.stage.StageExceptionHandler;
import com.lattice.stage.StageLogic;
import com.lattice.wait.WaitSpec;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

final class StageWorker implements Runnable {

    private static final int PROCESS_MESSAGES = 0;
    private static final int PROCESS_BATCH = 1;
    private static final int PROCESS_SINK = 2;
    private static final int PROCESS_DISPATCH = 3;
    private static final int PROCESS_BROADCAST = 4;
    private static final int PROCESS_PARTITION = 5;
    private static final int PROCESS_JOIN = 6;
    private static final int MAX_WEIGHTED_DISPATCH_SCHEDULE = 1_000_000;
    private static final int DEFAULT_SINGLE_MESSAGE_BATCH_SIZE = Math.max(
        1,
        Integer.getInteger("lattice.runtime.singleMessageBatchSize", 64)
    );

    private static final boolean FUSED_LOGICAL_EDGE_METRICS = Boolean.parseBoolean(
        System.getProperty("lattice.runtime.fusedLogicalEdgeMetrics", "true")
    );

    private static final boolean LOGICAL_METRICS_ON =
        StageMetrics.hotCountersEnabled() && FUSED_LOGICAL_EDGE_METRICS;

    private static final boolean LATTICE_VALIDATE_FUSED = Boolean.getBoolean("lattice.fusion.validateTypes");

    private static final MessageEdge[] NO_EDGES = new MessageEdge[0];

    private final String graphName;
    private final String stageName;
    private final MessageEdge[] inputs;
    private final EdgeSpec[] inputSpecs;
    private final String[] inputSources;
    private final MessageEdge[] outputEdges;
    private final EdgeSender[] outputSenders;
    private final EdgeMetrics[] outputMetrics;
    private final MessageEdge[] ownedEdges;
    private final Output<Object> output;
    private final MessageEdge.ItemProcessor sinkProcessor;
    private final FusedSink fusedSink;
    private final FusedStage fusedStage;
    private final FusedRouter fusedRouter;
    private final StageMetrics metrics;
    private final RuntimeCoordinator coordinator;
    private final RuntimeStageContext context;
    private final WaitStrategy waitStrategy;
    private final WaitMetrics waitMetrics;
    private final StageExceptionHandler exceptionHandler;
    private final PinPolicy pinPolicy;
    private final StageLogic<Object, Object> logic;
    private final BatchStageLogic<Object, Object> batchLogic;
    private final Consumer<Object> sink;
    private final DispatchSpec<Object> dispatchSpec;
    private final BroadcastSpec<Object> broadcastSpec;
    private final PartitionSpec<Object, ?> partitionSpec;
    private final JoinSpec<?> joinSpec;
    private final boolean broadcastSlabHandles;
    private final boolean isolateBroadcastBranches;
    private final Function<Object, Object> broadcastCopier;
    private final Function<Object, ?> partitionKeyExtractor;
    private final boolean joinLongStamp;
    private final int joinCapacity;
    private final long joinTimeoutNanos;
    private final boolean joinTimeoutEnabled;
    private final JoinSpec.JoinKind joinKind;
    private final JoinSpec.MissingBranchPolicy joinMissingBranchPolicy;
    private final JoinSpec.DuplicatePolicy joinDuplicatePolicy;
    private final Function<JoinGroup, ?> joinCombiner;
    private final int processingMode;
    private final int batchLimit;
    private final long lingerNanos;
    private final int parkIdleThreshold;
    private final boolean jfrEnabled;
    private final boolean timeBatches;
    private final boolean hotMetricsEnabled;
    private final boolean edgeHotMetricsEnabled;
    private final boolean messageMayCarryOwnedHandle;
    private final boolean directSinkDrain;
    private final int[] weightedSchedule;
    private final long[] partitionLaneCounts;
    private final long partitionHotKeyThreshold;
    private final int outputMask;
    private ArrayBatch batch;
    private Object[] batchItems;
    private int[] batchSources;
    private WorkerState currentWorkerState = WorkerState.NEW;
    private volatile boolean active;

    private boolean inlineFused;
    private Thread thread;
    private long routeSequence;
    private JoinStateTable joinStates;
    private JoinValuesMap joinValuesMap;
    private JoinGroup reusableJoinGroup;

    @SuppressWarnings("unchecked")
    StageWorker(
        final NodeDefinition node,
        final MessageEdge[] inputs,
        final EdgeSpec[] inputSpecs,
        final String[] inputSources,
        final MessageEdge[] outputEdges,
        final EdgeSender[] outputSenders,
        final MessageEdge[] ownedEdges,
        final Output<Object> output,
        final FusedSink fusedSink,
        final FusedStage fusedStage,
        final FusedRouter fusedRouter,
        final PinPolicy effectivePinPolicy,
        final StageMetrics metrics,
        final RuntimeCoordinator coordinator,
        final StageExceptionHandler exceptionHandler
    ) {
        if ((fusedSink != null ? 1 : 0) + (fusedStage != null ? 1 : 0) + (fusedRouter != null ? 1 : 0) > 1) {
            throw new IllegalArgumentException("a worker cannot fuse multiple output targets");
        }
        this.graphName = coordinator.graphName();
        this.stageName = node.name();
        this.inputs = inputs.clone();
        this.inputSpecs = inputSpecs.clone();
        this.inputSources = inputSources.clone();
        this.outputEdges = outputEdges.clone();
        this.outputSenders = outputSenders.clone();
        this.outputMetrics = outputMetrics(this.outputSenders);
        this.ownedEdges = ownedEdges.clone();
        final LinearStageSinkLoop linearStageSinkLoop = linearStageSinkLoop(fusedSink, fusedStage);
        this.output = linearStageSinkLoop != null ? linearStageSinkLoop.entryOutput()
            : fusedStage != null ? new DirectStageOutput(fusedStage)
            : fusedRouter != null ? new DirectRouterOutput(fusedRouter)
            : fusedSink == null ? output : new DirectSinkOutput(fusedSink);
        this.sinkProcessor = this::processSinkItem;
        this.fusedSink = fusedSink;
        this.fusedStage = fusedStage;
        this.fusedRouter = fusedRouter;
        this.metrics = metrics;
        this.coordinator = coordinator;
        this.context = new RuntimeStageContext(coordinator, stageName, metrics);
        final WaitSpec waitSpec = node.spec().waitSpec();
        this.waitStrategy = WaitStrategies.from(waitSpec);
        this.parkIdleThreshold = parkIdleThreshold(waitSpec);
        this.messageMayCarryOwnedHandle = mayCarryOwnedHandle(node.inputType());
        this.ownerInputType = node.inputType();
        this.exceptionHandler = exceptionHandler;
        this.pinPolicy = ObjectsRequireNonNull(effectivePinPolicy, "effectivePinPolicy");
        final BatchPolicy activeBatchPolicy = activeBatchPolicy(node, inputSpecs);
        this.batchLimit = batchLimit(activeBatchPolicy);
        this.lingerNanos = activeBatchPolicy.kind() == BatchPolicy.BatchKind.LINGER
            ? activeBatchPolicy.linger().toNanos()
            : 0L;

        this.logic = (StageLogic<Object, Object>) node.logic();
        this.batchLogic = (BatchStageLogic<Object, Object>) node.batchLogic();
        this.sink = (Consumer<Object>) node.sink();
        this.dispatchSpec = (DispatchSpec<Object>) node.dispatchSpec();
        this.broadcastSpec = (BroadcastSpec<Object>) node.broadcastSpec();
        this.partitionSpec = (PartitionSpec<Object, ?>) node.partitionSpec();
        this.joinSpec = node.joinSpec();
        this.broadcastSlabHandles = broadcastSpec != null
            && broadcastSpec.kind() == BroadcastSpec.BroadcastKind.SLAB_HANDLE;
        this.isolateBroadcastBranches = broadcastSpec != null && broadcastSpec.isolateSlowBranches();
        this.broadcastCopier = broadcastSpec == null ? null : (Function<Object, Object>) broadcastSpec.copier();
        this.partitionKeyExtractor = partitionSpec == null ? null : (Function<Object, ?>) partitionSpec.keyExtractor();
        this.joinLongStamp = joinSpec != null && joinSpec.longStamp();
        this.joinCapacity = joinSpec == null ? 0 : joinSpec.capacity();
        this.joinTimeoutNanos = joinSpec == null ? 0L : joinSpec.timeout().toNanos();
        this.joinTimeoutEnabled = joinTimeoutNanos > 0L;
        this.joinKind = joinSpec == null ? null : joinSpec.kind();
        this.joinMissingBranchPolicy = joinSpec == null ? null : joinSpec.missingBranchPolicy();
        this.joinDuplicatePolicy = joinSpec == null ? null : joinSpec.duplicatePolicy();
        this.joinCombiner = joinSpec == null ? null : (Function<JoinGroup, ?>) joinSpec.combiner();
        this.processingMode = processingMode(node);
        this.directSinkDrain = processingMode == PROCESS_SINK && inputs.length == 1
            && directSinkDrainSafe(inputSpecs[0]);
        this.weightedSchedule = dispatchSpec == null ? new int[0] : weightedSchedule(dispatchSpec, outputSenders.length);
        this.partitionLaneCounts = partitionSpec == null ? new long[0] : new long[outputSenders.length];
        this.partitionHotKeyThreshold = partitionSpec == null ? 0L : partitionSpec.hotKeyThreshold();
        this.jfrEnabled = JfrEvents.enabled();
        this.timeBatches = StageMetrics.histogramsEnabled() || jfrEnabled;
        this.hotMetricsEnabled = StageMetrics.hotCountersEnabled();
        this.edgeHotMetricsEnabled = EdgeMetrics.hotCountersEnabled();
        this.waitMetrics = hotMetricsEnabled ? metrics : null;
        this.outputMask = powerOfTwoMask(outputSenders.length);
    }

    private LinearStageSinkLoop linearStageSinkLoop(final FusedSink sink, final FusedStage stage) {
        if (sink != null) {
            return new LinearStageSinkLoop(new FusedStage[0], sink, null);
        }
        if (stage == null) {
            return null;
        }

        int count = 0;
        FusedStage current = stage;
        FusedSink terminalSink = null;
        Output<Object> tailOutput = null;
        while (current != null) {
            count++;
            if (current.nextStage() == null) {
                terminalSink = current.terminalSink();
                if (terminalSink == null) {
                    tailOutput = current.output;
                }
                break;
            }
            current = current.nextStage();
        }

        final FusedStage[] stages = new FusedStage[count];
        current = stage;
        for (int i = 0; i < count; i++) {
            stages[i] = current;
            current = current.nextStage();
        }
        return new LinearStageSinkLoop(stages, terminalSink, tailOutput);
    }

    void start() {
        workerState(WorkerState.STARTING);
        thread = Thread.ofPlatform()
            .name("lattice-" + graphName + "-" + stageName)
            .unstarted(this);

        thread.start();
    }

    Output<Object> inlineEntryOutput() {
        if ((fusedStage == null && fusedSink == null) || logic == null) {
            return null;
        }
        if (processingMode != PROCESS_MESSAGES) {
            return null;
        }
        return new InlineOwnerStageOutput();
    }

    private final class InlineOwnerStageOutput implements Output<Object> {
        private final Class<?> inputType = StageWorker.this.inputType();
        private final String inputTypeName = inputType == null ? "Object" : inputType.getName();
        private final boolean acceptsAnyType = inputType == null || inputType == Object.class;

        @Override
        public void push(final Object item) {
            run(item);
        }

        @Override
        public boolean push(final Object item, final Duration timeout) {
            run(item);
            return true;
        }

        @Override
        public boolean tryPush(final Object item) {
            run(item);
            return true;
        }

        private void run(final Object item) {
            if (item == null) {
                throw new NullPointerException(stageName + " cannot consume null");
            }
            if (!acceptsAnyType && item.getClass() != inputType && !inputType.isInstance(item)) {
                throw new ClassCastException(stageName + " received " + item.getClass().getName()
                    + ", expected " + inputTypeName);
            }
            if (StageMetrics.hotCountersEnabled()) {
                metrics.recordConsume();
            }
            try {
                if (messageMayCarryOwnedHandle) {
                    try (HandleOwnership.Scope ignored = HandleOwnership.scope(item)) {
                        logic.onMessage(item, output, context);
                    } finally {
                        releaseIfHandle(item);
                    }
                } else {
                    logic.onMessage(item, output, context);
                }
            } catch (final FusedStageException fused) {
                throw fused;
            } catch (final RuntimeException re) {
                throw new FusedStageException(stageName, metrics, context, re);
            } catch (final Exception ex) {
                throw new FusedStageException(stageName, metrics, context, ex);
            } catch (final Throwable ex) {
                throw new FusedStageException(stageName, metrics, context, ex);
            }
            if (StageMetrics.hotCountersEnabled()) {
                metrics.recordBatch(1, 0L);
            }
        }
    }

    private Class<?> inputType() {
        return ownerInputType;
    }

    private final Class<?> ownerInputType;


    void markInlineFused() {
        this.inlineFused = true;
    }

    void startInlineLifecycle() {
        metrics.markStarted();
        workerState(WorkerState.RUNNING);
        startFusedSink();
        startFusedStage();
        startFusedRouter();
    }

    void stopInlineLifecycle() {
        closeOutputs();
        active = false;
        stopFusedStage();
        stopFusedSink();
        stopFusedRouter();
        workerState(coordinator.state() == GraphState.FAILED ? WorkerState.FAILED : WorkerState.STOPPED);
        metrics.markStopped();
        coordinator.workerStopped();
    }

    void interrupt() {
        final Thread current = thread;
        if (current != null) {
            current.interrupt();
        }
    }

    String stageName() {
        return stageName;
    }

    boolean join(final Duration timeout) throws InterruptedException {
        final Thread current = thread;
        if (current == null) {
            return true;
        }
        current.join(timeout);
        return !current.isAlive();
    }

    void join() throws InterruptedException {
        final Thread current = thread;
        if (current != null) {
            current.join();
        }
    }

    boolean active() {
        return active;
    }

    @Override
    public void run() {
        WorkerState terminalState = WorkerState.STOPPED;
        boolean bootstrapped = false;
        try {
            final PlacementResult placement = PlacementBootstrap.bootstrap(stageName, pinPolicy, ownedEdges);
            this.batch = new ArrayBatch(batchLimit);
            this.batchItems = batch.items();
            this.batchSources = processingMode == PROCESS_JOIN ? new int[batchLimit] : null;
            if (processingMode == PROCESS_JOIN) {
                this.joinStates = joinLongStamp
                    ? new LongJoinStateTable(joinCapacity, inputs.length)
                    : new ObjectJoinStateTable(joinCapacity, inputs.length);
                this.joinValuesMap = new JoinValuesMap(inputSources);
                this.reusableJoinGroup = JoinGroup.reusableRuntimeGroup(joinValuesMap);
            }
            recordPlacement(placement);
            bootstrapped = true;
            coordinator.workerBootstrapped();
            coordinator.awaitRunRelease();
            metrics.markStarted();
            workerState(WorkerState.RUNNING);
            startFusedSink();
            startFusedStage();
            startFusedRouter();

            if (inlineFused) {
                while (!coordinator.isAbortRequested()
                    && (!allInputsClosedAndEmpty() || coordinator.hasInFlightInlineWork())) {
                    workerState(WorkerState.PARKED);
                    java.util.concurrent.locks.LockSupport.parkNanos(1_000_000_000L);
                }
                return;
            }

            int idle = 0;
            while (!coordinator.isAbortRequested()) {
                boolean activeNow = false;
                int received = 0;
                try {
                    active = true;
                    activeNow = true;
                    final long started = directSinkDrain && timeBatches ? System.nanoTime() : 0L;
                    received = directSinkDrain ? receiveAndProcessSink() : receive();
                    if (received == 0) {
                        active = false;
                        activeNow = false;
                        expireJoinGroups(false);
                        if (allInputsClosedAndEmpty()) {
                            break;
                        }
                        workerState(WorkerState.IDLE);
                        if (jfrEnabled) {
                            JfrEvents.workerBlocked(graphName, stageName);
                        }
                        final boolean willPark = parkIdleThreshold >= 0 && idle >= parkIdleThreshold;
                        idle = waitStrategy.idle(idle, waitMetrics);
                        if (willPark) {
                            workerState(WorkerState.PARKED);
                            if (jfrEnabled) {
                                JfrEvents.workerParked(graphName, stageName);
                            }
                        }
                        continue;
                    }

                    idle = 0;
                    workerState(WorkerState.RUNNING);
                    if (hotMetricsEnabled) {
                        metrics.recordConsume(received);
                    }
                    final long processStarted = directSinkDrain ? started : timeBatches ? System.nanoTime() : 0L;
                    try {
                        if (!directSinkDrain) {
                            processReceived(received);
                        }
                        final long serviceNanos = timeBatches ? System.nanoTime() - processStarted : 0L;
                        if (hotMetricsEnabled) {
                            metrics.recordBatch(received, serviceNanos);
                        }
                        if (jfrEnabled) {
                            JfrEvents.batchProcessed(graphName, stageName, received, serviceNanos);
                        }
                    } catch (final Throwable ex) {
                        releaseBatchItems(received);
                        batch.clear();
                        throw ex;
                    }
                } finally {
                    if (activeNow) {
                        active = false;
                    }
                }
            }
            if (coordinator.isAbortRequested()) {
                releaseJoinState();
            } else {
                expireJoinGroups(true);
            }
        } catch (final Throwable ex) {
            releaseJoinState();
            if (coordinator.isAbortRequested()
                && (coordinator.state() != GraphState.FAILED
                    || ex instanceof GraphRuntimeException
                    || ex instanceof InterruptedException)) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                terminalState = coordinator.state() == GraphState.FAILED ? WorkerState.FAILED : WorkerState.STOPPED;
            } else {
                terminalState = handleException(ex);
            }
        } finally {
            if (!bootstrapped) {
                coordinator.workerBootstrapped();
            }
            closeOutputs();
            active = false;
            stopFusedStage();
            stopFusedSink();
            stopFusedRouter();
            workerState(coordinator.state() == GraphState.FAILED ? WorkerState.FAILED : terminalState);
            metrics.markStopped();
            coordinator.workerStopped();
        }
    }

    private void recordPlacement(final PlacementResult placement) {
        metrics.recordPlacement(
            placement.status(),
            placement.message(),
            placement.expectedCpu(),
            placement.observedCpu(),
            placement.expectedNumaNode(),
            placement.observedNumaNode(),
            placement.allocationOwner(),
            placement.affinityViolation(),
            placement.numaViolation()
        );
        if (jfrEnabled) {
            JfrEvents.workerPlacement(
                graphName,
                stageName,
                placement.expectedCpu(),
                placement.observedCpu(),
                placement.expectedNumaNode(),
                placement.observedNumaNode(),
                placement.status().name()
            );
            if (placement.affinityViolation()) {
                JfrEvents.affinityMismatch(graphName, stageName, placement.expectedCpu(), placement.observedCpu());
            }
            if (placement.numaViolation()) {
                JfrEvents.numaMismatch(graphName, stageName, placement.expectedNumaNode(), placement.observedNumaNode());
            }
        }
    }

    private void startFusedSink() {
        startFusedSink(fusedSink);
    }

    private void startFusedSink(final FusedSink sink) {
        if (sink == null) {
            return;
        }
        sink.metrics().markStarted();
        sink.metrics().workerState(WorkerState.RUNNING);
    }

    private void stopFusedSink() {
        stopFusedSink(fusedSink);
    }

    private void stopFusedSink(final FusedSink sink) {
        if (sink == null) {
            return;
        }
        final WorkerState state = sink.poisoned()
            ? WorkerState.POISONED
            : coordinator.state() == GraphState.FAILED ? WorkerState.FAILED : WorkerState.STOPPED;
        sink.metrics().workerState(state);
        sink.metrics().markStopped();
    }

    private void startFusedStage() {
        startFusedStage(fusedStage);
    }

    private void startFusedStage(final FusedStage stage) {
        if (stage == null) {
            return;
        }
        stage.metrics().markStarted();
        stage.metrics().workerState(WorkerState.RUNNING);
        startFusedStage(stage.nextStage());
        startFusedSink(stage.terminalSink());
    }

    private void stopFusedStage() {
        stopFusedStage(fusedStage);
    }

    private void stopFusedStage(final FusedStage stage) {
        if (stage == null) {
            return;
        }
        stopFusedStage(stage.nextStage());
        stopFusedSink(stage.terminalSink());
        final WorkerState state = stage.poisoned()
            ? WorkerState.POISONED
            : coordinator.state() == GraphState.FAILED ? WorkerState.FAILED : WorkerState.STOPPED;
        stage.metrics().workerState(state);
        stage.metrics().markStopped();
    }

    private void startFusedRouter() {
        if (fusedRouter == null) {
            return;
        }
        fusedRouter.metrics().markStarted();
        fusedRouter.metrics().workerState(WorkerState.RUNNING);
    }

    private void stopFusedRouter() {
        if (fusedRouter == null) {
            return;
        }
        final WorkerState state = fusedRouter.poisoned()
            ? WorkerState.POISONED
            : coordinator.state() == GraphState.FAILED ? WorkerState.FAILED : WorkerState.STOPPED;
        fusedRouter.metrics().workerState(state);
        fusedRouter.metrics().markStopped();
    }

    private int receive() {
        if (processingMode == PROCESS_JOIN) {
            return receiveJoinInputs();
        }
        final MessageEdge input = inputs[0];
        int received = input.drainTo(batchItems, batchLimit);
        if (received == 0 || received == batchLimit || lingerNanos == 0L) {
            batch.size(received);
            return received;
        }

        final long deadline = System.nanoTime() + lingerNanos;
        while (received < batchLimit && System.nanoTime() < deadline) {
            final int more = input.drainTo(batchItems, received, batchLimit - received);
            if (more == 0) {
                Thread.onSpinWait();
            } else {
                received += more;
            }
        }
        batch.size(received);
        return received;
    }

    private int receiveAndProcessSink() throws Exception {
        final MessageEdge input = inputs[0];
        int received = input.drainToProcessor(sinkProcessor, batchLimit);
        if (received == 0 || received == batchLimit || lingerNanos == 0L) {
            return received;
        }

        final long deadline = System.nanoTime() + lingerNanos;
        while (received < batchLimit && System.nanoTime() < deadline) {
            final int more = input.drainToProcessor(sinkProcessor, batchLimit - received);
            if (more == 0) {
                Thread.onSpinWait();
            } else {
                received += more;
            }
        }
        return received;
    }

    private static boolean directSinkDrainSafe(final EdgeSpec spec) {
        return switch (spec.overflowPolicy().kind()) {
            case DROP_OLDEST, COALESCE -> false;
            default -> true;
        };
    }

    private void processSinkItem(final Object item) {
        try {
            sink.accept(item);
        } finally {
            if (messageMayCarryOwnedHandle) {
                releaseIfHandle(item);
            }
        }
    }

    private int receiveJoinInputs() {
        if (inputs.length == 2) {
            return receiveTwoJoinInputs();
        }
        return receiveManyJoinInputs();
    }

    private int receiveTwoJoinInputs() {
        int received = 0;
        final int firstQuota = Math.max(1, batchLimit >>> 1);
        received += drainJoinSource(0, received, firstQuota);
        received += drainJoinSource(1, received, batchLimit - received);
        if (received < batchLimit) {
            received += drainJoinSource(0, received, batchLimit - received);
        }
        batch.size(received);
        return received;
    }

    private int receiveManyJoinInputs() {
        int received = 0;
        final int inputCount = inputs.length;
        if (inputCount == 0) {
            batch.size(0);
            return 0;
        }
        final int quota = Math.max(1, batchLimit / inputCount);
        for (int source = 0; source < inputCount && received < batchLimit; source++) {
            received += drainJoinSource(source, received, Math.min(quota, batchLimit - received));
        }
        for (int source = 0; source < inputCount && received < batchLimit; source++) {
            received += drainJoinSource(source, received, batchLimit - received);
        }
        batch.size(received);
        return received;
    }

    private int drainJoinSource(final int sourceIndex, final int offset, final int limit) {
        if (limit <= 0) {
            return 0;
        }
        final int drained = inputs[sourceIndex].drainTo(batchItems, offset, limit);
        if (drained > 0) {
            final int end = offset + drained;
            for (int i = offset; i < end; i++) {
                batchSources[i] = sourceIndex;
            }
        }
        return drained;
    }

    private void processReceived(final int received) throws Exception {
        switch (processingMode) {
            case PROCESS_SINK -> processSink(received);
            case PROCESS_BATCH -> processBatch(received);
            case PROCESS_MESSAGES -> processMessages(received);
            case PROCESS_DISPATCH -> processDispatch(received);
            case PROCESS_BROADCAST -> processBroadcast(received);
            case PROCESS_PARTITION -> processPartition(received);
            case PROCESS_JOIN -> processJoin(received);
            default -> throw new IllegalStateException("unknown processing mode: " + processingMode);
        }
    }

    private void processSink(final int received) {
        final Object[] localBatchItems = batchItems;
        for (int i = 0; i < received; i++) {
            final Object item = localBatchItems[i];
            try {
                sink.accept(item);
            } finally {
                if (messageMayCarryOwnedHandle) {
                    releaseIfHandle(item);
                }
                localBatchItems[i] = null;
            }
        }
        batch.size(0);
    }

    private void processBatch(final int received) throws Exception {
        final Object[] localBatchItems = batchItems;
        try (HandleOwnership.Scope ignored = HandleOwnership.scope(localBatchItems, received)) {
            batchLogic.onBatch(batch, output, context);
        } finally {
            for (int i = 0; i < received; i++) {
                if (messageMayCarryOwnedHandle) {
                    releaseIfHandle(localBatchItems[i]);
                }
                localBatchItems[i] = null;
            }
            batch.size(0);
        }
    }

    private void processMessages(final int received) throws Exception {
        final Object[] localBatchItems = batchItems;
        if (messageMayCarryOwnedHandle) {
            processMessagesHandleAware(localBatchItems, received);
        } else {
            processMessagesPlain(localBatchItems, received);
        }
        batch.size(0);
    }

    private void processMessagesPlain(final Object[] localBatchItems, final int received) throws Exception {
        final StageLogic<Object, Object> localLogic = logic;
        final Output<Object> localOutput = output;
        final RuntimeStageContext localContext = context;
        int i = 0;
        try {
            for (; i < received; i++) {
                final Object item = localBatchItems[i];
                localBatchItems[i] = null;
                localLogic.onMessage(item, localOutput, localContext);
            }
        } catch (final Throwable ex) {
            for (int j = i; j < received; j++) {
                localBatchItems[j] = null;
            }
            throw ex;
        }
    }

    private void processMessagesHandleAware(final Object[] localBatchItems, final int received) throws Exception {
        for (int i = 0; i < received; i++) {
            final Object item = localBatchItems[i];
            try {
                try (HandleOwnership.Scope ignored = HandleOwnership.scope(item)) {
                    logic.onMessage(item, output, context);
                }
            } finally {
                releaseIfHandle(item);
                localBatchItems[i] = null;
            }
        }
    }

    private void processDispatch(final int received) {
        final Object[] localBatchItems = batchItems;
        for (int i = 0; i < received; i++) {
            final Object item = localBatchItems[i];
            boolean transferred = false;
            try {
                final int branch = dispatchBranch(item);
                if (hotMetricsEnabled) {
                    metrics.recordRoutingDecision();
                }
                if (edgeHotMetricsEnabled) {
                    outputMetrics[branch].recordLaneSelection();
                }
                outputSenders[branch].emit(item);
                transferred = true;
            } finally {
                if (!transferred && messageMayCarryOwnedHandle) {
                    releaseIfHandle(item);
                }
                localBatchItems[i] = null;
            }
        }
        batch.size(0);
    }

    private void processBroadcast(final int received) {
        final Object[] localBatchItems = batchItems;
        for (int i = 0; i < received; i++) {
            final Object item = localBatchItems[i];
            try {
                if (broadcastSlabHandles) {
                    broadcastHandle(item);
                } else {
                    broadcastCopy(item);
                }
                if (hotMetricsEnabled) {
                    metrics.recordRoutingDecision();
                }
            } finally {
                localBatchItems[i] = null;
            }
        }
        batch.size(0);
    }

    private void broadcastCopy(final Object item) {
        final EdgeSender[] senders = outputSenders;
        final EdgeMetrics[] metricsByBranch = outputMetrics;
        final Function<Object, Object> copier = broadcastCopier;
        final boolean isolate = isolateBroadcastBranches;
        for (int branch = 0; branch < senders.length; branch++) {
            final Object branchItem = copier == null ? item : copier.apply(item);
            if (isolate) {
                if (!senders[branch].tryEmit(branchItem)) {
                    if (hotMetricsEnabled) {
                        metrics.recordBranchIsolationAction();
                    }
                    if (edgeHotMetricsEnabled) {
                        metricsByBranch[branch].recordBranchIsolationAction();
                    }
                    releaseIfHandle(branchItem);
                }
            } else {
                senders[branch].emit(branchItem);
            }
        }
    }

    private void broadcastHandle(final Object item) {
        if (!(item instanceof SlabHandle<?> handle)) {
            throw new IllegalArgumentException("slab-handle broadcast requires SlabHandle payloads");
        }
        try {
            final EdgeSender[] senders = outputSenders;
            final EdgeMetrics[] metricsByBranch = outputMetrics;
            final boolean isolate = isolateBroadcastBranches;
            for (int branch = 0; branch < senders.length; branch++) {
                final SlabHandle<?> branchHandle = handle.retain();
                if (hotMetricsEnabled) {
                    metrics.recordRetainedHandle();
                }
                if (isolate) {
                    if (!senders[branch].tryEmit(branchHandle)) {
                        if (hotMetricsEnabled) {
                            metrics.recordBranchIsolationAction();
                        }
                        if (edgeHotMetricsEnabled) {
                            metricsByBranch[branch].recordBranchIsolationAction();
                        }
                        branchHandle.release();
                        if (hotMetricsEnabled) {
                            metrics.recordReleasedHandle();
                        }
                    }
                } else {
                    boolean enqueued = false;
                    try {
                        senders[branch].emit(branchHandle);
                        enqueued = true;
                    } finally {
                        if (!enqueued) {
                            branchHandle.release();
                            if (hotMetricsEnabled) {
                                metrics.recordReleasedHandle();
                            }
                        }
                    }
                }
            }
        } finally {
            handle.release();
            if (hotMetricsEnabled) {
                metrics.recordReleasedHandle();
            }
        }
    }

    private void processPartition(final int received) {
        final Object[] localBatchItems = batchItems;
        for (int i = 0; i < received; i++) {
            final Object item = localBatchItems[i];
            boolean transferred = false;
            try {
                final Object key = partitionKeyExtractor.apply(item);
                final int lane = branchForKey(key);
                if (hotMetricsEnabled) {
                    metrics.recordRoutingDecision();
                }
                if (edgeHotMetricsEnabled) {
                    outputMetrics[lane].recordLaneSelection();
                }
                if (partitionHotKeyThreshold > 0L) {
                    final long laneCount = ++partitionLaneCounts[lane];
                    if (laneCount == partitionHotKeyThreshold && edgeHotMetricsEnabled) {
                        outputMetrics[lane].recordHotKeySignal();
                    }
                }
                outputSenders[lane].emit(item);
                transferred = true;
            } finally {
                if (!transferred && messageMayCarryOwnedHandle) {
                    releaseIfHandle(item);
                }
                localBatchItems[i] = null;
            }
        }
        batch.size(0);
    }

    private void processJoin(final int received) {
        final Object[] localBatchItems = batchItems;
        for (int i = 0; i < received; i++) {
            try {
                processJoinItem(batchSources[i], localBatchItems[i]);
            } finally {
                localBatchItems[i] = null;
            }
        }
        batch.size(0);
        if (joinTimeoutEnabled) {
            expireJoinGroups(false);
        }
    }

    private int dispatchBranch(final Object item) {
        return switch (dispatchSpec.kind()) {
            case ROUND_ROBIN -> {
                final long sequence = routeSequence++;
                yield outputMask >= 0 ? (int) sequence & outputMask : (int) (sequence % outputSenders.length);
            }
            case KEYED -> {
                final Object key = dispatchSpec.keyExtractor().apply(item);
                yield branchForKey(key);
            }
            case WEIGHTED -> weightedSchedule[(int) (routeSequence++ % weightedSchedule.length)];
        };
    }

    private int branchForKey(final Object key) {
        final int hash = spread(key == null ? 0 : key.hashCode());
        if (outputMask >= 0) {
            return hash & outputMask;
        }
        return floorMod(hash, outputSenders.length);
    }

    private void processJoinItem(final int sourceIndex, final Object item) {
        final long stampLong = joinLongStamp ? joinSpec.extractLongStamp(item) : 0L;
        final Object stampObject = joinLongStamp ? null
            : ObjectsRequireNonNull(joinSpec.extractStamp(item), "join stamp");
        final String source = inputSources[sourceIndex];
        JoinState state = joinLongStamp ? joinStates.get(stampLong) : joinStates.get(stampObject);
        if (state == null) {
            ensureJoinCapacity();
            final long createdNanos = joinTimeoutEnabled ? System.nanoTime() : 0L;
            state = joinLongStamp ? joinStates.create(stampLong, createdNanos)
                : joinStates.create(stampObject, createdNanos);
            if (hotMetricsEnabled) {
                metrics.recordOpenJoinGroup();
            }
        }

        if (joinKind == JoinSpec.JoinKind.ANY_OF) {
            if (state.hasSeen(sourceIndex)) {
                handleDuplicateJoinStamp(state, source, item);
                return;
            }
            state.markSeen(sourceIndex);
            if (!state.emitted) {
                state.setValue(sourceIndex, item);
                if (hotMetricsEnabled && item instanceof SlabHandle<?>) {
                    metrics.recordRetainedHandle();
                }
                emitJoin(state, source, false, false);
                state.emitted = true;
            } else {
                handleDuplicateJoinStamp(state, source, item);
            }
            if (state.seenCount == inputs.length) {
                joinStates.remove(state);
                releaseAndRecycleJoinState(state);
            }
            return;
        }

        if (state.hasValue(sourceIndex)) {
            handleDuplicateJoinStamp(state, source, item);
            return;
        }

        state.setValue(sourceIndex, item);
        if (hotMetricsEnabled && item instanceof SlabHandle<?>) {
            metrics.recordRetainedHandle();
        }
        if (state.receivedCount == inputs.length) {
            emitJoin(state, source, false, true);
            joinStates.remove(state);
            joinStates.recycle(state);
        }
    }

    private void handleDuplicateJoinStamp(final JoinState state, final String source, final Object item) {
        switch (joinDuplicatePolicy) {
            case IGNORE -> releaseIfHandle(item);
            case COUNT -> {
                if (hotMetricsEnabled) {
                    metrics.recordDuplicateJoinStamp();
                }
                releaseIfHandle(item);
            }
            case FAIL -> {
                if (hotMetricsEnabled) {
                    metrics.recordDuplicateJoinStamp();
                }
                releaseIfHandle(item);
                throw new IllegalStateException("duplicate join stamp " + state.stamp() + " from " + source);
            }
            default -> throw new IllegalStateException("unknown duplicate policy: " + joinDuplicatePolicy);
        }
    }

    private void ensureJoinCapacity() {
        if (joinStates.size() < joinCapacity) {
            return;
        }
        final JoinState eldest = joinStates.removeEldest();
        if (eldest == null) {
            return;
        }
        if (hotMetricsEnabled) {
            metrics.recordTimedOutJoinGroup();
        }
        releaseAndRecycleJoinState(eldest);
    }

    private void expireJoinGroups(final boolean closing) {
        if (processingMode != PROCESS_JOIN || joinStates == null || joinStates.isEmpty()) {
            return;
        }
        if (!closing && !joinTimeoutEnabled) {
            return;
        }
        final long now = closing ? 0L : System.nanoTime();
        while (true) {
            final JoinState state = joinStates.eldest();
            if (state == null) {
                return;
            }
            final boolean timedOut = joinTimeoutEnabled && now - state.createdNanos >= joinTimeoutNanos;
            if (!closing && !timedOut) {
                break;
            }
            joinStates.remove(state);
            if (state.receivedCount < inputs.length) {
                if (hotMetricsEnabled) {
                    metrics.recordMissingJoinBranch();
                }
            }
            if ((timedOut || closing) && joinMissingBranchPolicy == JoinSpec.MissingBranchPolicy.EMIT_PARTIAL) {
                if (joinKind != JoinSpec.JoinKind.ANY_OF || !state.emitted) {
                    emitJoin(state, "", timedOut, true);
                    joinStates.recycle(state);
                } else {
                    releaseAndRecycleJoinState(state);
                }
            } else {
                releaseAndRecycleJoinState(state);
            }
            if (timedOut) {
                if (hotMetricsEnabled) {
                    metrics.recordTimedOutJoinGroup();
                }
            }
        }
    }

    private void emitJoin(
        final JoinState state,
        final String triggeringSource,
        final boolean timedOut,
        final boolean releaseValues
    ) {
        joinValuesMap.state(state);
        if (state.longStamp) {
            reusableJoinGroup.resetRuntime(
                state.longStampValue,
                state.receivedCount == inputs.length,
                timedOut,
                triggeringSource
            );
        } else {
            reusableJoinGroup.resetRuntime(
                state.objectStamp,
                state.receivedCount == inputs.length,
                timedOut,
                triggeringSource
            );
        }
        final Object outputItem = joinCombiner.apply(reusableJoinGroup);
        final Output<Object> localOutput = output;
        if (!messageMayCarryOwnedHandle) {
            localOutput.push(outputItem);
        } else {
            try (HandleOwnership.Scope ignored = HandleOwnership.scope(state.values, state.values.length)) {
                localOutput.push(outputItem);
            }
        }
        if (hotMetricsEnabled) {
            metrics.recordCompletedJoinGroup();
        }
        if (releaseValues) {
            releaseJoinValues(state);
        }
        joinValuesMap.clearState(state);
    }

    private void releaseJoinState() {
        if (joinStates == null) {
            return;
        }
        while (true) {
            final JoinState state = joinStates.removeEldest();
            if (state == null) {
                return;
            }
            releaseAndRecycleJoinState(state);
        }
    }

    private void releaseJoinValues(final JoinState state) {
        final Object[] values = state.values;
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (hotMetricsEnabled && value instanceof SlabHandle<?>) {
                metrics.recordReleasedHandle();
            }
            releaseIfHandle(value);
            values[i] = null;
        }
        state.receivedCount = 0;
    }

    private void releaseAndRecycleJoinState(final JoinState state) {
        releaseJoinValues(state);
        joinStates.recycle(state);
    }

    private boolean allInputsClosedAndEmpty() {
        for (int i = 0; i < inputs.length; i++) {
            if (!inputs[i].isClosed() || !inputs[i].isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private void releaseBatchItems(final int received) {
        if (processingMode == PROCESS_BROADCAST || processingMode == PROCESS_JOIN) {
            return;
        }
        final Object[] localBatchItems = batchItems;
        for (int i = 0; i < received; i++) {
            final Object item = localBatchItems[i];
            if (item != null) {
                if (messageMayCarryOwnedHandle) {
                    releaseIfHandle(item);
                }
                localBatchItems[i] = null;
            }
        }
    }

    private WorkerState handleException(final Throwable ex) {
        final FusedStageException fusedFailure = ex instanceof FusedStageException failure ? failure : null;
        final String failedStageName = fusedFailure == null ? stageName : fusedFailure.stageName();
        final StageMetrics failedMetrics = fusedFailure == null ? metrics : fusedFailure.metrics();
        final RuntimeStageContext failedContext = fusedFailure == null ? context : fusedFailure.context();
        final Throwable cause = fusedFailure == null ? ex : fusedFailure.getCause();

        failedMetrics.recordException();
        final StageExceptionAction action;
        try {
            action = exceptionHandler.onException(graphName, failedStageName, cause, failedContext);
        } catch (final Throwable handlerFailure) {
            coordinator.fail(failedStageName, handlerFailure);
            return WorkerState.FAILED;
        }

        if (action == StageExceptionAction.POISON_STAGE) {
            markFusedPoisoned(fusedFailure);
            coordinator.poison(failedStageName, cause, inputs, outputEdges);
            return WorkerState.POISONED;
        }

        coordinator.fail(failedStageName, cause);
        return WorkerState.FAILED;
    }

    private void markFusedPoisoned(final FusedStageException failure) {
        if (failure == null) {
            return;
        }
        final FusedStage stage = fusedStage;
        if (stage != null && stage.name().equals(failure.stageName())) {
            stage.markPoisoned();
        }
        markFusedPoisoned(stage == null ? null : stage.nextStage(), failure.stageName());
        final FusedSink sink = fusedSink;
        if (sink != null && sink.name().equals(failure.stageName())) {
            sink.markPoisoned();
        }
        final FusedRouter router = fusedRouter;
        if (router != null && router.name().equals(failure.stageName())) {
            router.markPoisoned();
        }
        final FusedSink terminalSink = stage == null ? null : stage.terminalSink();
        if (terminalSink != null && terminalSink.name().equals(failure.stageName())) {
            terminalSink.markPoisoned();
        }
    }

    private void markFusedPoisoned(final FusedStage stage, final String failedStageName) {
        if (stage == null) {
            return;
        }
        if (stage.name().equals(failedStageName)) {
            stage.markPoisoned();
        }
        markFusedPoisoned(stage.nextStage(), failedStageName);
        final FusedSink terminalSink = stage.terminalSink();
        if (terminalSink != null && terminalSink.name().equals(failedStageName)) {
            terminalSink.markPoisoned();
        }
    }

    private void closeOutputs() {
        for (int i = 0; i < outputEdges.length; i++) {
            outputEdges[i].close();
        }
    }

    private void workerState(final WorkerState state) {
        if (currentWorkerState != state) {
            metrics.workerState(state);
            currentWorkerState = state;
        }
    }

    private static BatchPolicy activeBatchPolicy(final NodeDefinition node, final EdgeSpec[] inputSpecs) {
        if (node.kind() == GraphPlan.NodeKind.JOIN || inputSpecs.length == 0) {
            final BatchPolicy stagePolicy = node.spec().batchPolicy();
            return stagePolicy.kind() == BatchPolicy.BatchKind.DISABLED ? BatchPolicy.maxItems(64) : stagePolicy;
        }
        final BatchPolicy stagePolicy = node.spec().batchPolicy();
        if (stagePolicy.kind() != BatchPolicy.BatchKind.DISABLED) {
            return stagePolicy;
        }
        return inputSpecs[0].batchPolicy();
    }

    private static int batchLimit(final BatchPolicy policy) {
        return policy.kind() == BatchPolicy.BatchKind.DISABLED
            ? DEFAULT_SINGLE_MESSAGE_BATCH_SIZE
            : Math.max(1, policy.maxItems());
    }

    static int defaultSingleMessageBatchSize() {
        return DEFAULT_SINGLE_MESSAGE_BATCH_SIZE;
    }

    private static int parkIdleThreshold(final WaitSpec spec) {
        return switch (spec.kind()) {
            case BUSY_SPIN -> -1;
            case BLOCKING -> 0;
            case PHASED -> spec.parkNanos().isZero() ? -1 : spec.spins() + spec.yields();
        };
    }

    private static int processingMode(final NodeDefinition node) {
        return switch (node.kind()) {
            case SINK -> PROCESS_SINK;
            case DISPATCH -> PROCESS_DISPATCH;
            case BROADCAST -> PROCESS_BROADCAST;
            case PARTITION -> PROCESS_PARTITION;
            case JOIN -> PROCESS_JOIN;
            case STAGE -> node.batchLogic() == null ? PROCESS_MESSAGES : PROCESS_BATCH;
            default -> throw new IllegalStateException("node is not executable: " + node.kind());
        };
    }

    private static int[] weightedSchedule(final DispatchSpec<Object> dispatchSpec, final int branchCount) {
        if (dispatchSpec.kind() != DispatchSpec.DispatchKind.WEIGHTED) {
            return new int[0];
        }
        final int[] weights = dispatchSpec.weights();
        long total = 0L;
        for (final int weight : weights) {
            total += weight;
            if (total > MAX_WEIGHTED_DISPATCH_SCHEDULE) {
                throw new GraphBuildException("weighted dispatch schedule is too large: " + total);
            }
        }
        final int[] schedule = new int[(int) total];
        int cursor = 0;
        for (int branch = 0; branch < branchCount; branch++) {
            for (int i = 0; i < weights[branch]; i++) {
                schedule[cursor++] = branch;
            }
        }
        return schedule;
    }

    private static EdgeMetrics[] outputMetrics(final EdgeSender[] outputSenders) {
        final EdgeMetrics[] metricsByBranch = new EdgeMetrics[outputSenders.length];
        for (int i = 0; i < outputSenders.length; i++) {
            metricsByBranch[i] = outputSenders[i].edgeMetrics();
        }
        return metricsByBranch;
    }

    private static int floorMod(final int value, final int modulo) {
        final int result = value % modulo;
        return result < 0 ? result + modulo : result;
    }

    private static int powerOfTwoMask(final int value) {
        return value > 0 && Integer.bitCount(value) == 1 ? value - 1 : -1;
    }

    private static int spread(final int value) {
        return value ^ (value >>> 16);
    }

    private static void releaseIfHandle(final Object item) {
        if (item instanceof SlabHandle<?> handle) {
            handle.release();
        } else if (item instanceof Stamped<?> stamped) {
            releaseIfHandle(stamped.value());
        }
    }

    private static boolean mayCarryOwnedHandle(final Class<?> inputType) {
        return inputType == Object.class
            || inputType.isAssignableFrom(SlabHandle.class)
            || inputType.isAssignableFrom(Stamped.class);
    }

    private void recordLogicalTransfer(
        final StageMetrics ownerMetrics,
        final EdgeMetrics logicalEdgeMetrics,
        final GraphMetrics graphMetrics,
        final StageMetrics consumerMetrics
    ) {
        // Single static-final gate; the JIT folds the entire call away when off.
        if (!LOGICAL_METRICS_ON) {
            return;
        }
        logicalEdgeMetrics.recordEmit();
        graphMetrics.recordEmit();
        ownerMetrics.recordEmit();
        logicalEdgeMetrics.recordConsume();
        graphMetrics.recordConsume();
        consumerMetrics.recordConsume();
    }

    private static <T> T ObjectsRequireNonNull(final T value, final String label) {
        if (value == null) {
            throw new NullPointerException(label);
        }
        return value;
    }

    static final class FusedSink {
        private final String name;
        private final Class<?> inputType;
        private final String inputTypeName;
        private final boolean acceptsAnyType;
        private final Consumer<Object> consumer;
        private final StageMetrics metrics;
        private final StageMetrics ownerMetrics;
        private final EdgeMetrics logicalEdgeMetrics;
        private final GraphMetrics graphMetrics;
        private final RuntimeStageContext context;
        private boolean poisoned;

        @SuppressWarnings("unchecked")
        FusedSink(
            final String name,
            final Class<?> inputType,
            final Consumer<?> consumer,
            final StageMetrics metrics,
            final StageMetrics ownerMetrics,
            final EdgeMetrics logicalEdgeMetrics,
            final GraphMetrics graphMetrics,
            final RuntimeCoordinator coordinator
        ) {
            this.name = name;
            this.inputType = inputType;
            this.inputTypeName = inputType.getName();
            this.acceptsAnyType = inputType == Object.class;
            this.consumer = (Consumer<Object>) consumer;
            this.metrics = metrics;
            this.ownerMetrics = ownerMetrics;
            this.logicalEdgeMetrics = logicalEdgeMetrics;
            this.graphMetrics = graphMetrics;
            this.context = new RuntimeStageContext(coordinator, name, metrics);
        }

        String name() {
            return name;
        }

        StageMetrics metrics() {
            return metrics;
        }

        void markPoisoned() {
            poisoned = true;
        }

        boolean poisoned() {
            return poisoned;
        }
    }

    static final class FusedStage {
        private final String name;
        private final Class<?> inputType;
        private final String inputTypeName;
        private final boolean acceptsAnyType;
        private final StageLogic<Object, Object> logic;
        private final Output<Object> output;
        private final StageMetrics metrics;
        private final StageMetrics ownerMetrics;
        private final EdgeMetrics logicalEdgeMetrics;
        private final GraphMetrics graphMetrics;
        private final RuntimeStageContext context;
        private final FusedStage nextStage;
        private final FusedSink terminalSink;
        private boolean poisoned;

        @SuppressWarnings("unchecked")
        FusedStage(
            final String name,
            final Class<?> inputType,
            final StageLogic<?, ?> logic,
            final Output<Object> output,
            final StageMetrics metrics,
            final StageMetrics ownerMetrics,
            final EdgeMetrics logicalEdgeMetrics,
            final GraphMetrics graphMetrics,
            final RuntimeCoordinator coordinator,
            final FusedStage nextStage,
            final FusedSink terminalSink
        ) {
            this.name = name;
            this.inputType = inputType;
            this.inputTypeName = inputType.getName();
            this.acceptsAnyType = inputType == Object.class;
            this.logic = (StageLogic<Object, Object>) logic;
            this.output = output;
            this.metrics = metrics;
            this.ownerMetrics = ownerMetrics;
            this.logicalEdgeMetrics = logicalEdgeMetrics;
            this.graphMetrics = graphMetrics;
            this.context = new RuntimeStageContext(coordinator, name, metrics);
            this.nextStage = nextStage;
            this.terminalSink = terminalSink;
        }

        String name() {
            return name;
        }

        StageMetrics metrics() {
            return metrics;
        }

        FusedStage nextStage() {
            return nextStage;
        }

        FusedSink terminalSink() {
            return terminalSink;
        }

        void markPoisoned() {
            poisoned = true;
        }

        boolean poisoned() {
            return poisoned;
        }
    }

    static final class FusedRouter {
        private final String name;
        private final GraphPlan.NodeKind kind;
        private final Class<?> inputType;
        private final String inputTypeName;
        private final boolean acceptsAnyType;
        private final DispatchSpec<Object> dispatchSpec;
        private final BroadcastSpec<Object> broadcastSpec;
        private final PartitionSpec<Object, ?> partitionSpec;
        private final EdgeSender[] outputSenders;
        private final EdgeMetrics[] outputMetrics;
        private final StageMetrics metrics;
        private final StageMetrics ownerMetrics;
        private final EdgeMetrics logicalEdgeMetrics;
        private final GraphMetrics graphMetrics;
        private final RuntimeStageContext context;
        private final boolean broadcastSlabHandles;
        private final boolean isolateBroadcastBranches;
        private final Function<Object, Object> broadcastCopier;
        private final Function<Object, ?> partitionKeyExtractor;
        private final int[] weightedSchedule;
        private final long[] partitionLaneCounts;
        private final long partitionHotKeyThreshold;
        private final int outputMask;
        private long routeSequence;
        private boolean poisoned;

        @SuppressWarnings("unchecked")
        FusedRouter(
            final NodeDefinition router,
            final EdgeSender[] outputSenders,
            final StageMetrics metrics,
            final StageMetrics ownerMetrics,
            final EdgeMetrics logicalEdgeMetrics,
            final GraphMetrics graphMetrics,
            final RuntimeCoordinator coordinator
        ) {
            this.name = router.name();
            this.kind = router.kind();
            this.inputType = router.inputType();
            this.inputTypeName = inputType.getName();
            this.acceptsAnyType = inputType == Object.class;
            this.dispatchSpec = (DispatchSpec<Object>) router.dispatchSpec();
            this.broadcastSpec = (BroadcastSpec<Object>) router.broadcastSpec();
            this.partitionSpec = (PartitionSpec<Object, ?>) router.partitionSpec();
            this.outputSenders = outputSenders.clone();
            this.outputMetrics = outputMetrics(outputSenders);
            this.metrics = metrics;
            this.ownerMetrics = ownerMetrics;
            this.logicalEdgeMetrics = logicalEdgeMetrics;
            this.graphMetrics = graphMetrics;
            this.context = new RuntimeStageContext(coordinator, name, metrics);
            this.broadcastSlabHandles = broadcastSpec != null
                && broadcastSpec.kind() == BroadcastSpec.BroadcastKind.SLAB_HANDLE;
            this.isolateBroadcastBranches = broadcastSpec != null && broadcastSpec.isolateSlowBranches();
            this.broadcastCopier = broadcastSpec == null ? null : (Function<Object, Object>) broadcastSpec.copier();
            this.partitionKeyExtractor = partitionSpec == null
                ? null
                : (Function<Object, ?>) partitionSpec.keyExtractor();
            this.weightedSchedule = dispatchSpec == null
                ? new int[0]
                : weightedSchedule(dispatchSpec, outputSenders.length);
            this.partitionLaneCounts = partitionSpec == null ? new long[0] : new long[outputSenders.length];
            this.partitionHotKeyThreshold = partitionSpec == null ? 0L : partitionSpec.hotKeyThreshold();
            this.outputMask = powerOfTwoMask(outputSenders.length);
        }

        String name() {
            return name;
        }

        StageMetrics metrics() {
            return metrics;
        }

        void markPoisoned() {
            poisoned = true;
        }

        boolean poisoned() {
            return poisoned;
        }
    }

    private final class DirectRouterOutput implements Output<Object> {
        private final FusedRouter router;
        private final boolean retainForScope;

        private DirectRouterOutput(final FusedRouter router) {
            this.router = router;
            this.retainForScope = mayCarryOwnedHandle(router.inputType);
        }

        @Override
        public void push(final Object item) {
            emit(item);
        }

        @Override
        public boolean push(final Object item, final Duration timeout) {
            emit(item);
            return true;
        }

        @Override
        public boolean tryPush(final Object item) {
            emit(item);
            return true;
        }

        private void emit(final Object item) {
            if (item == null) {
                throw new NullPointerException(router.name + " cannot consume null");
            }
            final Object outbound = retainForScope ? HandleOwnership.prepareForEnqueue(item) : item;
            if (LATTICE_VALIDATE_FUSED) {
                validate(outbound);
            }
            recordLogicalTransfer(router.ownerMetrics, router.logicalEdgeMetrics, router.graphMetrics, router.metrics);

            final boolean timeBatchesLocal = StageMetrics.histogramsEnabled() || JfrEvents.enabled();
            final long started = timeBatchesLocal ? System.nanoTime() : 0L;
            try {
                if (retainForScope) {
                    try (HandleOwnership.Scope ignored = HandleOwnership.scope(outbound)) {
                        route(outbound);
                    }
                } else {
                    route(outbound);
                }
                final long serviceNanos = timeBatchesLocal ? System.nanoTime() - started : 0L;
                if (StageMetrics.hotCountersEnabled()) {
                    router.metrics.recordBatch(1, serviceNanos);
                }
                if (JfrEvents.enabled()) {
                    JfrEvents.batchProcessed(graphName, router.name, 1, serviceNanos);
                }
            } catch (final Throwable ex) {
                throw new FusedStageException(router.name, router.metrics, router.context, ex);
            } finally {
                if (retainForScope) {
                    releaseIfHandle(outbound);
                }
            }
        }

        private void route(final Object item) {
            switch (router.kind) {
                case DISPATCH -> routeDispatch(item);
                case BROADCAST -> routeBroadcast(item);
                case PARTITION -> routePartition(item);
                default -> throw new IllegalStateException("not a router: " + router.kind);
            }
        }

        private void routeDispatch(final Object item) {
            final int branch = dispatchBranch(item);
            if (hotMetricsEnabled) {
                router.metrics.recordRoutingDecision();
            }
            if (edgeHotMetricsEnabled) {
                router.outputMetrics[branch].recordLaneSelection();
            }
            router.outputSenders[branch].emit(item);
        }

        private int dispatchBranch(final Object item) {
            return switch (router.dispatchSpec.kind()) {
                case ROUND_ROBIN -> {
                    final long sequence = router.routeSequence++;
                    yield router.outputMask >= 0
                        ? (int) sequence & router.outputMask
                        : (int) (sequence % router.outputSenders.length);
                }
                case KEYED -> {
                    final Object key = router.dispatchSpec.keyExtractor().apply(item);
                    yield branchForKey(key);
                }
                case WEIGHTED -> router.weightedSchedule[(int) (router.routeSequence++ % router.weightedSchedule.length)];
            };
        }

        private void routeBroadcast(final Object item) {
            if (router.broadcastSlabHandles) {
                broadcastHandle(item);
            } else {
                broadcastCopy(item);
            }
            if (hotMetricsEnabled) {
                router.metrics.recordRoutingDecision();
            }
        }

        private void broadcastCopy(final Object item) {
            final EdgeSender[] senders = router.outputSenders;
            final EdgeMetrics[] metricsByBranch = router.outputMetrics;
            final Function<Object, Object> copier = router.broadcastCopier;
            final boolean isolate = router.isolateBroadcastBranches;
            for (int branch = 0; branch < senders.length; branch++) {
                final Object branchItem = copier == null ? item : copier.apply(item);
                if (isolate) {
                    if (!senders[branch].tryEmit(branchItem)) {
                        if (hotMetricsEnabled) {
                            router.metrics.recordBranchIsolationAction();
                        }
                        if (edgeHotMetricsEnabled) {
                            metricsByBranch[branch].recordBranchIsolationAction();
                        }
                        releaseIfHandle(branchItem);
                    }
                } else {
                    senders[branch].emit(branchItem);
                }
            }
        }

        private void broadcastHandle(final Object item) {
            if (!(item instanceof SlabHandle<?> handle)) {
                throw new IllegalArgumentException("slab-handle broadcast requires SlabHandle payloads");
            }
            final EdgeSender[] senders = router.outputSenders;
            final EdgeMetrics[] metricsByBranch = router.outputMetrics;
            final boolean isolate = router.isolateBroadcastBranches;
            for (int branch = 0; branch < senders.length; branch++) {
                final SlabHandle<?> branchHandle = handle.retain();
                if (hotMetricsEnabled) {
                    router.metrics.recordRetainedHandle();
                }
                if (isolate) {
                    if (!senders[branch].tryEmit(branchHandle)) {
                        if (hotMetricsEnabled) {
                            router.metrics.recordBranchIsolationAction();
                        }
                        if (edgeHotMetricsEnabled) {
                            metricsByBranch[branch].recordBranchIsolationAction();
                        }
                        branchHandle.release();
                        if (hotMetricsEnabled) {
                            router.metrics.recordReleasedHandle();
                        }
                    }
                } else {
                    boolean enqueued = false;
                    try {
                        senders[branch].emit(branchHandle);
                        enqueued = true;
                    } finally {
                        if (!enqueued) {
                            branchHandle.release();
                            if (hotMetricsEnabled) {
                                router.metrics.recordReleasedHandle();
                            }
                        }
                    }
                }
            }
        }

        private void routePartition(final Object item) {
            final Object key = router.partitionKeyExtractor.apply(item);
            final int lane = branchForKey(key);
            if (hotMetricsEnabled) {
                router.metrics.recordRoutingDecision();
            }
            if (edgeHotMetricsEnabled) {
                router.outputMetrics[lane].recordLaneSelection();
            }
            if (router.partitionHotKeyThreshold > 0L) {
                final long laneCount = ++router.partitionLaneCounts[lane];
                if (laneCount == router.partitionHotKeyThreshold && edgeHotMetricsEnabled) {
                    router.outputMetrics[lane].recordHotKeySignal();
                }
            }
            router.outputSenders[lane].emit(item);
        }

        private int branchForKey(final Object key) {
            final int hash = spread(key == null ? 0 : key.hashCode());
            if (router.outputMask >= 0) {
                return hash & router.outputMask;
            }
            return floorMod(hash, router.outputSenders.length);
        }

        private void validate(final Object item) {
            if (!router.acceptsAnyType && item.getClass() != router.inputType && !router.inputType.isInstance(item)) {
                throw new ClassCastException(router.name + " received " + item.getClass().getName()
                    + ", expected " + router.inputTypeName);
            }
        }
    }

    private final class DirectSinkOutput implements Output<Object> {
        private final FusedSink sink;
        private final boolean retainForScope;

        private DirectSinkOutput(final FusedSink sink) {
            this.sink = sink;
            this.retainForScope = mayCarryOwnedHandle(sink.inputType);
        }

        @Override
        public void push(final Object item) {
            emit(item);
        }

        @Override
        public boolean push(final Object item, final Duration timeout) {
            emit(item);
            return true;
        }

        @Override
        public boolean tryPush(final Object item) {
            emit(item);
            return true;
        }

        private void emit(final Object item) {
            if (item == null) {
                throw new NullPointerException(sink.name + " cannot consume null");
            }
            final Object outbound = retainForScope ? HandleOwnership.prepareForEnqueue(item) : item;
            if (LATTICE_VALIDATE_FUSED) {
                validate(outbound);
            }
            recordLogicalTransfer(sink.ownerMetrics, sink.logicalEdgeMetrics, sink.graphMetrics, sink.metrics);
            try {
                sink.consumer.accept(outbound);
                if (StageMetrics.hotCountersEnabled()) {
                    sink.metrics.recordBatch(1, 0L);
                }
            } catch (final Throwable ex) {
                throw new FusedStageException(sink.name, sink.metrics, sink.context, ex);
            } finally {
                if (retainForScope) {
                    releaseIfHandle(outbound);
                }
            }
        }

        private void validate(final Object item) {
            if (!sink.acceptsAnyType && item.getClass() != sink.inputType && !sink.inputType.isInstance(item)) {
                throw new ClassCastException(sink.name + " received " + item.getClass().getName()
                    + ", expected " + sink.inputTypeName);
            }
        }
    }

    private final class DirectStageOutput implements Output<Object> {
        private final FusedStage stage;
        private final Output<Object> output;
        private final boolean retainForScope;

        private DirectStageOutput(final FusedStage stage) {
            this.stage = stage;
            this.output = stage.nextStage() != null ? new DirectStageOutput(stage.nextStage())
                : stage.terminalSink() == null ? stage.output : new DirectSinkOutput(stage.terminalSink());
            this.retainForScope = mayCarryOwnedHandle(stage.inputType);
        }

        @Override
        public void push(final Object item) {
            emit(item);
        }

        @Override
        public boolean push(final Object item, final Duration timeout) {
            emit(item);
            return true;
        }

        @Override
        public boolean tryPush(final Object item) {
            emit(item);
            return true;
        }

        private void emit(final Object item) {
            if (item == null) {
                throw new NullPointerException(stage.name + " cannot consume null");
            }
            final Object outbound = retainForScope ? HandleOwnership.prepareForEnqueue(item) : item;
            if (LATTICE_VALIDATE_FUSED) {
                validate(outbound);
            }
            recordLogicalTransfer(stage.ownerMetrics, stage.logicalEdgeMetrics, stage.graphMetrics, stage.metrics);

            final boolean timeBatchesLocal = StageMetrics.histogramsEnabled() || JfrEvents.enabled();
            final long started = timeBatchesLocal ? System.nanoTime() : 0L;
            try {
                if (retainForScope) {
                    try (HandleOwnership.Scope ignored = HandleOwnership.scope(outbound)) {
                        processDirectStage(outbound, started, timeBatchesLocal);
                    }
                } else {
                    processDirectStage(outbound, started, timeBatchesLocal);
                }
            } catch (final FusedStageException ex) {
                throw ex;
            } catch (final Throwable ex) {
                throw new FusedStageException(stage.name, stage.metrics, stage.context, ex);
            } finally {
                if (retainForScope) {
                    releaseIfHandle(outbound);
                }
            }
        }

        private void processDirectStage(final Object outbound, final long started, final boolean timeBatchesLocal) throws Exception {
            stage.logic.onMessage(outbound, output, stage.context);
            if (StageMetrics.hotCountersEnabled()) {
                final long serviceNanos = timeBatchesLocal ? System.nanoTime() - started : 0L;
                stage.metrics.recordBatch(1, serviceNanos);
                if (JfrEvents.enabled()) {
                    JfrEvents.batchProcessed(graphName, stage.name, 1, serviceNanos);
                }
            } else if (JfrEvents.enabled()) {
                final long serviceNanos = timeBatchesLocal ? System.nanoTime() - started : 0L;
                JfrEvents.batchProcessed(graphName, stage.name, 1, serviceNanos);
            }
        }

        private void validate(final Object item) {
            // Null already handled in emit(); keep this for the optional type assertion.
            if (!stage.acceptsAnyType && item.getClass() != stage.inputType && !stage.inputType.isInstance(item)) {
                throw new ClassCastException(stage.name + " received " + item.getClass().getName()
                    + ", expected " + stage.inputTypeName);
            }
        }
    }

    private final class LinearStageSinkLoop {
        private final FusedStage[] stages;
        private final FusedSink sink;
        private final Output<Object> tailOutput;
        private final Output<Object>[] outputs;
        private final boolean retainForScope;

        @SuppressWarnings("unchecked")
        private LinearStageSinkLoop(
            final FusedStage[] stages,
            final FusedSink sink,
            final Output<Object> tailOutput
        ) {
            this.stages = stages;
            this.sink = sink;
            this.tailOutput = tailOutput;
            this.retainForScope = mayCarryOwnedHandle(stages) || mayCarryOwnedHandle(sink);
            this.outputs = new Output[stages.length + 1];
            final boolean retain = retainForScope;
            if (sink != null) {
                outputs[stages.length] = retain ? new RetainingLinearSinkOutput() : new BenignLinearSinkOutput();
            } else if (tailOutput != null) {
                outputs[stages.length] = tailOutput;
            } else {
                throw new IllegalStateException("linear fused chain must terminate in a sink or tail output");
            }
            for (int i = stages.length - 1; i >= 0; i--) {
                final boolean entry = i == 0;
                outputs[i] = retain
                    ? new RetainingLinearStageOutput(i, outputs[i + 1])
                    : new BenignLinearStageOutput(i, outputs[i + 1], entry);
            }
        }

        Output<Object> entryOutput() {
            return outputs[0];
        }

        private void emitStageBenign(final FusedStage stage, final Output<Object> next, final Object item) {
            final boolean timeBatchesLocal = timeBatches;
            final long started = timeBatchesLocal ? System.nanoTime() : 0L;
            try {
                if (item == null) {
                    throw new NullPointerException(stage.name + " cannot consume null");
                }
                if (LATTICE_VALIDATE_FUSED) {
                    validate(stage, item);
                }
                recordLogicalTransfer(stage.ownerMetrics, stage.logicalEdgeMetrics, stage.graphMetrics, stage.metrics);
                stage.logic.onMessage(item, next, stage.context);
                if (StageMetrics.hotCountersEnabled()) {
                    final long serviceNanos = timeBatchesLocal ? System.nanoTime() - started : 0L;
                    stage.metrics.recordBatch(1, serviceNanos);
                    if (JfrEvents.enabled()) {
                        JfrEvents.batchProcessed(graphName, stage.name, 1, serviceNanos);
                    }
                } else if (JfrEvents.enabled()) {
                    JfrEvents.batchProcessed(graphName, stage.name, 1, System.nanoTime() - started);
                }
            } catch (final FusedStageException ex) {
                throw ex;
            } catch (final Throwable ex) {
                throw new FusedStageException(stage.name, stage.metrics, stage.context, ex);
            }
        }

        private void emitSinkBenign(final Object item) {
            try {
                if (item == null) {
                    throw new NullPointerException(sink.name + " cannot consume null");
                }
                if (LATTICE_VALIDATE_FUSED) {
                    validate(sink, item);
                }
                recordLogicalTransfer(sink.ownerMetrics, sink.logicalEdgeMetrics, sink.graphMetrics, sink.metrics);
                sink.consumer.accept(item);
                if (StageMetrics.hotCountersEnabled()) {
                    sink.metrics.recordBatch(1, 0L);
                }
            } catch (final Throwable ex) {
                throw new FusedStageException(sink.name, sink.metrics, sink.context, ex);
            }
        }

        private void emitStageRetaining(final FusedStage stage, final Output<Object> next, final Object item) {
            if (item == null) {
                throw new NullPointerException(stage.name + " cannot consume null");
            }
            final Object outbound = HandleOwnership.prepareForEnqueue(item);
            if (LATTICE_VALIDATE_FUSED) {
                validate(stage, outbound);
            }
            recordLogicalTransfer(stage.ownerMetrics, stage.logicalEdgeMetrics, stage.graphMetrics, stage.metrics);

            final boolean timeBatchesLocal = StageMetrics.histogramsEnabled() || JfrEvents.enabled();
            final long started = timeBatchesLocal ? System.nanoTime() : 0L;
            try {
                try (HandleOwnership.Scope ignored = HandleOwnership.scope(outbound)) {
                    stage.logic.onMessage(outbound, next, stage.context);
                    if (StageMetrics.hotCountersEnabled()) {
                        final long serviceNanos = timeBatchesLocal ? System.nanoTime() - started : 0L;
                        stage.metrics.recordBatch(1, serviceNanos);
                        if (JfrEvents.enabled()) {
                            JfrEvents.batchProcessed(graphName, stage.name, 1, serviceNanos);
                        }
                    }
                }
            } catch (final FusedStageException ex) {
                throw ex;
            } catch (final Throwable ex) {
                throw new FusedStageException(stage.name, stage.metrics, stage.context, ex);
            } finally {
                releaseIfHandle(outbound);
            }
        }

        private void emitSinkRetaining(final Object item) {
            if (item == null) {
                throw new NullPointerException(sink.name + " cannot consume null");
            }
            final Object outbound = HandleOwnership.prepareForEnqueue(item);
            if (LATTICE_VALIDATE_FUSED) {
                validate(sink, outbound);
            }
            recordLogicalTransfer(sink.ownerMetrics, sink.logicalEdgeMetrics, sink.graphMetrics, sink.metrics);
            try {
                sink.consumer.accept(outbound);
                if (StageMetrics.hotCountersEnabled()) {
                    sink.metrics.recordBatch(1, 0L);
                }
            } catch (final Throwable ex) {
                throw new FusedStageException(sink.name, sink.metrics, sink.context, ex);
            } finally {
                releaseIfHandle(outbound);
            }
        }

        private static boolean mayCarryOwnedHandle(final FusedStage[] stages) {
            for (int i = 0; i < stages.length; i++) {
                if (mayCarryOwnedHandle(stages[i].inputType)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean mayCarryOwnedHandle(final FusedSink sink) {
            return sink != null && mayCarryOwnedHandle(sink.inputType);
        }

        private static boolean mayCarryOwnedHandle(final Class<?> inputType) {
            return inputType == Object.class
                || inputType.isAssignableFrom(SlabHandle.class)
                || inputType.isAssignableFrom(Stamped.class);
        }

        private void validate(final FusedStage stage, final Object item) {
            if (item == null) {
                throw new NullPointerException(stage.name + " cannot consume null");
            }
            if (!stage.acceptsAnyType && item.getClass() != stage.inputType && !stage.inputType.isInstance(item)) {
                throw new ClassCastException(stage.name + " received " + item.getClass().getName()
                    + ", expected " + stage.inputTypeName);
            }
        }

        private void validate(final FusedSink sink, final Object item) {
            if (item == null) {
                throw new NullPointerException(sink.name + " cannot consume null");
            }
            if (!sink.acceptsAnyType && item.getClass() != sink.inputType && !sink.inputType.isInstance(item)) {
                throw new ClassCastException(sink.name + " received " + item.getClass().getName()
                    + ", expected " + sink.inputTypeName);
            }
        }

        private final class BenignLinearStageOutput implements Output<Object> {
            private final FusedStage stage;
            private final Output<Object> next;

            private BenignLinearStageOutput(final int index, final Output<Object> next, final boolean entry) {
                this.stage = stages[index];
                this.next = next;
            }

            @Override
            public void push(final Object item) {
                emitStageBenign(stage, next, item);
            }

            @Override
            public boolean push(final Object item, final Duration timeout) {
                emitStageBenign(stage, next, item);
                return true;
            }

            @Override
            public boolean tryPush(final Object item) {
                emitStageBenign(stage, next, item);
                return true;
            }
        }

        private final class BenignLinearSinkOutput implements Output<Object> {
            @Override
            public void push(final Object item) {
                emitSinkBenign(item);
            }

            @Override
            public boolean push(final Object item, final Duration timeout) {
                emitSinkBenign(item);
                return true;
            }

            @Override
            public boolean tryPush(final Object item) {
                emitSinkBenign(item);
                return true;
            }
        }

        private final class RetainingLinearStageOutput implements Output<Object> {
            private final FusedStage stage;
            private final Output<Object> next;

            private RetainingLinearStageOutput(final int index, final Output<Object> next) {
                this.stage = stages[index];
                this.next = next;
            }

            @Override
            public void push(final Object item) {
                emitStageRetaining(stage, next, item);
            }

            @Override
            public boolean push(final Object item, final Duration timeout) {
                emitStageRetaining(stage, next, item);
                return true;
            }

            @Override
            public boolean tryPush(final Object item) {
                emitStageRetaining(stage, next, item);
                return true;
            }
        }

        private final class RetainingLinearSinkOutput implements Output<Object> {
            @Override
            public void push(final Object item) {
                emitSinkRetaining(item);
            }

            @Override
            public boolean push(final Object item, final Duration timeout) {
                emitSinkRetaining(item);
                return true;
            }

            @Override
            public boolean tryPush(final Object item) {
                emitSinkRetaining(item);
                return true;
            }
        }
    }

    static final class FusedStageException extends RuntimeException {
        private final String stageName;
        private final StageMetrics metrics;
        private final RuntimeStageContext context;

        FusedStageException(
            final String stageName,
            final StageMetrics metrics,
            final RuntimeStageContext context,
            final Throwable cause
        ) {
            super(cause);
            this.stageName = stageName;
            this.metrics = metrics;
            this.context = context;
        }

        String stageName() {
            return stageName;
        }

        StageMetrics metrics() {
            return metrics;
        }

        RuntimeStageContext context() {
            return context;
        }
    }

    private interface JoinStateTable {
        int size();

        boolean isEmpty();

        JoinState eldest();

        JoinState get(Object stamp);

        JoinState get(long stamp);

        JoinState create(Object stamp, long createdNanos);

        JoinState create(long stamp, long createdNanos);

        void remove(JoinState state);

        JoinState removeEldest();

        void recycle(JoinState state);
    }

    private abstract static class AbstractJoinStateTable implements JoinStateTable {
        final int capacity;
        private JoinState freeHead;
        private JoinState eldest;
        private JoinState newest;
        private int size;

        AbstractJoinStateTable(final int capacity, final int sourceCount) {
            this.capacity = capacity;
            for (int i = 0; i < capacity; i++) {
                final JoinState state = new JoinState(sourceCount);
                state.nextFree = freeHead;
                freeHead = state;
            }
        }

        @Override
        public final int size() {
            return size;
        }

        @Override
        public final boolean isEmpty() {
            return size == 0;
        }

        @Override
        public final JoinState eldest() {
            return eldest;
        }

        @Override
        public final JoinState removeEldest() {
            final JoinState state = eldest;
            if (state != null) {
                remove(state);
            }
            return state;
        }

        @Override
        public final void recycle(final JoinState state) {
            state.clearForRecycle();
            state.nextFree = freeHead;
            freeHead = state;
        }

        final JoinState takeState() {
            final JoinState state = freeHead;
            if (state == null) {
                throw new IllegalStateException("join state pool exhausted");
            }
            freeHead = state.nextFree;
            state.nextFree = null;
            return state;
        }

        final void linkNewest(final JoinState state) {
            state.older = newest;
            state.newer = null;
            if (newest == null) {
                eldest = state;
            } else {
                newest.newer = state;
            }
            newest = state;
            size++;
        }

        final void unlink(final JoinState state) {
            final JoinState older = state.older;
            final JoinState newer = state.newer;
            if (older == null) {
                eldest = newer;
            } else {
                older.newer = newer;
            }
            if (newer == null) {
                newest = older;
            } else {
                newer.older = older;
            }
            state.older = null;
            state.newer = null;
            size--;
        }

        static int tableSize(final int capacity) {
            int size = 1;
            final int target = Math.max(2, capacity << 1);
            while (size < target) {
                size <<= 1;
            }
            return size;
        }
    }

    private static final class LongJoinStateTable extends AbstractJoinStateTable {
        private final long[] keys;
        private final JoinState[] states;
        private final int mask;

        LongJoinStateTable(final int capacity, final int sourceCount) {
            super(capacity, sourceCount);
            final int tableSize = tableSize(capacity);
            this.keys = new long[tableSize];
            this.states = new JoinState[tableSize];
            this.mask = tableSize - 1;
        }

        @Override
        public JoinState get(final Object stamp) {
            throw new IllegalStateException("long join state table requires long stamps");
        }

        @Override
        public JoinState get(final long stamp) {
            final int index = findIndex(stamp);
            return index < 0 ? null : states[index];
        }

        @Override
        public JoinState create(final Object stamp, final long createdNanos) {
            throw new IllegalStateException("long join state table requires long stamps");
        }

        @Override
        public JoinState create(final long stamp, final long createdNanos) {
            final JoinState state = takeState();
            state.resetLong(stamp, createdNanos);
            insert(stamp, state);
            linkNewest(state);
            return state;
        }

        @Override
        public void remove(final JoinState state) {
            final int index = findIndex(state.longStampValue);
            if (index >= 0) {
                deleteIndex(index);
                unlink(state);
            }
        }

        private int findIndex(final long stamp) {
            int index = spread(Long.hashCode(stamp)) & mask;
            while (states[index] != null) {
                if (keys[index] == stamp) {
                    return index;
                }
                index = (index + 1) & mask;
            }
            return -1;
        }

        private void insert(final long stamp, final JoinState state) {
            int index = spread(Long.hashCode(stamp)) & mask;
            while (states[index] != null) {
                index = (index + 1) & mask;
            }
            keys[index] = stamp;
            states[index] = state;
        }

        private void deleteIndex(final int deleteIndex) {
            keys[deleteIndex] = 0L;
            states[deleteIndex] = null;
            int index = (deleteIndex + 1) & mask;
            while (states[index] != null) {
                final long key = keys[index];
                final JoinState state = states[index];
                keys[index] = 0L;
                states[index] = null;
                insert(key, state);
                index = (index + 1) & mask;
            }
        }
    }

    private static final class ObjectJoinStateTable extends AbstractJoinStateTable {
        private final Object[] keys;
        private final JoinState[] states;
        private final int mask;

        ObjectJoinStateTable(final int capacity, final int sourceCount) {
            super(capacity, sourceCount);
            final int tableSize = tableSize(capacity);
            this.keys = new Object[tableSize];
            this.states = new JoinState[tableSize];
            this.mask = tableSize - 1;
        }

        @Override
        public JoinState get(final Object stamp) {
            final int index = findIndex(stamp);
            return index < 0 ? null : states[index];
        }

        @Override
        public JoinState get(final long stamp) {
            throw new IllegalStateException("object join state table requires object stamps");
        }

        @Override
        public JoinState create(final Object stamp, final long createdNanos) {
            final JoinState state = takeState();
            state.resetObject(stamp, createdNanos);
            insert(stamp, state);
            linkNewest(state);
            return state;
        }

        @Override
        public JoinState create(final long stamp, final long createdNanos) {
            throw new IllegalStateException("object join state table requires object stamps");
        }

        @Override
        public void remove(final JoinState state) {
            final int index = findIndex(state.objectStamp);
            if (index >= 0) {
                deleteIndex(index);
                unlink(state);
            }
        }

        private int findIndex(final Object stamp) {
            int index = spread(stamp.hashCode()) & mask;
            while (keys[index] != null) {
                if (keys[index].equals(stamp)) {
                    return index;
                }
                index = (index + 1) & mask;
            }
            return -1;
        }

        private void insert(final Object stamp, final JoinState state) {
            int index = spread(stamp.hashCode()) & mask;
            while (keys[index] != null) {
                index = (index + 1) & mask;
            }
            keys[index] = stamp;
            states[index] = state;
        }

        private void deleteIndex(final int deleteIndex) {
            keys[deleteIndex] = null;
            states[deleteIndex] = null;
            int index = (deleteIndex + 1) & mask;
            while (keys[index] != null) {
                final Object key = keys[index];
                final JoinState state = states[index];
                keys[index] = null;
                states[index] = null;
                insert(key, state);
                index = (index + 1) & mask;
            }
        }
    }

    private static final class JoinState {

        final Object[] values;
        long createdNanos;
        Object objectStamp;
        long longStampValue;
        boolean longStamp;
        int receivedCount;
        int seenCount;
        long seenMask;
        final boolean[] seenValues;
        boolean emitted;
        JoinState older;
        JoinState newer;
        JoinState nextFree;

        JoinState(final int sourceCount) {
            this.values = new Object[sourceCount];
            this.seenValues = sourceCount > Long.SIZE ? new boolean[sourceCount] : null;
        }

        void resetObject(final Object stamp, final long createdNanos) {
            this.objectStamp = stamp;
            this.longStampValue = 0L;
            this.longStamp = false;
            this.createdNanos = createdNanos;
        }

        void resetLong(final long stamp, final long createdNanos) {
            this.objectStamp = null;
            this.longStampValue = stamp;
            this.longStamp = true;
            this.createdNanos = createdNanos;
        }

        Object stamp() {
            return longStamp ? longStampValue : objectStamp;
        }

        boolean hasValue(final int sourceIndex) {
            return values[sourceIndex] != null;
        }

        boolean hasSeen(final int sourceIndex) {
            if (seenValues != null) {
                return seenValues[sourceIndex];
            }
            return (seenMask & (1L << sourceIndex)) != 0L;
        }

        void markSeen(final int sourceIndex) {
            if (seenValues != null) {
                seenValues[sourceIndex] = true;
            } else {
                seenMask |= 1L << sourceIndex;
            }
            seenCount++;
        }

        void setValue(final int sourceIndex, final Object item) {
            values[sourceIndex] = item;
            receivedCount++;
        }

        void clearForRecycle() {
            objectStamp = null;
            longStampValue = 0L;
            longStamp = false;
            createdNanos = 0L;
            receivedCount = 0;
            seenCount = 0;
            seenMask = 0L;
            if (seenValues != null) {
                for (int i = 0; i < seenValues.length; i++) {
                    seenValues[i] = false;
                }
            }
            emitted = false;
            older = null;
            newer = null;
            nextFree = null;
        }
    }

    private final class JoinValuesMap extends AbstractMap<String, Object> {
        private final String[] sources;
        private final String[] lookupKeys;
        private final int[] lookupIndexes;
        private final int lookupMask;
        private final JoinEntrySet entrySet = new JoinEntrySet();
        private JoinState state;

        JoinValuesMap(final String[] sources) {
            this.sources = sources.clone();
            final int lookupSize = joinValuesLookupSize(sources.length);
            this.lookupKeys = new String[lookupSize];
            this.lookupIndexes = new int[lookupSize];
            this.lookupMask = lookupSize - 1;
            for (int i = 0; i < sources.length; i++) {
                insertSourceLookup(sources[i], i);
            }
        }

        void state(final JoinState state) {
            this.state = state;
        }

        void clearState(final JoinState expected) {
            if (state == expected) {
                state = null;
            }
        }

        @Override
        public Object get(final Object key) {
            final JoinState current = state;
            if (current == null) {
                return null;
            }
            final int index = sourceIndex(key);
            return index < 0 ? null : current.values[index];
        }

        @Override
        public boolean containsKey(final Object key) {
            return get(key) != null;
        }

        @Override
        public int size() {
            final JoinState current = state;
            return current == null ? 0 : current.receivedCount;
        }

        @Override
        public Set<Map.Entry<String, Object>> entrySet() {
            return entrySet;
        }

        private void insertSourceLookup(final String source, final int sourceIndex) {
            int index = spread(source.hashCode()) & lookupMask;
            while (lookupKeys[index] != null) {
                index = (index + 1) & lookupMask;
            }
            lookupKeys[index] = source;
            lookupIndexes[index] = sourceIndex;
        }

        private int sourceIndex(final Object key) {
            if (key == null) {
                return -1;
            }
            int index = spread(key.hashCode()) & lookupMask;
            while (lookupKeys[index] != null) {
                if (lookupKeys[index].equals(key)) {
                    return lookupIndexes[index];
                }
                index = (index + 1) & lookupMask;
            }
            return -1;
        }

        private static int joinValuesLookupSize(final int sourceCount) {
            int size = 1;
            final int target = Math.max(2, sourceCount << 1);
            while (size < target) {
                size <<= 1;
            }
            return size;
        }

        private final class JoinEntrySet extends AbstractSet<Map.Entry<String, Object>> {
            private final JoinEntryIterator iterator = new JoinEntryIterator();

            @Override
            public Iterator<Map.Entry<String, Object>> iterator() {
                iterator.reset();
                return iterator;
            }

            @Override
            public int size() {
                return JoinValuesMap.this.size();
            }
        }

        private final class JoinEntryIterator implements Iterator<Map.Entry<String, Object>> {
            private final JoinEntry entry = new JoinEntry();
            private int nextIndex;

            void reset() {
                nextIndex = nextPresentIndex(0);
            }

            @Override
            public boolean hasNext() {
                return nextIndex < sources.length;
            }

            @Override
            public Map.Entry<String, Object> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                entry.index = nextIndex;
                nextIndex = nextPresentIndex(nextIndex + 1);
                return entry;
            }

            private int nextPresentIndex(final int startIndex) {
                final JoinState current = state;
                if (current == null) {
                    return sources.length;
                }
                for (int i = startIndex; i < sources.length; i++) {
                    if (current.values[i] != null) {
                        return i;
                    }
                }
                return sources.length;
            }
        }

        private final class JoinEntry implements Map.Entry<String, Object> {
            private int index;

            @Override
            public String getKey() {
                return sources[index];
            }

            @Override
            public Object getValue() {
                final JoinState current = state;
                return current == null ? null : current.values[index];
            }

            @Override
            public Object setValue(final Object value) {
                throw new UnsupportedOperationException("join values are read-only");
            }
        }
    }
}
