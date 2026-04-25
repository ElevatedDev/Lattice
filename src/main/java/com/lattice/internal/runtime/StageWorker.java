package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
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
import com.lattice.metrics.StageMetrics;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

final class StageWorker implements Runnable {

    private static final int PROCESS_MESSAGES = 0;
    private static final int PROCESS_BATCH = 1;
    private static final int PROCESS_SINK = 2;
    private static final int PROCESS_DISPATCH = 3;
    private static final int PROCESS_BROADCAST = 4;
    private static final int PROCESS_PARTITION = 5;
    private static final int PROCESS_JOIN = 6;

    private static final MessageEdge[] NO_EDGES = new MessageEdge[0];

    private final String graphName;
    private final String stageName;
    private final MessageEdge[] inputs;
    private final EdgeSpec[] inputSpecs;
    private final String[] inputSources;
    private final MessageEdge[] outputEdges;
    private final EdgeSender[] outputSenders;
    private final MessageEdge[] ownedEdges;
    private final Output<Object> output;
    private final StageMetrics metrics;
    private final RuntimeCoordinator coordinator;
    private final RuntimeStageContext context;
    private final WaitStrategy waitStrategy;
    private final StageExceptionHandler exceptionHandler;
    private final PinPolicy pinPolicy;
    private final StageLogic<Object, Object> logic;
    private final BatchStageLogic<Object, Object> batchLogic;
    private final Consumer<Object> sink;
    private final DispatchSpec<Object> dispatchSpec;
    private final BroadcastSpec<Object> broadcastSpec;
    private final PartitionSpec<Object, ?> partitionSpec;
    private final JoinSpec<?> joinSpec;
    private final int processingMode;
    private final int batchLimit;
    private final long lingerNanos;
    private final int parkIdleThreshold;
    private final boolean jfrEnabled;
    private final boolean timeBatches;
    private final int[] weightedSchedule;
    private final long[] partitionLaneCounts;
    private final int outputMask;
    private ArrayBatch batch;
    private Object[] batchItems;
    private int[] batchSources;
    private WorkerState currentWorkerState = WorkerState.NEW;
    private Thread thread;
    private long routeSequence;
    private LinkedHashMap<Object, JoinState> joinStates;

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
        final StageMetrics metrics,
        final RuntimeCoordinator coordinator,
        final StageExceptionHandler exceptionHandler
    ) {
        this.graphName = coordinator.graphName();
        this.stageName = node.name();
        this.inputs = inputs.clone();
        this.inputSpecs = inputSpecs.clone();
        this.inputSources = inputSources.clone();
        this.outputEdges = outputEdges.clone();
        this.outputSenders = outputSenders.clone();
        this.ownedEdges = ownedEdges.clone();
        this.output = output;
        this.metrics = metrics;
        this.coordinator = coordinator;
        this.context = new RuntimeStageContext(coordinator, stageName, metrics);
        final WaitSpec waitSpec = node.spec().waitSpec();
        this.waitStrategy = WaitStrategies.from(waitSpec);
        this.parkIdleThreshold = parkIdleThreshold(waitSpec);
        this.exceptionHandler = exceptionHandler;
        this.pinPolicy = node.spec().pinPolicy();
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
        this.processingMode = processingMode(node);
        this.weightedSchedule = dispatchSpec == null ? new int[0] : weightedSchedule(dispatchSpec, outputSenders.length);
        this.partitionLaneCounts = partitionSpec == null ? new long[0] : new long[outputSenders.length];
        this.jfrEnabled = JfrEvents.enabled();
        this.timeBatches = StageMetrics.histogramsEnabled() || jfrEnabled;
        this.outputMask = powerOfTwoMask(outputSenders.length);
    }

    void start() {
        workerState(WorkerState.STARTING);
        thread = Thread.ofPlatform()
            .name("lattice-" + graphName + "-" + stageName)
            .unstarted(this);

        thread.start();
    }

    void interrupt() {
        final Thread current = thread;
        if (current != null) {
            current.interrupt();
        }
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

    @Override
    public void run() {
        WorkerState terminalState = WorkerState.STOPPED;
        boolean bootstrapped = false;
        try {
            final PlacementResult placement = PlacementBootstrap.bootstrap(stageName, pinPolicy, ownedEdges);
            this.batch = new ArrayBatch(batchLimit);
            this.batchItems = batch.items();
            this.batchSources = new int[batchLimit];
            if (processingMode == PROCESS_JOIN) {
                this.joinStates = new LinkedHashMap<>(Math.min(joinSpec.capacity(), 1024));
            }
            recordPlacement(placement);
            bootstrapped = true;
            coordinator.workerBootstrapped();
            coordinator.awaitRunRelease();
            metrics.markStarted();
            workerState(WorkerState.RUNNING);

            int idle = 0;
            while (!coordinator.isAbortRequested()) {
                boolean active = false;
                int received = 0;
                try {
                    coordinator.workerActive();
                    active = true;
                    received = receive();
                    if (received == 0) {
                        coordinator.workerInactive();
                        active = false;
                        expireJoinGroups(false);
                        if (allInputsClosedAndEmpty()) {
                            break;
                        }
                        workerState(WorkerState.IDLE);
                        if (jfrEnabled) {
                            JfrEvents.workerBlocked(graphName, stageName);
                        }
                        final boolean willPark = parkIdleThreshold >= 0 && idle >= parkIdleThreshold;
                        idle = waitStrategy.idle(idle, metrics);
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
                    metrics.recordConsume(received);
                    final long started = timeBatches ? System.nanoTime() : 0L;
                    try {
                        processReceived(received);
                        final long serviceNanos = timeBatches ? System.nanoTime() - started : 0L;
                        metrics.recordBatch(received, serviceNanos);
                        if (jfrEnabled) {
                            JfrEvents.batchProcessed(graphName, stageName, received, serviceNanos);
                        }
                    } catch (final Throwable ex) {
                        releaseBatchItems(received);
                        throw ex;
                    }
                } finally {
                    if (active) {
                        coordinator.workerInactive();
                    }
                    batch.clear();
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
            workerState(coordinator.state() == GraphState.FAILED ? WorkerState.FAILED : terminalState);
            metrics.markStopped();
            if (batch != null) {
                batch.clear();
            }
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

    private int receiveJoinInputs() {
        int received = 0;
        while (received < batchLimit) {
            boolean progressed = false;
            for (int i = 0; i < inputs.length && received < batchLimit; i++) {
                final Object item = inputs[i].poll();
                if (item != null) {
                    batchItems[received] = item;
                    batchSources[received] = i;
                    received++;
                    progressed = true;
                }
            }
            if (!progressed) {
                break;
            }
        }
        batch.size(received);
        return received;
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
        for (int i = 0; i < received; i++) {
            final Object item = batch.itemAt(i);
            try {
                sink.accept(item);
            } finally {
                releaseIfHandle(item);
                batchItems[i] = null;
            }
        }
    }

    private void processBatch(final int received) throws Exception {
        try (HandleOwnership.Scope ignored = HandleOwnership.scope(batchItems, received)) {
            batchLogic.onBatch(batch, output, context);
        } finally {
            for (int i = 0; i < received; i++) {
                releaseIfHandle(batch.itemAt(i));
                batchItems[i] = null;
            }
        }
    }

    private void processMessages(final int received) throws Exception {
        for (int i = 0; i < received; i++) {
            final Object item = batch.itemAt(i);
            try (HandleOwnership.Scope ignored = HandleOwnership.scope(item)) {
                logic.onMessage(item, output, context);
            } finally {
                releaseIfHandle(item);
                batchItems[i] = null;
            }
        }
    }

    private void processDispatch(final int received) {
        for (int i = 0; i < received; i++) {
            final Object item = batch.itemAt(i);
            boolean transferred = false;
            try {
                final int branch = dispatchBranch(item);
                metrics.recordRoutingDecision();
                outputSenders[branch].edgeMetrics().recordLaneSelection();
                outputSenders[branch].emit(item);
                transferred = true;
            } finally {
                if (!transferred) {
                    releaseIfHandle(item);
                }
                batchItems[i] = null;
            }
        }
    }

    private void processBroadcast(final int received) {
        for (int i = 0; i < received; i++) {
            final Object item = batch.itemAt(i);
            if (broadcastSpec.kind() == BroadcastSpec.BroadcastKind.SLAB_HANDLE) {
                broadcastHandle(item);
            } else {
                broadcastCopy(item);
            }
            metrics.recordRoutingDecision();
        }
    }

    private void broadcastCopy(final Object item) {
        for (int branch = 0; branch < outputSenders.length; branch++) {
            final Object branchItem = copyForBranch(item);
            if (broadcastSpec.isolateSlowBranches()) {
                if (!outputSenders[branch].tryEmit(branchItem)) {
                    metrics.recordBranchIsolationAction();
                    outputSenders[branch].edgeMetrics().recordBranchIsolationAction();
                    releaseIfHandle(branchItem);
                }
            } else {
                outputSenders[branch].emit(branchItem);
            }
        }
    }

    private void broadcastHandle(final Object item) {
        if (!(item instanceof SlabHandle<?> handle)) {
            throw new IllegalArgumentException("slab-handle broadcast requires SlabHandle payloads");
        }
        try {
            for (int branch = 0; branch < outputSenders.length; branch++) {
                final SlabHandle<?> branchHandle = handle.retain();
                metrics.recordRetainedHandle();
                if (broadcastSpec.isolateSlowBranches()) {
                    if (!outputSenders[branch].tryEmit(branchHandle)) {
                        metrics.recordBranchIsolationAction();
                        outputSenders[branch].edgeMetrics().recordBranchIsolationAction();
                        branchHandle.release();
                        metrics.recordReleasedHandle();
                    }
                } else {
                    boolean enqueued = false;
                    try {
                        outputSenders[branch].emit(branchHandle);
                        enqueued = true;
                    } finally {
                        if (!enqueued) {
                            branchHandle.release();
                            metrics.recordReleasedHandle();
                        }
                    }
                }
            }
        } finally {
            handle.release();
            metrics.recordReleasedHandle();
        }
    }

    @SuppressWarnings("unchecked")
    private Object copyForBranch(final Object item) {
        if (broadcastSpec.copier() == null) {
            return item;
        }
        return ((BroadcastSpec<Object>) broadcastSpec).copier().apply(item);
    }

    private void processPartition(final int received) {
        for (int i = 0; i < received; i++) {
            final Object item = batch.itemAt(i);
            boolean transferred = false;
            try {
                final Object key = partitionSpec.keyExtractor().apply(item);
                final int lane = branchForKey(key);
                metrics.recordRoutingDecision();
                outputSenders[lane].edgeMetrics().recordLaneSelection();
                final long laneCount = ++partitionLaneCounts[lane];
                if (partitionSpec.hotKeyThreshold() > 0L && laneCount == partitionSpec.hotKeyThreshold()) {
                    outputSenders[lane].edgeMetrics().recordHotKeySignal();
                }
                outputSenders[lane].emit(item);
                transferred = true;
            } finally {
                if (!transferred) {
                    releaseIfHandle(item);
                }
                batchItems[i] = null;
            }
        }
    }

    private void processJoin(final int received) {
        for (int i = 0; i < received; i++) {
            processJoinItem(batchSources[i], batch.itemAt(i));
        }
        expireJoinGroups(false);
    }

    private int dispatchBranch(final Object item) {
        return switch (dispatchSpec.kind()) {
            case ROUND_ROBIN -> (int) (routeSequence++ % outputSenders.length);
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
        final Object stamp = ObjectsRequireNonNull(joinSpec.stampExtractor().apply(item), "join stamp");
        final String source = inputSources[sourceIndex];
        JoinState state = joinStates.get(stamp);
        if (state == null) {
            ensureJoinCapacity();
            state = new JoinState(System.nanoTime());
            joinStates.put(stamp, state);
            metrics.recordOpenJoinGroup();
        }

        if (state.values.containsKey(source) || (joinSpec.kind() == JoinSpec.JoinKind.ANY_OF && state.emitted)) {
            handleDuplicateJoinStamp(stamp, source, item);
            return;
        }

        state.values.put(source, item);
        if (item instanceof SlabHandle<?>) {
            metrics.recordRetainedHandle();
        }

        if (joinSpec.kind() == JoinSpec.JoinKind.ANY_OF) {
            if (!state.emitted) {
                emitJoin(stamp, state, source, false, false);
                state.emitted = true;
            }
            if (state.values.size() == inputs.length) {
                joinStates.remove(stamp);
                releaseJoinValues(state);
            }
            return;
        }
        if (state.values.size() == inputs.length) {
            emitJoin(stamp, state, source, false, true);
            joinStates.remove(stamp);
        }
    }

    private void handleDuplicateJoinStamp(final Object stamp, final String source, final Object item) {
        switch (joinSpec.duplicatePolicy()) {
            case IGNORE -> releaseIfHandle(item);
            case COUNT -> {
                metrics.recordDuplicateJoinStamp();
                releaseIfHandle(item);
            }
            case FAIL -> {
                metrics.recordDuplicateJoinStamp();
                releaseIfHandle(item);
                throw new IllegalStateException("duplicate join stamp " + stamp + " from " + source);
            }
            default -> throw new IllegalStateException("unknown duplicate policy: " + joinSpec.duplicatePolicy());
        }
    }

    private void ensureJoinCapacity() {
        if (joinStates.size() < joinSpec.capacity()) {
            return;
        }
        final Iterator<Map.Entry<Object, JoinState>> iterator = joinStates.entrySet().iterator();
        if (!iterator.hasNext()) {
            return;
        }
        final Map.Entry<Object, JoinState> eldest = iterator.next();
        iterator.remove();
        metrics.recordTimedOutJoinGroup();
        releaseJoinValues(eldest.getValue());
    }

    private void expireJoinGroups(final boolean closing) {
        if (processingMode != PROCESS_JOIN || joinStates == null || joinStates.isEmpty()) {
            return;
        }
        final long timeoutNanos = joinSpec.timeout().toNanos();
        final long now = System.nanoTime();
        final Iterator<Map.Entry<Object, JoinState>> iterator = joinStates.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Object, JoinState> entry = iterator.next();
            final JoinState state = entry.getValue();
            final boolean timedOut = timeoutNanos > 0L && now - state.createdNanos >= timeoutNanos;
            if (!closing && !timedOut) {
                break;
            }
            if (state.values.size() < inputs.length) {
                metrics.recordMissingJoinBranch();
            }
            if ((timedOut || closing) && joinSpec.missingBranchPolicy() == JoinSpec.MissingBranchPolicy.EMIT_PARTIAL) {
                if (joinSpec.kind() != JoinSpec.JoinKind.ANY_OF || !state.emitted) {
                    emitJoin(entry.getKey(), state, "", timedOut, true);
                } else {
                    releaseJoinValues(state);
                }
            } else {
                releaseJoinValues(state);
            }
            if (timedOut) {
                metrics.recordTimedOutJoinGroup();
            }
            iterator.remove();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void emitJoin(
        final Object stamp,
        final JoinState state,
        final String triggeringSource,
        final boolean timedOut,
        final boolean releaseValues
    ) {
        final JoinGroup group = new JoinGroup(
            stamp,
            state.values,
            state.values.size() == inputs.length,
            timedOut,
            triggeringSource
        );
        final Object outputItem = ((JoinSpec) joinSpec).combiner().apply(group);
        try (HandleOwnership.Scope ignored = HandleOwnership.scope(state.values.values())) {
            outputSenders[0].emit(outputItem);
        }
        metrics.recordCompletedJoinGroup();
        if (releaseValues) {
            releaseJoinValues(state);
        }
    }

    private void releaseJoinState() {
        if (joinStates == null) {
            return;
        }
        for (final JoinState state : joinStates.values()) {
            releaseJoinValues(state);
        }
        joinStates.clear();
    }

    private void releaseJoinValues(final JoinState state) {
        for (final Object value : state.values.values()) {
            if (value instanceof SlabHandle<?>) {
                metrics.recordReleasedHandle();
            }
            releaseIfHandle(value);
        }
        state.values.clear();
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
        for (int i = 0; i < received; i++) {
            final Object item = batchItems[i];
            if (item != null) {
                releaseIfHandle(item);
                batchItems[i] = null;
            }
        }
    }

    private WorkerState handleException(final Throwable ex) {
        metrics.recordException();
        final StageExceptionAction action;
        try {
            action = exceptionHandler.onException(graphName, stageName, ex, context);
        } catch (final Throwable handlerFailure) {
            coordinator.fail(stageName, handlerFailure);
            return WorkerState.FAILED;
        }

        if (action == StageExceptionAction.POISON_STAGE) {
            coordinator.poison(stageName, ex, inputs, outputEdges);
            return WorkerState.POISONED;
        }

        coordinator.fail(stageName, ex);
        return WorkerState.FAILED;
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
        return policy.kind() == BatchPolicy.BatchKind.DISABLED ? 1 : Math.max(1, policy.maxItems());
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
        int total = 0;
        for (final int weight : weights) {
            total += weight;
        }
        final int[] schedule = new int[total];
        int cursor = 0;
        for (int branch = 0; branch < branchCount; branch++) {
            for (int i = 0; i < weights[branch]; i++) {
                schedule[cursor++] = branch;
            }
        }
        return schedule;
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

    private static <T> T ObjectsRequireNonNull(final T value, final String label) {
        if (value == null) {
            throw new NullPointerException(label);
        }
        return value;
    }

    private static final class JoinState {
        final long createdNanos;
        final Map<String, Object> values = new LinkedHashMap<>();
        boolean emitted;

        JoinState(final long createdNanos) {
            this.createdNanos = createdNanos;
        }
    }
}
