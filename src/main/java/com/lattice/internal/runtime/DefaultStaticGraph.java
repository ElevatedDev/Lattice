package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.GraphBuildException;
import com.lattice.graph.GraphPlan;
import com.lattice.graph.GraphRuntimeException;
import com.lattice.graph.GraphState;
import com.lattice.graph.PreallocationSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.internal.edge.EdgeFactory;
import com.lattice.internal.edge.MessageEdge;
import com.lattice.internal.graph.CompiledGraph;
import com.lattice.internal.graph.EdgeDefinition;
import com.lattice.internal.graph.NodeDefinition;
import com.lattice.internal.jfr.JfrEvents;
import com.lattice.internal.placement.TopologyAwarePlacement;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.placement.MemoryMode;
import com.lattice.placement.PinPolicy;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.Emitter;
import com.lattice.stage.PreallocatedEmitter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

public final class DefaultStaticGraph implements StaticGraph {

    private static final MessageEdge[] NO_EDGES = new MessageEdge[0];
    private static final SourceEmitter<?>[] NO_EMITTERS = new SourceEmitter<?>[0];
    private static final StageWorker[] NO_WORKERS = new StageWorker[0];

    private final CompiledGraph compiled;
    private final GraphMetrics metrics;
    private final Map<String, MessageEdge> edgesByKey = new LinkedHashMap<>();
    private final Map<String, SourceEmitter<?>> emitters = new LinkedHashMap<>();
    private final Map<String, PreallocatedSourceEmitter<?>> preallocatedEmitters = new LinkedHashMap<>();
    private final List<StageWorker> workers = new ArrayList<>();
    private final RuntimePlan runtimePlan;
    private final Map<String, PinPolicy> topologyAwarePins;
    private MessageEdge[] edgeArray = NO_EDGES;
    private SourceEmitter<?>[] sourceEmitterArray = NO_EMITTERS;
    private StageWorker[] workerArray = NO_WORKERS;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicReference<GraphState> state = new AtomicReference<>(GraphState.NEW);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final CountDownLatch termination;
    private final RuntimeCoordinator coordinator;

    public DefaultStaticGraph(final CompiledGraph compiled) {
        this.compiled = compiled;
        final Map<String, StageMetrics> stageMetrics = new LinkedHashMap<>();
        for (final GraphPlan.Node node : compiled.plan().nodes()) {
            stageMetrics.put(node.name(), new StageMetrics(node.name()));
        }

        final Map<String, EdgeMetrics> edgeMetrics = new LinkedHashMap<>();
        for (final EdgeDefinition edge : compiled.edges()) {
            edgeMetrics.put(edge.key(), new EdgeMetrics(
                    edge.from(),
                    edge.to(),
                    allocationOwner(edge),
                    edge.spec().memoryMode().kind()
            ));
        }

        this.runtimePlan = RuntimePlan.build(compiled);
        this.topologyAwarePins = TopologyAwarePlacement.plan(compiled, runtimePlan.workerOrder());
        this.metrics = new GraphMetrics(compiled.plan().name(), stageMetrics, edgeMetrics);
        this.termination = new CountDownLatch(runtimePlan.workerOrder().size());
        this.coordinator = new RuntimeCoordinator(compiled.plan().name(), state, failure, metrics, runtimePlan.workerOrder().size()) {
            @Override
            void workerStopped() {
                super.workerStopped();
                termination.countDown();
            }
        };

        buildEdges(edgeMetrics);
        buildEmitters(stageMetrics);
        buildWorkers(stageMetrics);
        coordinator.attach(edgeArray, workerArray, sourceEmitterArray);
    }

    @Override
    public GraphPlan plan() {
        return compiled.plan();
    }

    @Override
    public GraphMetrics metrics() {
        return metrics;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Emitter<T> emitter(final String sourceName, final Class<T> type) {
        final SourceEmitter<?> emitter = emitters.get(sourceName);
        if (emitter == null) {
            throw new GraphRuntimeException("unknown source: " + sourceName);
        }

        final NodeDefinition source = compiled.nodes().get(sourceName);
        if (source.preallocationSpec() != null) {
            throw new GraphRuntimeException("source " + sourceName
                    + " is preallocated; use preallocatedEmitter(...)");
        }
        final Class<?> exposedType = source.stampedSource() ? source.sourcePayloadType() : source.outputType();
        if (!type.isAssignableFrom(exposedType)) {
            throw new GraphRuntimeException("source " + sourceName + " emits " + exposedType.getName()
                    + ", not " + type.getName());
        }
        return (Emitter<T>) emitter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> PreallocatedEmitter<T> preallocatedEmitter(final String sourceName, final Class<T> type) {
        final PreallocatedSourceEmitter<?> emitter = preallocatedEmitters.get(sourceName);
        if (emitter == null) {
            final NodeDefinition source = compiled.nodes().get(sourceName);
            if (source == null || source.kind() != GraphPlan.NodeKind.SOURCE) {
                throw new GraphRuntimeException("unknown source: " + sourceName);
            }
            throw new GraphRuntimeException("source " + sourceName + " is not preallocated");
        }

        final NodeDefinition source = compiled.nodes().get(sourceName);
        final Class<?> exposedType = source.outputType();
        if (!type.isAssignableFrom(exposedType)) {
            throw new GraphRuntimeException("source " + sourceName + " emits " + exposedType.getName()
                    + ", not " + type.getName());
        }
        return (PreallocatedEmitter<T>) emitter;
    }

    @Override
    public void start() {
        if (!state.compareAndSet(GraphState.NEW, GraphState.STARTING)) {
            throw new GraphRuntimeException("graph can only be started once; current state is " + state.get());
        }
        started.set(true);
        metrics.markStarted();

        final StageWorker[] currentWorkers = workerArray;
        for (int i = 0; i < currentWorkers.length; i++) {
            currentWorkers[i].start();
        }
        try {
            coordinator.awaitWorkerBootstrap();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            coordinator.requestStop();
            closeSources();
            interruptWorkers();
            throw new GraphRuntimeException("interrupted while waiting for worker placement bootstrap", ex);
        }

        if (failure.get() != null) {
            coordinator.releaseWorkers();
            throw new GraphRuntimeException("graph failed during worker placement bootstrap", failure.get());
        }

        if (!state.compareAndSet(GraphState.STARTING, GraphState.RUNNING)) {
            coordinator.releaseWorkers();
            throw new GraphRuntimeException("graph startup was cancelled; current state is " + state.get());
        }
        final SourceEmitter<?>[] sources = sourceEmitterArray;
        for (int i = 0; i < sources.length; i++) {
            sources[i].markStarted();
        }
        JfrEvents.graphStarted(compiled.plan().name());
        coordinator.releaseWorkers();
    }

    @Override
    public void stop() {
        requestDrain();
        if (!started.get()) {
            return;
        }
        try {
            awaitTermination();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            abort();
        }
    }

    @Override
    public boolean stop(final Duration timeout) {
        requestDrain();
        if (!started.get()) {
            return true;
        }
        try {
            return awaitTermination(timeout);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            abort();
            return false;
        }
    }

    @Override
    public boolean quiesce(final Duration timeout) {
        final GraphState current = state.get();
        if (current == GraphState.STOPPED || current == GraphState.FAILED) {
            return true;
        }
        if (current == GraphState.NEW) {
            return state.compareAndSet(GraphState.NEW, GraphState.STOPPED);
        }
        if (current != GraphState.QUIESCING && !state.compareAndSet(GraphState.RUNNING, GraphState.QUIESCING)) {
            throw new GraphRuntimeException("graph cannot quiesce from state " + state.get());
        }

        final long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (allWorkQuiesced()) {
                return true;
            }
            Thread.onSpinWait();
        }
        return allWorkQuiesced();
    }

    @Override
    public void resume() {
        if (!state.compareAndSet(GraphState.QUIESCING, GraphState.RUNNING)) {
            throw new GraphRuntimeException("graph can only resume from QUIESCING; current state is " + state.get());
        }
    }

    @Override
    public void abort() {
        final GraphState current = state.get();
        if (current == GraphState.STOPPED || current == GraphState.FAILED) {
            return;
        }
        if (!started.get()) {
            state.set(GraphState.STOPPED);
            metrics.markStopped();
            return;
        }
        coordinator.requestStop();
        closeSources();
        final MessageEdge[] edges = edgeArray;
        for (int i = 0; i < edges.length; i++) {
            edges[i].abort();
        }
        interruptWorkers();
        try {
            final boolean terminated = awaitTermination(Duration.ofSeconds(5));
            if (!terminated) {
                return;
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            return;
        }
        if (failure.get() == null) {
            state.compareAndSet(GraphState.STOPPING, GraphState.STOPPED);
            state.compareAndSet(GraphState.DRAINING, GraphState.STOPPED);
            metrics.markStopped();
        }
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        termination.await();
    }

    @Override
    public boolean awaitTermination(final Duration timeout) throws InterruptedException {
        return termination.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public GraphState state() {
        return state.get();
    }

    @Override
    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure.get());
    }

    private void requestDrain() {
        while (true) {
            final GraphState current = state.get();
            if (current == GraphState.NEW) {
                if (state.compareAndSet(GraphState.NEW, GraphState.STOPPED)) {
                    metrics.markStopped();
                    return;
                }
                continue;
            }
            if (current == GraphState.STOPPED || current == GraphState.FAILED || current == GraphState.DRAINING) {
                return;
            }
            if (current == GraphState.STARTING) {
                if (state.compareAndSet(GraphState.STARTING, GraphState.DRAINING)) {
                    closeSources();
                    coordinator.releaseWorkers();
                    return;
                }
                continue;
            }
            if (state.compareAndSet(current, GraphState.DRAINING)) {
                closeSources();
                coordinator.releaseWorkers();
                return;
            }
        }
    }

    private void closeSources() {
        final SourceEmitter<?>[] sources = sourceEmitterArray;
        for (int i = 0; i < sources.length; i++) {
            sources[i].close();
        }
    }

    private void interruptWorkers() {
        final StageWorker[] currentWorkers = workerArray;
        for (int i = 0; i < currentWorkers.length; i++) {
            currentWorkers[i].interrupt();
        }
    }

    private void buildEdges(final Map<String, EdgeMetrics> edgeMetrics) {
        for (final EdgeDefinition definition : compiled.edges()) {
            final EdgeMetrics metricsForEdge = edgeMetrics.get(definition.key());
            edgesByKey.put(definition.key(), EdgeFactory.create(definition, metricsForEdge, metrics));
        }
        edgeArray = edgesByKey.values().toArray(MessageEdge[]::new);
    }

    private void buildEmitters(final Map<String, StageMetrics> stageMetrics) {
        for (final NodeDefinition node : compiled.nodes().values()) {
            if (node.kind() != GraphPlan.NodeKind.SOURCE) {
                continue;
            }
            final List<EdgeDefinition> outgoingEdges = compiled.normalOutgoingBySource().get(node.name());
            final EdgeDefinition outgoing = outgoingEdges.get(0);
            final MessageEdge edge = edgesByKey.get(outgoing.key());
            final EdgeSender sender = new EdgeSender(
                    node.name(),
                    node.outputType(),
                    edge,
                    outgoing.spec(),
                    stageMetrics.get(node.name()),
                    coordinator
            );
            wireRedirect(node.name(), sender, outgoing, stageMetrics.get(node.name()));
            final SourceEmitter<?> emitter = new SourceEmitter<>(
                    node.name(),
                    sender,
                    stageMetrics.get(node.name()),
                    state,
                    node.stampedSource(),
                    node.sourceMode()
            );
            emitters.put(node.name(), emitter);
            if (node.preallocationSpec() != null) {
                preallocatedEmitters.put(node.name(), buildPreallocatedEmitter(node, emitter));
            }
        }
        sourceEmitterArray = emitters.values().toArray(new SourceEmitter<?>[0]);
    }

    @SuppressWarnings("unchecked")
    private <T> PreallocatedSourceEmitter<T> buildPreallocatedEmitter(
            final NodeDefinition node,
            final SourceEmitter<?> sourceEmitter
    ) {
        final Class<T> type = (Class<T>) node.outputType();
        final PreallocationSpec<T> spec = (PreallocationSpec<T>) node.preallocationSpec();
        final int reuseBound = preallocationReuseBound(node.name());
        final Object[] pool = preallocationPool(node.name(), type, spec, reuseBound);
        return new PreallocatedSourceEmitter<>((SourceEmitter<T>) sourceEmitter, pool, reuseBound);
    }

    private <T> Object[] preallocationPool(
            final String sourceName,
            final Class<T> type,
            final PreallocationSpec<T> spec,
            final int reuseBound
    ) {
        if (spec.fixed()) {
            final T[] pool = spec.fixedPool();
            validatePreallocationPoolSize(sourceName, pool.length, reuseBound);
            for (int i = 0; i < pool.length; i++) {
                if (!type.isInstance(pool[i])) {
                    throw new GraphBuildException("preallocated pool item " + i + " for source " + sourceName
                            + " is " + pool[i].getClass().getName() + ", expected " + type.getName());
                }
            }
            return pool;
        }

        final int requestedPoolSize = spec.requestedPoolSize();
        final int poolSize = requestedPoolSize == 0 ? nextPowerOfTwo(reuseBound + 1) : requestedPoolSize;
        validatePreallocationPoolSize(sourceName, poolSize, reuseBound);

        final IntFunction<? extends T> factory = spec.factory();
        final Object[] pool = new Object[poolSize];
        for (int i = 0; i < pool.length; i++) {
            final T item = factory.apply(i);
            if (item == null) {
                throw new GraphBuildException("preallocated pool item " + i + " for source " + sourceName
                        + " must not be null");
            }
            if (!type.isInstance(item)) {
                throw new GraphBuildException("preallocated pool item " + i + " for source " + sourceName
                        + " is " + item.getClass().getName() + ", expected " + type.getName());
            }
            pool[i] = item;
        }
        return pool;
    }

    private int preallocationReuseBound(final String sourceName) {
        long bound = 1L;
        String currentName = sourceName;
        while (true) {
            final List<EdgeDefinition> outgoing = compiled.normalOutgoingBySource()
                    .getOrDefault(currentName, List.of());
            if (outgoing.isEmpty()) {
                break;
            }
            final EdgeDefinition edge = outgoing.get(0);
            if (!runtimePlan.elidedEdgeKeys().contains(edge.key())) {
                bound += edge.spec().capacity();
            }
            final NodeDefinition target = compiled.nodes().get(edge.to());
            if (runtimePlan.workerOrder().contains(target.name())) {
                bound += StageWorker.defaultSingleMessageBatchSize();
            }
            if (bound > Integer.MAX_VALUE) {
                throw new GraphBuildException("preallocated source " + sourceName
                        + " reuse bound exceeds supported pool size");
            }
            if (target.kind() == GraphPlan.NodeKind.SINK) {
                break;
            }
            currentName = target.name();
        }
        return (int) bound;
    }

    private static void validatePreallocationPoolSize(
            final String sourceName,
            final int poolSize,
            final int reuseBound
    ) {
        if (Integer.bitCount(poolSize) != 1) {
            throw new GraphBuildException("preallocated pool size for source " + sourceName
                    + " must be a power of two: " + poolSize);
        }
        if (poolSize <= reuseBound) {
            throw new GraphBuildException("preallocated pool size for source " + sourceName
                    + " must be greater than reuse bound " + reuseBound + ": " + poolSize);
        }
    }

    private static int nextPowerOfTwo(final int value) {
        if (value <= 1) {
            return 1;
        }
        if (value > (1 << 30)) {
            throw new GraphBuildException("preallocated pool size exceeds maximum supported capacity: " + value);
        }
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(value - 1));
    }

    private void buildWorkers(final Map<String, StageMetrics> stageMetrics) {
        for (final String workerName : runtimePlan.workerOrder()) {
            final NodeDefinition node = compiled.nodes().get(workerName);
            final String nodeName = node.name();
            final StageMetrics nodeMetrics = stageMetrics.get(nodeName);
            final FusedSinkPlan fusedSinkPlan = runtimePlan.fusedSink(workerName);
            final FusedStagePlan fusedStagePlan = runtimePlan.fusedStage(workerName);
            final List<EdgeDefinition> incomingDefinitions = compiled.incomingByTarget()
                    .getOrDefault(workerName, List.of());
            final MessageEdge[] inputs = edges(incomingDefinitions);
            final EdgeSpec[] inputSpecs = specs(incomingDefinitions);
            final String[] inputSources = incomingDefinitions.stream().map(EdgeDefinition::from).toArray(String[]::new);

            final List<EdgeDefinition> outgoingDefinitions = compiled.outgoingBySource()
                    .getOrDefault(workerName, List.of());
            final List<EdgeDefinition> runtimeOutgoingDefinitions = runtimeOutgoingDefinitions(
                    outgoingDefinitions,
                    fusedStagePlan
            );
            final List<EdgeDefinition> normalOutgoingDefinitions = compiled.normalOutgoingBySource()
                    .getOrDefault(workerName, List.of());
            final MessageEdge[] outputEdges = edges(runtimeOutgoingDefinitions);
            final List<EdgeDefinition> senderDefinitions = fusedSinkPlan == null && fusedStagePlan == null
                    ? normalOutgoingDefinitions
                    : List.of();
            final EdgeSender[] outputSenders = senders(node, senderDefinitions, nodeMetrics);
            final EdgeOutput<Object> output = outputSenders.length == 0 ? null : new EdgeOutput<>(outputSenders[0]);
            final StageWorker.FusedSink fusedSink = fusedSinkPlan == null
                    ? null
                    : fusedSink(fusedSinkPlan, nodeMetrics, stageMetrics);
            final StageWorker.FusedStage fusedStage = fusedStagePlan == null
                    ? null
                    : fusedStage(fusedStagePlan, nodeMetrics, stageMetrics);
            PinPolicy effectivePinPolicy = fusedStagePlan != null
                    ? fusedStagePlan.effectivePinPolicy()
                    : fusedSinkPlan == null ? node.spec().pinPolicy() : fusedSinkPlan.effectivePinPolicy();
            if (effectivePinPolicy.kind() == PinPolicy.PinKind.NONE) {
                effectivePinPolicy = topologyAwarePins.getOrDefault(nodeName, effectivePinPolicy);
            }
            workers.add(new StageWorker(
                    node,
                    inputs,
                    inputSpecs,
                    inputSources,
                    outputEdges,
                    outputSenders,
                    ownedEdges(
                            nodeName,
                            incomingDefinitions,
                            runtimeOutgoingDefinitions,
                            runtimePlan.elidedEdgeKeys(),
                            fusedStagePlan
                    ),
                    output,
                    fusedSink,
                    fusedStage,
                    effectivePinPolicy,
                    nodeMetrics,
                    coordinator,
                    compiled.exceptionHandler()
            ));
        }
        workerArray = workers.toArray(StageWorker[]::new);
    }

    private MessageEdge[] edges(final List<EdgeDefinition> definitions) {
        final MessageEdge[] edges = new MessageEdge[definitions.size()];
        for (int i = 0; i < definitions.size(); i++) {
            edges[i] = edgesByKey.get(definitions.get(i).key());
        }
        return edges;
    }

    private EdgeSpec[] specs(final List<EdgeDefinition> definitions) {
        final EdgeSpec[] specs = new EdgeSpec[definitions.size()];
        for (int i = 0; i < definitions.size(); i++) {
            specs[i] = definitions.get(i).spec();
        }
        return specs;
    }

    private EdgeSender[] senders(
            final NodeDefinition node,
            final List<EdgeDefinition> outgoingDefinitions,
            final StageMetrics nodeMetrics
    ) {
        final EdgeSender[] senders = new EdgeSender[outgoingDefinitions.size()];
        for (int i = 0; i < outgoingDefinitions.size(); i++) {
            final EdgeDefinition outgoing = outgoingDefinitions.get(i);
            final EdgeSender sender = new EdgeSender(
                    node.name(),
                    node.outputType(),
                    edgesByKey.get(outgoing.key()),
                    outgoing.spec(),
                    nodeMetrics,
                    coordinator
            );
            wireRedirect(node.name(), sender, outgoing, nodeMetrics);
            senders[i] = sender;
        }
        return senders;
    }

    private List<EdgeDefinition> runtimeOutgoingDefinitions(
            final List<EdgeDefinition> outgoingDefinitions,
            final FusedStagePlan fusedStagePlan
    ) {
        if (fusedStagePlan == null) {
            return outgoingDefinitions;
        }
        final List<EdgeDefinition> definitions = new ArrayList<>(
                outgoingDefinitions.size() + fusedStagePlan.downstreamOutgoing().size()
        );
        definitions.addAll(outgoingDefinitions);
        definitions.addAll(fusedStagePlan.downstreamOutgoing());
        return definitions;
    }

    private StageWorker.FusedSink fusedSink(
            final FusedSinkPlan fusedSinkPlan,
            final StageMetrics ownerMetrics,
            final Map<String, StageMetrics> stageMetrics
    ) {
        final NodeDefinition sink = compiled.nodes().get(fusedSinkPlan.sinkName());
        final EdgeDefinition edge = fusedSinkPlan.edge();
        return new StageWorker.FusedSink(
                sink.name(),
                sink.inputType(),
                sink.sink(),
                stageMetrics.get(sink.name()),
                ownerMetrics,
                edgesByKey.get(edge.key()).metrics(),
                coordinator.metrics(),
                coordinator
        );
    }

    private StageWorker.FusedStage fusedStage(
            final FusedStagePlan fusedStagePlan,
            final StageMetrics ownerMetrics,
            final Map<String, StageMetrics> stageMetrics
    ) {
        return fusedStage(fusedStagePlan, 0, ownerMetrics, stageMetrics);
    }

    private StageWorker.FusedStage fusedStage(
            final FusedStagePlan fusedStagePlan,
            final int stageIndex,
            final StageMetrics ownerMetrics,
            final Map<String, StageMetrics> stageMetrics
    ) {
        final NodeDefinition stage = compiled.nodes().get(fusedStagePlan.stageNames().get(stageIndex));
        final EdgeDefinition edge = fusedStagePlan.stageInputEdges().get(stageIndex);
        final StageMetrics currentMetrics = stageMetrics.get(stage.name());
        final boolean terminalStage = stageIndex == fusedStagePlan.stageNames().size() - 1;
        final StageWorker.FusedStage nextStage = terminalStage
                ? null
                : fusedStage(fusedStagePlan, stageIndex + 1, currentMetrics, stageMetrics);
        final StageWorker.FusedSink terminalSink = fusedStagePlan.sinkPlan() == null
                ? null
                : terminalStage ? fusedSink(fusedStagePlan.sinkPlan(), currentMetrics, stageMetrics) : null;
        final EdgeOutput<Object> downstreamOutput;
        if (nextStage == null && terminalSink == null) {
            final List<EdgeDefinition> normalOutgoing = compiled.normalOutgoingBySource()
                    .getOrDefault(stage.name(), List.of());
            final EdgeSender[] senders = senders(stage, normalOutgoing, stageMetrics.get(stage.name()));
            downstreamOutput = senders.length == 0 ? null : new EdgeOutput<>(senders[0]);
        } else {
            downstreamOutput = null;
        }
        return new StageWorker.FusedStage(
                stage.name(),
                stage.inputType(),
                stage.logic(),
                downstreamOutput,
                currentMetrics,
                ownerMetrics,
                edgesByKey.get(edge.key()).metrics(),
                coordinator.metrics(),
                coordinator,
                nextStage,
                terminalSink
        );
    }

    private void wireRedirect(
            final String sourceName,
            final EdgeSender sender,
            final EdgeDefinition primary,
            final StageMetrics metrics
    ) {
        final String redirectTarget = primary.spec().overflowPolicy().redirectTarget();
        if (redirectTarget == null) {
            return;
        }
        final EdgeDefinition redirect = compiled.redirectBySourceAndTarget()
                .getOrDefault(sourceName, Map.of())
                .get(redirectTarget);
        if (redirect == null) {
            return;
        }
        sender.redirectSender(new EdgeSender(
                sourceName,
                primary.messageType(),
                edgesByKey.get(redirect.key()),
                redirect.spec(),
                metrics,
                coordinator
        ));
    }

    private MessageEdge[] ownedEdges(
            final String nodeName,
            final List<EdgeDefinition> incomingDefinitions,
            final List<EdgeDefinition> outgoingDefinitions,
            final Set<String> elidedEdgeKeys,
            final FusedStagePlan fusedStagePlan
    ) {
        final List<MessageEdge> owned = new ArrayList<>(incomingDefinitions.size() + outgoingDefinitions.size());
        for (final EdgeDefinition incoming : incomingDefinitions) {
            if (elidedEdgeKeys.contains(incoming.key())) {
                continue;
            }
            if (nodeName.equals(allocationOwner(incoming))) {
                owned.add(edgesByKey.get(incoming.key()));
            }
        }
        for (final EdgeDefinition outgoing : outgoingDefinitions) {
            if (elidedEdgeKeys.contains(outgoing.key())) {
                continue;
            }
            if (nodeName.equals(allocationOwner(outgoing))) {
                owned.add(edgesByKey.get(outgoing.key()));
            }
        }
        if (fusedStagePlan != null) {
            for (final EdgeDefinition downstreamOutgoing : fusedStagePlan.downstreamOutgoing()) {
                if (elidedEdgeKeys.contains(downstreamOutgoing.key())) {
                    continue;
                }
                if (fusedStagePlan.stageNames().contains(allocationOwner(downstreamOutgoing))) {
                    owned.add(edgesByKey.get(downstreamOutgoing.key()));
                }
            }
        }
        return owned.toArray(MessageEdge[]::new);
    }

    private String allocationOwner(final EdgeDefinition edge) {
        final NodeDefinition from = compiled.nodes().get(edge.from());
        return from.kind() == GraphPlan.NodeKind.SOURCE ? edge.to() : edge.from();
    }

    private boolean allEdgesEmpty() {
        final MessageEdge[] edges = edgeArray;
        for (int i = 0; i < edges.length; i++) {
            if (!edges[i].isEmpty() || edges[i].inFlight() > 0) {
                return false;
            }
        }
        return true;
    }

    private boolean allWorkQuiesced() {
        return allEdgesEmpty() && !coordinator.hasInFlightWork();
    }

    private record FusedSinkPlan(
            String stageName,
            String sinkName,
            EdgeDefinition edge,
            PinPolicy effectivePinPolicy
    ) {
    }

    private record FusedStagePlan(
            String ownerName,
            List<String> stageNames,
            List<EdgeDefinition> stageInputEdges,
            List<EdgeDefinition> elidedEdges,
            List<EdgeDefinition> downstreamOutgoing,
            FusedSinkPlan sinkPlan,
            PinPolicy effectivePinPolicy
    ) {
        String firstStageName() {
            return stageNames.get(0);
        }

        EdgeDefinition firstEdge() {
            return stageInputEdges.get(0);
        }
    }

    private record RuntimePlan(
            List<String> workerOrder,
            Map<String, FusedSinkPlan> fusedSinks,
            Map<String, FusedStagePlan> fusedStages,
            Set<String> elidedEdgeKeys
    ) {
        private static final String FUSION_ENABLED_PROPERTY = "lattice.fusion.enabled";

        static RuntimePlan build(final CompiledGraph compiled) {
            if (!Boolean.parseBoolean(System.getProperty(FUSION_ENABLED_PROPERTY, "true"))) {
                return new RuntimePlan(compiled.workerOrder(), Map.of(), Map.of(), Set.of());
            }

            final Map<String, FusedSinkPlan> fusedSinks = new LinkedHashMap<>();
            final Map<String, FusedStagePlan> fusedStages = new LinkedHashMap<>();
            final Set<String> skippedWorkers = new HashSet<>();
            final Set<String> elidedEdges = new HashSet<>();
            for (final String workerName : compiled.workerOrder()) {
                if (skippedWorkers.contains(workerName)) {
                    continue;
                }
                final FusedStagePlan stagePlan = fusedStagePlan(compiled, workerName);
                if (stagePlan != null) {
                    fusedStages.put(workerName, stagePlan);
                    skippedWorkers.addAll(stagePlan.stageNames());
                    for (final EdgeDefinition edge : stagePlan.elidedEdges()) {
                        elidedEdges.add(edge.key());
                    }
                    if (stagePlan.sinkPlan() != null) {
                        skippedWorkers.add(stagePlan.sinkPlan().sinkName());
                        elidedEdges.add(stagePlan.sinkPlan().edge().key());
                    }
                    continue;
                }
                final FusedSinkPlan plan = fusedSinkPlan(compiled, workerName);
                if (plan == null) {
                    continue;
                }
                fusedSinks.put(workerName, plan);
                skippedWorkers.add(plan.sinkName());
                elidedEdges.add(plan.edge().key());
            }

            if (fusedSinks.isEmpty() && fusedStages.isEmpty()) {
                return new RuntimePlan(compiled.workerOrder(), Map.of(), Map.of(), Set.of());
            }

            final List<String> actualWorkers = compiled.workerOrder().stream()
                    .filter(worker -> !skippedWorkers.contains(worker))
                    .toList();
            return new RuntimePlan(
                    actualWorkers,
                    Map.copyOf(fusedSinks),
                    Map.copyOf(fusedStages),
                    Set.copyOf(elidedEdges)
            );
        }

        FusedSinkPlan fusedSink(final String workerName) {
            return fusedSinks.get(workerName);
        }

        FusedStagePlan fusedStage(final String workerName) {
            return fusedStages.get(workerName);
        }

        private static FusedSinkPlan fusedSinkPlan(final CompiledGraph compiled, final String workerName) {
            final NodeDefinition stage = compiled.nodes().get(workerName);
            if (stage == null
                    || stage.kind() != GraphPlan.NodeKind.STAGE
                    || stage.batchLogic() != null
                    || stage.spec().batchPolicy().kind() != BatchPolicy.BatchKind.DISABLED) {
                return null;
            }

            final List<EdgeDefinition> normalOutgoing = compiled.normalOutgoingBySource()
                    .getOrDefault(workerName, List.of());
            final List<EdgeDefinition> allOutgoing = compiled.outgoingBySource()
                    .getOrDefault(workerName, List.of());
            if (normalOutgoing.size() != 1 || allOutgoing.size() != 1) {
                return null;
            }

            final EdgeDefinition edge = normalOutgoing.get(0);
            if (!fusibleEdge(edge.spec())) {
                return null;
            }

            final NodeDefinition sink = compiled.nodes().get(edge.to());
            if (sink == null || sink.kind() != GraphPlan.NodeKind.SINK) {
                return null;
            }

            final PinPolicy effectivePinPolicy = effectivePinPolicy(stage.spec().pinPolicy(), sink.spec().pinPolicy());
            if (effectivePinPolicy == null) {
                return null;
            }

            return new FusedSinkPlan(workerName, sink.name(), edge, effectivePinPolicy);
        }

        private static PinPolicy effectivePinPolicy(final PinPolicy owner, final PinPolicy fused) {
            final boolean ownerExplicit = owner.kind() != PinPolicy.PinKind.NONE;
            final boolean fusedExplicit = fused.kind() != PinPolicy.PinKind.NONE;
            if (ownerExplicit && fusedExplicit) {
                return null;
            }
            return ownerExplicit ? owner : fused;
        }

        private static FusedStagePlan fusedStagePlan(final CompiledGraph compiled, final String workerName) {
            final NodeDefinition stage = compiled.nodes().get(workerName);
            if (!fusibleStage(stage)) {
                return null;
            }

            final List<String> stageNames = new ArrayList<>();
            final List<EdgeDefinition> stageInputEdges = new ArrayList<>();
            final List<EdgeDefinition> elidedEdges = new ArrayList<>();
            final List<EdgeDefinition> downstreamOutgoing = new ArrayList<>();
            String currentName = workerName;
            PinPolicy effectivePinPolicy = stage.spec().pinPolicy();
            FusedSinkPlan sinkPlan = null;

            while (true) {
                final List<EdgeDefinition> normalOutgoing = compiled.normalOutgoingBySource()
                        .getOrDefault(currentName, List.of());
                final List<EdgeDefinition> allOutgoing = compiled.outgoingBySource()
                        .getOrDefault(currentName, List.of());
                if (normalOutgoing.size() != 1 || allOutgoing.size() != 1) {
                    break;
                }

                final EdgeDefinition edge = normalOutgoing.get(0);
                if (!fusibleEdge(edge.spec())) {
                    break;
                }

                final NodeDefinition downstream = compiled.nodes().get(edge.to());
                if (fusibleStage(downstream) && downstream.spec().pinPolicy().kind() == PinPolicy.PinKind.NONE) {
                    stageNames.add(downstream.name());
                    stageInputEdges.add(edge);
                    elidedEdges.add(edge);
                    downstreamOutgoing.addAll(compiled.outgoingBySource().getOrDefault(downstream.name(), List.of()));
                    currentName = downstream.name();
                    continue;
                }

                if (downstream != null && downstream.kind() == GraphPlan.NodeKind.SINK && !stageNames.isEmpty()) {
                    final FusedSinkPlan candidate = fusedSinkPlan(compiled, currentName);
                    if (candidate != null) {
                        final PinPolicy combinedPinPolicy = effectivePinPolicy(
                                effectivePinPolicy,
                                candidate.effectivePinPolicy()
                        );
                        if (combinedPinPolicy != null) {
                            effectivePinPolicy = combinedPinPolicy;
                            sinkPlan = candidate;
                            elidedEdges.add(candidate.edge());
                        }
                    }
                }
                break;
            }

            if (stageNames.isEmpty()) {
                return null;
            }
            return new FusedStagePlan(
                    workerName,
                    List.copyOf(stageNames),
                    List.copyOf(stageInputEdges),
                    List.copyOf(elidedEdges),
                    List.copyOf(downstreamOutgoing),
                    sinkPlan,
                    effectivePinPolicy
            );
        }

        private static boolean fusibleStage(final NodeDefinition stage) {
            return stage != null
                    && stage.kind() == GraphPlan.NodeKind.STAGE
                    && stage.batchLogic() == null
                    && stage.spec().batchPolicy().kind() == BatchPolicy.BatchKind.DISABLED;
        }

        private static boolean fusibleEdge(final EdgeSpec spec) {
            return spec.kind() == EdgeSpec.EdgeKind.SPSC_RING
                    && spec.overflowPolicy().kind() == OverflowPolicy.OverflowKind.BLOCK
                    && spec.memoryMode().kind() == MemoryMode.MemoryKind.ON_HEAP_SLOTS
                    && spec.batchPolicy().kind() == BatchPolicy.BatchKind.DISABLED;
        }
    }
}
