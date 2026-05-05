package com.lattice;

import com.lattice.edge.EdgeSpec;
import com.lattice.edge.OverflowPolicy;
import com.lattice.graph.DiagnosticsSpec;
import com.lattice.graph.FusionSpec;
import com.lattice.graph.GraphCompilationReport;
import com.lattice.graph.GraphPlacementSpec;
import com.lattice.graph.MetricsSpec;
import com.lattice.graph.PreallocationSpec;
import com.lattice.placement.MemoryMode;
import com.lattice.placement.PinPolicy;
import com.lattice.routing.BroadcastSpec;
import com.lattice.routing.DispatchSpec;
import com.lattice.routing.JoinGroup;
import com.lattice.routing.JoinSpec;
import com.lattice.routing.PartitionSpec;
import com.lattice.routing.Stamped;
import com.lattice.stage.BatchPolicy;
import com.lattice.stage.StageSpec;
import com.lattice.wait.WaitSpec;
import java.time.Duration;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PublicApiSpecContractTest {

    @Test
    void edgeSpecsAreImmutableAndValidateTheirConfiguration() {
        final EdgeSpec base = EdgeSpec.mpscRing(8);
        final WaitSpec wait = WaitSpec.blocking();
        final OverflowPolicy overflow = OverflowPolicy.blockFor(Duration.ofMillis(2));
        final MemoryMode memory = MemoryMode.offHeapHandles(64);
        final BatchPolicy batch = BatchPolicy.maxItems(4);

        final EdgeSpec configured = base
            .wait(wait)
            .overflow(overflow)
            .memory(memory)
            .batch(batch)
            .capacity(16)
            .withKind(EdgeSpec.EdgeKind.SPSC_RING);

        assertEquals(EdgeSpec.EdgeKind.MPSC_RING, base.kind());
        assertEquals(8, base.capacity());
        assertEquals(OverflowPolicy.OverflowKind.BLOCK, base.overflowPolicy().kind());
        assertEquals(MemoryMode.MemoryKind.ON_HEAP_SLOTS, base.memoryMode().kind());
        assertEquals(BatchPolicy.BatchKind.DISABLED, base.batchPolicy().kind());

        assertEquals(EdgeSpec.EdgeKind.SPSC_RING, configured.kind());
        assertEquals(16, configured.capacity());
        assertSame(wait, configured.waitSpec());
        assertSame(overflow, configured.overflowPolicy());
        assertSame(memory, configured.memoryMode());
        assertSame(batch, configured.batchPolicy());

        assertThrows(IllegalArgumentException.class, () -> EdgeSpec.spscRing(0));
        assertThrows(IllegalArgumentException.class, () -> base.capacity(-1));
        assertThrows(NullPointerException.class, () -> base.wait(null));
        assertThrows(NullPointerException.class, () -> base.overflow(null));
        assertThrows(NullPointerException.class, () -> base.memory(null));
        assertThrows(NullPointerException.class, () -> base.batch(null));
        assertThrows(NullPointerException.class, () -> base.withKind(null));
    }

    @Test
    void overflowPoliciesExposePreciseFullEdgeBehavior() {
        assertEquals(OverflowPolicy.OverflowKind.BLOCK, OverflowPolicy.block().kind());
        assertEquals(OverflowPolicy.OverflowKind.FAIL_FAST, OverflowPolicy.failFast().kind());
        assertEquals(OverflowPolicy.OverflowKind.DROP_LATEST, OverflowPolicy.dropLatest().kind());
        assertEquals(OverflowPolicy.OverflowKind.DROP_LATEST, OverflowPolicy.dropNewest().kind());
        assertEquals(OverflowPolicy.OverflowKind.DROP_OLDEST, OverflowPolicy.dropOldest().kind());

        final OverflowPolicy timed = OverflowPolicy.blockFor(Duration.ofMillis(7));
        assertEquals(OverflowPolicy.OverflowKind.BLOCK_FOR, timed.kind());
        assertEquals(Duration.ofMillis(7), timed.timeout());

        final OverflowPolicy redirect = OverflowPolicy.redirectTo(" dlq ");
        assertEquals(OverflowPolicy.OverflowKind.REDIRECT, redirect.kind());
        assertEquals("dlq", redirect.redirectTarget());

        final OverflowPolicy coalescing = OverflowPolicy.coalesceBy((Message item) -> item.key());
        assertEquals(OverflowPolicy.OverflowKind.COALESCE, coalescing.kind());
        assertEquals(3, coalescing.coalescingKey().apply(new Message(3, "new")));

        assertNull(OverflowPolicy.block().timeout());
        assertNull(OverflowPolicy.block().coalescingKey());
        assertNull(OverflowPolicy.block().redirectTarget());
        assertThrows(IllegalArgumentException.class, () -> OverflowPolicy.blockFor(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> OverflowPolicy.blockFor(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> OverflowPolicy.redirectTo(" "));
        assertThrows(NullPointerException.class, () -> OverflowPolicy.redirectTo(null));
        assertThrows(NullPointerException.class, () -> OverflowPolicy.coalesceBy(null));
    }

    @Test
    void waitBatchAndStageSpecsPreserveDefaultsAndValidateBounds() {
        final WaitSpec defaultWait = WaitSpec.phasedDefault();
        assertEquals(WaitSpec.WaitKind.PHASED, defaultWait.kind());
        assertEquals(10_000, defaultWait.spins());
        assertEquals(50, defaultWait.yields());
        assertEquals(Duration.ofNanos(500), defaultWait.parkNanos());

        final WaitSpec phased = WaitSpec.phased(1, 2, Duration.ofNanos(3));
        assertEquals(1, phased.spins());
        assertEquals(2, phased.yields());
        assertEquals(Duration.ofNanos(3), phased.parkNanos());
        assertEquals(WaitSpec.WaitKind.BUSY_SPIN, WaitSpec.busySpin().kind());
        assertEquals(WaitSpec.WaitKind.BLOCKING, WaitSpec.blocking().kind());

        assertThrows(IllegalArgumentException.class, () -> WaitSpec.phased(-1, 0, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> WaitSpec.phased(0, -1, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> WaitSpec.phased(0, 0, Duration.ofNanos(-1)));
        assertThrows(NullPointerException.class, () -> WaitSpec.phased(0, 0, null));

        final BatchPolicy disabled = BatchPolicy.disabled();
        final BatchPolicy maxItems = BatchPolicy.maxItems(32);
        final BatchPolicy linger = BatchPolicy.linger(16, Duration.ofMillis(1));
        assertEquals(BatchPolicy.BatchKind.DISABLED, disabled.kind());
        assertEquals(0, disabled.maxItems());
        assertEquals(BatchPolicy.BatchKind.MAX_ITEMS, maxItems.kind());
        assertEquals(32, maxItems.maxItems());
        assertEquals(BatchPolicy.BatchKind.LINGER, linger.kind());
        assertEquals(16, linger.maxItems());
        assertEquals(Duration.ofMillis(1), linger.linger());
        assertEquals(64, BatchPolicy.linger(Duration.ZERO).maxItems());
        assertThrows(IllegalArgumentException.class, () -> BatchPolicy.maxItems(0));
        assertThrows(IllegalArgumentException.class, () -> BatchPolicy.linger(0, Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> BatchPolicy.linger(1, Duration.ofNanos(-1)));
        assertThrows(NullPointerException.class, () -> BatchPolicy.linger(1, null));

        final StageSpec base = StageSpec.singleThreaded();
        final StageSpec configured = base.batch(maxItems).pin(PinPolicy.cpu(1)).wait(WaitSpec.busySpin());
        assertEquals(StageSpec.StageExecution.SINGLE_THREADED, base.execution());
        assertEquals(BatchPolicy.BatchKind.DISABLED, base.batchPolicy().kind());
        assertEquals(PinPolicy.PinKind.NONE, base.pinPolicy().kind());
        assertEquals(WaitSpec.WaitKind.PHASED, base.waitSpec().kind());
        assertSame(maxItems, configured.batchPolicy());
        assertEquals(PinPolicy.PinKind.CPU, configured.pinPolicy().kind());
        assertEquals(WaitSpec.WaitKind.BUSY_SPIN, configured.waitSpec().kind());
        assertThrows(NullPointerException.class, () -> base.batch(null));
        assertThrows(NullPointerException.class, () -> base.pin(null));
        assertThrows(NullPointerException.class, () -> base.wait(null));
    }

    @Test
    void placementAndMemorySpecsDefensivelyRepresentNativeRequests() {
        final BitSet cpus = new BitSet();
        cpus.set(2);
        final PinPolicy cpuSet = PinPolicy.cpuSet(cpus);
        cpus.set(9);

        assertEquals(PinPolicy.PinKind.CPU_SET, cpuSet.kind());
        assertTrue(cpuSet.cpuSet().get(2));
        assertFalse(cpuSet.cpuSet().get(9));
        final BitSet returned = cpuSet.cpuSet();
        returned.set(11);
        assertFalse(cpuSet.cpuSet().get(11));

        assertFalse(PinPolicy.none().requiresNativePlacement());
        assertTrue(PinPolicy.cpu(0).requiresNativePlacement());
        assertEquals(3, PinPolicy.core(3).coreId());
        assertEquals(4, PinPolicy.numaNode(4).numaNode());
        assertEquals(PinPolicy.PinKind.INHERIT_CPUSET, PinPolicy.inheritCpuset().kind());
        assertThrows(IllegalArgumentException.class, () -> PinPolicy.cpu(-1));
        assertThrows(IllegalArgumentException.class, () -> PinPolicy.core(-1));
        assertThrows(IllegalArgumentException.class, () -> PinPolicy.numaNode(-1));
        assertThrows(IllegalArgumentException.class, () -> PinPolicy.cpuSet(-1));
        assertThrows(IllegalArgumentException.class, () -> PinPolicy.cpuSet(new BitSet()));
        assertThrows(NullPointerException.class, () -> PinPolicy.cpuSet((BitSet) null));

        assertEquals(MemoryMode.MemoryKind.ON_HEAP_SLOTS, MemoryMode.onHeapSlots().kind());
        assertEquals(MemoryMode.MemoryKind.OFF_HEAP_SLOTS, MemoryMode.offHeapSlots().kind());
        assertEquals(MemoryMode.MemoryKind.OFF_HEAP_HANDLES, MemoryMode.offHeapHandles().kind());
        assertEquals(128L, MemoryMode.offHeapHandles(128).bytes());
        assertThrows(IllegalArgumentException.class, () -> MemoryMode.offHeapHandles(-1));
    }

    @Test
    void graphLevelSpecsAreImmutableLocalRuntimeControls() {
        final FusionSpec fusion = FusionSpec.defaults()
            .enabled(false)
            .inlineSources(true)
            .elideInlineSourcePhysicalPath(true)
            .validateTypes(true);
        assertTrue(FusionSpec.defaults().enabled());
        assertFalse(FusionSpec.defaults().inlineSources());
        assertFalse(FusionSpec.disabled().enabled());
        assertFalse(fusion.enabled());
        assertTrue(fusion.inlineSources());
        assertTrue(fusion.elideInlineSourcePhysicalPath());
        assertTrue(fusion.validateTypes());

        final MetricsSpec metrics = MetricsSpec.off()
            .hotCounters(true)
            .residenceTiming(true)
            .stageHistograms(true)
            .fusedLogicalEdgeCounters(true);
        assertFalse(MetricsSpec.off().hotCounters());
        assertTrue(metrics.hotCounters());
        assertTrue(metrics.residenceTiming());
        assertTrue(metrics.stageHistograms());
        assertTrue(metrics.fusedLogicalEdgeCounters());

        final GraphPlacementSpec placement = GraphPlacementSpec.off()
            .topologyAware(true)
            .strict(true)
            .firstTouch(true);
        assertFalse(GraphPlacementSpec.off().topologyAware());
        assertTrue(placement.topologyAware());
        assertTrue(placement.strict());
        assertTrue(placement.firstTouch());

        final DiagnosticsSpec diagnostics = DiagnosticsSpec.off().jfr(true);
        assertFalse(DiagnosticsSpec.off().jfr());
        assertTrue(diagnostics.jfr());
    }

    @Test
    void compilationReportRowsExposeStableDecisionVocabulary() {
        final GraphCompilationReport.Controls controls =
            new GraphCompilationReport.Controls(true, false, false, false, false);
        final GraphCompilationReport.Fallback fallback = new GraphCompilationReport.Fallback(
            GraphCompilationReport.SubjectKind.EDGE,
            "a->b",
            GraphCompilationReport.Reason.NON_FUSIBLE_EDGE_OVERFLOW,
            null
        );
        final GraphCompilationReport report = new GraphCompilationReport(
            "report",
            controls,
            java.util.List.of(),
            java.util.List.of(),
            java.util.List.of(),
            java.util.List.of(),
            java.util.List.of(fallback)
        );

        assertTrue(report.controls().fusionEnabled());
        assertEquals("fusion.non_fusible_edge.overflow", fallback.reasonCode());
        assertEquals(GraphCompilationReport.Reason.NON_FUSIBLE_EDGE_OVERFLOW,
            GraphCompilationReport.Reason.fromCode(fallback.reasonCode()).orElseThrow());
        assertThrows(UnsupportedOperationException.class,
            () -> report.fallbacks().add(fallback));
    }

    @Test
    void preallocationSpecsSnapshotFixedPoolsAndValidatePoolSizing() {
        final Reusable[] pool = { new Reusable(1), new Reusable(2) };
        final PreallocationSpec<Reusable> fixed = PreallocationSpec.fixedPool(pool);
        pool[0] = new Reusable(99);

        assertTrue(fixed.fixed());
        assertNull(fixed.factory());
        assertEquals(2, fixed.requestedPoolSize());
        assertEquals(1, fixed.fixedPool()[0].id());
        final Reusable[] returned = fixed.fixedPool();
        returned[1] = new Reusable(100);
        assertEquals(2, fixed.fixedPool()[1].id());
        assertNotSame(returned, fixed.fixedPool());

        final PreallocationSpec<Reusable> factory = PreallocationSpec.pool(Reusable::new);
        assertFalse(factory.fixed());
        assertNull(factory.fixedPool());
        assertEquals(0, factory.requestedPoolSize());
        assertEquals(32, factory.poolSize(32).requestedPoolSize());
        assertEquals(64, PreallocationSpec.pool(Reusable::new, 64).requestedPoolSize());

        assertThrows(NullPointerException.class, () -> PreallocationSpec.pool(null));
        assertThrows(NullPointerException.class, () -> PreallocationSpec.fixedPool(null));
        assertThrows(IllegalArgumentException.class, () -> PreallocationSpec.fixedPool(new Reusable[0]));
        assertThrows(IllegalArgumentException.class,
            () -> PreallocationSpec.fixedPool(new Reusable[] { new Reusable(1), null }));
        assertThrows(IllegalArgumentException.class, () -> factory.poolSize(0));
        assertThrows(IllegalArgumentException.class, () -> fixed.poolSize(4));
    }

    @Test
    void routingSpecsExposeDeterministicConfigurationWithoutLeakingMutableArrays() {
        assertEquals(DispatchSpec.DispatchKind.ROUND_ROBIN, DispatchSpec.roundRobin().kind());

        final DispatchSpec<Message> keyed = DispatchSpec.keyed(Message::key);
        assertEquals(DispatchSpec.DispatchKind.KEYED, keyed.kind());
        assertEquals(7, keyed.keyExtractor().apply(new Message(7, "value")));

        final int[] weights = { 1, 3 };
        final DispatchSpec<Message> weighted = DispatchSpec.weighted(weights);
        weights[0] = 100;
        assertArrayEquals(new int[] { 1, 3 }, weighted.weights());
        final int[] returnedWeights = weighted.weights();
        returnedWeights[1] = 100;
        assertArrayEquals(new int[] { 1, 3 }, weighted.weights());
        assertEquals(4, weighted.weightSum());
        assertThrows(IllegalArgumentException.class, DispatchSpec::weighted);
        assertThrows(IllegalArgumentException.class, () -> DispatchSpec.weighted(1, 0));
        assertThrows(NullPointerException.class, () -> DispatchSpec.keyed(null));

        final BroadcastSpec<MutablePayload> copy = BroadcastSpec.copy(payload -> new MutablePayload(payload.value));
        final BroadcastSpec<MutablePayload> isolated = copy.withBranchIsolation();
        assertEquals(BroadcastSpec.BroadcastKind.COPY, copy.kind());
        assertFalse(copy.isolateSlowBranches());
        assertTrue(isolated.isolateSlowBranches());
        assertEquals(5, ((MutablePayload) copy.copier().apply(new MutablePayload(5))).value);
        assertEquals(BroadcastSpec.BroadcastKind.SLAB_HANDLE, BroadcastSpec.slabHandles().kind());
        assertThrows(NullPointerException.class, () -> BroadcastSpec.copy(null));

        final PartitionSpec<Message, Integer> partition = PartitionSpec.byKey(Message::key, 8).hotKeyThreshold(12);
        assertEquals(8, partition.lanes());
        assertEquals(12, partition.hotKeyThreshold());
        assertEquals(4, partition.keyExtractor().apply(new Message(4, "value")));
        assertThrows(IllegalArgumentException.class, () -> PartitionSpec.byKey(Message::key, 0));
        assertThrows(IllegalArgumentException.class, () -> partition.hotKeyThreshold(-1));
        assertThrows(NullPointerException.class, () -> PartitionSpec.byKey(null, 1));
    }

    @Test
    void joinSpecsAndGroupsMakeStampAndSnapshotSemanticsExplicit() {
        final JoinSpec<String> allOf = JoinSpec.allOf(group -> group.stamp().toString());
        final Stamped<String> stamped = Stamped.of(42L, "payload");
        assertEquals(JoinSpec.JoinKind.ALL_OF, allOf.kind());
        assertTrue(allOf.longStamp());
        assertEquals(42L, allOf.extractLongStamp(stamped));
        assertEquals(42L, allOf.extractStamp(stamped));
        assertEquals(42L, allOf.stampExtractor().apply(stamped));
        assertThrows(IllegalArgumentException.class, () -> allOf.extractLongStamp("not stamped"));

        final JoinSpec<String> objectStamped = allOf
            .stamp(item -> ((Message) item).key())
            .capacity(16)
            .timeout(Duration.ofMillis(1))
            .missingBranches(JoinSpec.MissingBranchPolicy.EMIT_PARTIAL)
            .duplicates(JoinSpec.DuplicatePolicy.FAIL);
        assertFalse(objectStamped.longStamp());
        assertEquals(16, objectStamped.capacity());
        assertEquals(Duration.ofMillis(1), objectStamped.timeout());
        assertEquals(JoinSpec.MissingBranchPolicy.EMIT_PARTIAL, objectStamped.missingBranchPolicy());
        assertEquals(JoinSpec.DuplicatePolicy.FAIL, objectStamped.duplicatePolicy());
        assertEquals(3, objectStamped.extractStamp(new Message(3, "value")));
        assertThrows(IllegalStateException.class, () -> objectStamped.extractLongStamp(new Message(3, "value")));
        assertThrows(IllegalArgumentException.class, () -> allOf.capacity(0));
        assertThrows(IllegalArgumentException.class, () -> allOf.timeout(Duration.ofNanos(-1)));
        assertThrows(NullPointerException.class, () -> allOf.stamp(null));
        assertThrows(NullPointerException.class, () -> allOf.stampLong(null));
        assertThrows(NullPointerException.class, () -> allOf.missingBranches(null));
        assertThrows(NullPointerException.class, () -> allOf.duplicates(null));
        assertThrows(NullPointerException.class, () -> Stamped.of(1, null));

        final Map<String, Object> constructorValues = new LinkedHashMap<>();
        constructorValues.put("left", "L");
        final JoinGroup constructorGroup = new JoinGroup("stamp", constructorValues, true, false, "left");
        constructorValues.put("right", "R");
        assertEquals(Map.of("left", "L"), constructorGroup.valuesBySource());
        assertThrows(UnsupportedOperationException.class, () -> constructorGroup.valuesBySource().put("right", "R"));
        assertEquals("L", constructorGroup.value("left", String.class).orElseThrow());
        assertTrue(constructorGroup.value("missing", String.class).isEmpty());
        assertThrows(ClassCastException.class, () -> constructorGroup.value("left", Integer.class));

        final Map<String, Object> runtimeValues = new LinkedHashMap<>();
        runtimeValues.put("left", "L1");
        final JoinGroup runtimeGroup = JoinGroup.reusableRuntimeGroup(runtimeValues);
        runtimeGroup.resetRuntime(99L, true, false, "left");
        final Map<String, Object> snapshot = runtimeGroup.snapshotValuesBySource();
        runtimeValues.put("left", "L2");
        runtimeValues.put("right", "R2");
        assertEquals(Map.of("left", "L1"), snapshot);
        assertTrue(runtimeGroup.usesLongStamp());
        assertEquals(99L, runtimeGroup.longStamp());
        assertEquals(99L, runtimeGroup.stamp());
        assertEquals("left", runtimeGroup.triggeringSource());
        assertTrue(runtimeGroup.complete());
        assertFalse(runtimeGroup.timedOut());

        runtimeGroup.resetRuntime("object-stamp", false, true, null);
        assertFalse(runtimeGroup.usesLongStamp());
        assertEquals("object-stamp", runtimeGroup.stamp());
        assertEquals("", runtimeGroup.triggeringSource());
        assertFalse(runtimeGroup.complete());
        assertTrue(runtimeGroup.timedOut());
        assertThrows(IllegalStateException.class, runtimeGroup::longStamp);
    }

    private record Message(int key, String value) {
    }

    private record Reusable(int id) {
    }

    private static final class MutablePayload {
        private final int value;

        private MutablePayload(final int value) {
            this.value = value;
        }
    }
}
