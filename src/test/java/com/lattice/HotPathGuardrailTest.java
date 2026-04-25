package com.lattice;

import com.lattice.internal.edge.MpscRingEdge;
import com.lattice.internal.edge.SpscRingEdge;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.placement.MemoryMode;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HotPathGuardrailTest {

    @Test
    void spscOfferPollSteadyStateDoesNotAllocate() {
        final com.sun.management.ThreadMXBean bean = allocationBean();
        if (bean == null) {
            return;
        }
        final SpscRingEdge edge = new SpscRingEdge("producer", "consumer", 1024, edgeMetrics(), graphMetrics());
        final Object payload = new Object();

        for (int i = 0; i < 100_000; i++) {
            edge.offer(payload);
            edge.poll();
        }

        final long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
        for (int i = 0; i < 250_000; i++) {
            edge.offer(payload);
            edge.poll();
        }
        final long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;
        assertTrue(allocated <= 1024, "steady-state SPSC hot path allocated " + allocated + " bytes");
    }

    @Test
    void mpscOfferPollSteadyStateDoesNotAllocateOnHeapSlots() {
        assertMpscOfferPollSteadyStateDoesNotAllocate(MemoryMode.onHeapSlots());
    }

    @Test
    void mpscOfferPollSteadyStateDoesNotAllocateOffHeapHandles() {
        assertMpscOfferPollSteadyStateDoesNotAllocate(MemoryMode.offHeapHandles());
    }

    @Test
    void hotPathPackagesDoNotUseReflectionOrLogging() throws Exception {
        final Path root = Path.of("src", "main", "java", "com", "lattice", "internal");
        try (Stream<Path> files = Files.walk(root)) {
            final String joined = files
                .filter(path -> path.toString().endsWith(".java"))
                .filter(path -> !path.toString().contains("jfr"))
                .map(HotPathGuardrailTest::read)
                .reduce("", String::concat);

            assertFalse(joined.contains("java.lang.reflect"));
            assertFalse(joined.contains("Logger"));
            assertFalse(joined.contains("System.out"));
        }
    }

    private static void assertMpscOfferPollSteadyStateDoesNotAllocate(final MemoryMode memoryMode) {
        final com.sun.management.ThreadMXBean bean = allocationBean();
        if (bean == null) {
            return;
        }
        final MpscRingEdge edge = new MpscRingEdge(
            "producer",
            "consumer",
            1024,
            memoryMode,
            edgeMetrics(),
            graphMetrics()
        );
        edge.firstTouch("consumer");
        final Object payload = new Object();

        for (int i = 0; i < 100_000; i++) {
            if (!edge.offer(payload) || edge.poll() != payload) {
                throw new AssertionError("MPSC warmup failed for " + memoryMode.kind());
            }
        }

        final long before = bean.getThreadAllocatedBytes(Thread.currentThread().threadId());
        for (int i = 0; i < 250_000; i++) {
            if (!edge.offer(payload) || edge.poll() != payload) {
                throw new AssertionError("MPSC steady-state failed for " + memoryMode.kind());
            }
        }
        final long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().threadId()) - before;
        assertTrue(allocated <= 1024,
            "steady-state MPSC hot path allocated " + allocated + " bytes for " + memoryMode.kind());
    }

    private static String read(final Path path) {
        try {
            return Files.readString(path);
        } catch (final Exception ex) {
            throw new AssertionError(ex);
        }
    }

    private static com.sun.management.ThreadMXBean allocationBean() {
        final java.lang.management.ThreadMXBean platformBean = ManagementFactory.getThreadMXBean();
        if (!(platformBean instanceof com.sun.management.ThreadMXBean bean) || !bean.isThreadAllocatedMemorySupported()) {
            return null;
        }
        bean.setThreadAllocatedMemoryEnabled(true);
        return bean;
    }

    private static EdgeMetrics edgeMetrics() {
        return new EdgeMetrics("producer", "consumer");
    }

    private static GraphMetrics graphMetrics() {
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put("producer", new StageMetrics("producer"));
        stages.put("consumer", new StageMetrics("consumer"));
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put("producer->consumer", new EdgeMetrics("producer", "consumer"));
        return new GraphMetrics("guardrail", stages, edges);
    }
}
