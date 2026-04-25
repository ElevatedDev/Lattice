package com.lattice.benchmark;

import com.lattice.internal.jfr.JfrEvents;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class MetricsOverheadBenchmark {

    @Benchmark
    public void edgeEmitConsumeCounters(final EdgeCounterState state) {
        state.metrics.recordEmit();
        state.metrics.recordConsume();
    }

    @Benchmark
    @Group("graphSharedCounters")
    @GroupThreads(4)
    public void graphSharedCounters(final GraphCounterState state) {
        state.metrics.recordEmit();
        state.metrics.recordConsume();
    }

    @Benchmark
    public void stageBatchMetrics(final StageCounterState state) {
        state.metrics.recordConsume(32);
        state.metrics.recordBatch(32, 1_000L);
    }

    @Benchmark
    public void jfrDisabledBatchProcessed(final JfrState state) {
        JfrEvents.batchProcessed(state.graphName, state.stageName, 32, 1_000L);
    }

    @State(Scope.Thread)
    public static class EdgeCounterState {
        final EdgeMetrics metrics = new EdgeMetrics("producer", "consumer");
    }

    @State(Scope.Benchmark)
    public static class GraphCounterState {
        final GraphMetrics metrics = graphMetrics();
    }

    @State(Scope.Thread)
    public static class StageCounterState {
        final StageMetrics metrics = new StageMetrics("worker");
    }

    @State(Scope.Thread)
    public static class JfrState {
        final String graphName = "jfr-overhead";
        final String stageName = "stage";
    }

    private static GraphMetrics graphMetrics() {
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put("producer", new StageMetrics("producer"));
        stages.put("consumer", new StageMetrics("consumer"));
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put("producer->consumer", new EdgeMetrics("producer", "consumer"));
        return new GraphMetrics("metrics-overhead", stages, edges);
    }
}
