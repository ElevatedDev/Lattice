package com.lattice.examples;

import com.lattice.edge.EdgeSpec;
import com.lattice.graph.StaticGraph;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import com.lattice.stage.Emitter;
import com.lattice.stage.StageSpec;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class MetricsDiagnosticsExample {

    private MetricsDiagnosticsExample() {
    }

    public static void main(final String[] args) throws Exception {
        final CountDownLatch consumed = new CountDownLatch(2);
        final StaticGraph graph = StaticGraph.builder("example-metrics-diagnostics")
            .source("ingress", String.class)
            .stage("parse", String.class, Integer.class, (value, out, ctx) ->
                out.push(Integer.parseInt(value)), StageSpec.singleThreaded())
            .sink("egress", Integer.class, ignored -> consumed.countDown(), StageSpec.singleThreaded())
            .edge("ingress", "parse", EdgeSpec.mpscRing(32))
            .edge("parse", "egress", EdgeSpec.spscRing(32))
            .build();

        graph.start();
        final Emitter<String> ingress = graph.emitter("ingress", String.class);
        ingress.emit("41");
        ingress.emit("42");
        ingress.close();

        await(consumed, graph, "metrics input");
        awaitTermination(graph);

        final GraphMetrics metrics = graph.metrics();
        final StageMetrics parse = metrics.stage("parse");
        final EdgeMetrics ingressToParse = metrics.edge("ingress", "parse");

        System.out.printf("graph=%s emitted=%d consumed=%d failedOffers=%d%n",
            metrics.graphName(), metrics.emittedCount(), metrics.consumedCount(), metrics.failedOffers());
        System.out.printf("stage=parse processed=%d state=%s ratePerSecond=%.2f%n",
            parse.processedMessages(), parse.workerState(), parse.processRatePerSecond());
        System.out.printf("edge=ingress->parse highWaterMark=%d blockedOffers=%d parks=%d%n",
            ingressToParse.highWaterMark(), ingressToParse.blockedOffers(), ingressToParse.parkCount());
        System.out.printf("diagnosticFlags hotCounters=%s stageHistograms=%s residenceTiming=%s%n",
            StageMetrics.hotCountersEnabled(), StageMetrics.histogramsEnabled(), EdgeMetrics.residenceTimingEnabled());
    }

    private static void await(
        final CountDownLatch latch,
        final StaticGraph graph,
        final String name
    ) throws InterruptedException {
        if (!latch.await(5, TimeUnit.SECONDS)) {
            graph.abort();
            throw new IllegalStateException("timed out waiting for " + name);
        }
    }

    private static void awaitTermination(final StaticGraph graph) throws InterruptedException {
        if (!graph.awaitTermination(Duration.ofSeconds(5))) {
            graph.abort();
            throw new IllegalStateException("graph did not stop cleanly");
        }
    }
}
