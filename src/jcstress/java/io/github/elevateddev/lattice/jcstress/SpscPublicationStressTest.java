package io.github.elevateddev.lattice.jcstress;

import io.github.elevateddev.lattice.internal.edge.SpscRingEdge;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import io.github.elevateddev.lattice.metrics.StageMetrics;
import java.util.LinkedHashMap;
import java.util.Map;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

@JCStressTest
@Outcome(id = "0", expect = Expect.ACCEPTABLE, desc = "Consumer observed the queue before publication.")
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Consumer observed the published value.")
@State
public class SpscPublicationStressTest {

    private final SpscRingEdge edge = new SpscRingEdge("source", "sink", 2, edgeMetrics(), graphMetrics());

    @Actor
    public void producer() {
        edge.offer(1);
    }

    @Actor
    public void consumer(final I_Result result) {
        final Object value = edge.poll();
        result.r1 = value == null ? 0 : (Integer) value;
    }

    private static EdgeMetrics edgeMetrics() {
        return new EdgeMetrics("source", "sink");
    }

    private static GraphMetrics graphMetrics() {
        final Map<String, StageMetrics> stages = new LinkedHashMap<>();
        stages.put("source", new StageMetrics("source"));
        stages.put("sink", new StageMetrics("sink"));
        final Map<String, EdgeMetrics> edges = new LinkedHashMap<>();
        edges.put("source->sink", new EdgeMetrics("source", "sink"));
        return new GraphMetrics("jcstress", stages, edges);
    }
}
