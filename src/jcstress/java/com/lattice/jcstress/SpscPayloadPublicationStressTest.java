package com.lattice.jcstress;

import com.lattice.internal.edge.SpscRingEdge;
import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.metrics.StageMetrics;
import java.util.LinkedHashMap;
import java.util.Map;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

@JCStressTest
@Outcome(id = "0, 0", expect = Expect.ACCEPTABLE, desc = "Consumer observed the queue before publication.")
@Outcome(id = "1, 2", expect = Expect.ACCEPTABLE, desc = "Consumer observed the fully published payload.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Consumer must not observe a partially initialized payload.")
@State
public class SpscPayloadPublicationStressTest {

    private final SpscRingEdge edge = new SpscRingEdge("source", "sink", 2, edgeMetrics(), graphMetrics());

    @Actor
    public void producer() {
        final Payload payload = new Payload();
        payload.left = 1;
        payload.right = 2;
        edge.offer(payload);
    }

    @Actor
    public void consumer(final II_Result result) {
        final Object value = edge.poll();
        if (value == null) {
            return;
        }
        final Payload payload = (Payload) value;
        result.r1 = payload.left;
        result.r2 = payload.right;
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

    public static final class Payload {
        int left;
        int right;
    }
}
