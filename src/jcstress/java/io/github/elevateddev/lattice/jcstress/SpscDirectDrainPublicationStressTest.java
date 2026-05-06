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
import org.openjdk.jcstress.infra.results.II_Result;

@JCStressTest
@Outcome(id = "0, 0", expect = Expect.ACCEPTABLE, desc = "Consumer observed the queue before publication.")
@Outcome(id = "1, 2", expect = Expect.ACCEPTABLE, desc = "Direct drain observed the fully published payload.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Direct drain must not observe a partially initialized payload.")
@State
public class SpscDirectDrainPublicationStressTest {

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
        try {
            edge.drainToProcessor(item -> {
                final Payload payload = (Payload) item;
                result.r1 = payload.left;
                result.r2 = payload.right;
            }, 1);
        } catch (final Exception ex) {
            result.r1 = -1;
            result.r2 = -1;
        }
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
