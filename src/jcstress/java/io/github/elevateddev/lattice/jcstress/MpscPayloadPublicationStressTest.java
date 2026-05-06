package io.github.elevateddev.lattice.jcstress;

import io.github.elevateddev.lattice.internal.edge.MpscRingEdge;
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
@Outcome(id = "1, 10", expect = Expect.ACCEPTABLE, desc = "Consumer observed producer one's payload.")
@Outcome(id = "2, 20", expect = Expect.ACCEPTABLE, desc = "Consumer observed producer two's payload.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Consumer must not observe a partially initialized MPSC payload.")
@State
public class MpscPayloadPublicationStressTest {

    private final MpscRingEdge edge = new MpscRingEdge("source", "sink", 2, edgeMetrics(), graphMetrics());

    @Actor
    public void producerOne() {
        edge.offer(new Payload(1, 10));
    }

    @Actor
    public void producerTwo() {
        edge.offer(new Payload(2, 20));
    }

    @Actor
    public void consumer(final II_Result result) {
        final Object value = edge.poll();
        if (value == null) {
            return;
        }
        final Payload payload = (Payload) value;
        result.r1 = payload.id;
        result.r2 = payload.value;
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

    private record Payload(int id, int value) {
    }
}
