package io.github.elevateddev.lattice.jcstress;

import io.github.elevateddev.lattice.internal.edge.MpscRingEdge;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import io.github.elevateddev.lattice.metrics.StageMetrics;
import java.util.LinkedHashMap;
import java.util.Map;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIII_Result;

@JCStressTest
@Outcome(id = "1, 1, 0, 3", expect = Expect.ACCEPTABLE, desc = "Producers one and two filled the bounded edge.")
@Outcome(id = "1, 0, 1, 5", expect = Expect.ACCEPTABLE, desc = "Producers one and three filled the bounded edge.")
@Outcome(id = "0, 1, 1, 6", expect = Expect.ACCEPTABLE, desc = "Producers two and three filled the bounded edge.")
@Outcome(expect = Expect.FORBIDDEN, desc = "A capacity-two MPSC edge must accept exactly two concurrent producers.")
@State
public class MpscBoundedCapacityStressTest {

    private final MpscRingEdge edge = new MpscRingEdge("source", "sink", 2, edgeMetrics(), graphMetrics());

    private int firstOffer;
    private int secondOffer;
    private int thirdOffer;

    @Actor
    public void producerOne() {
        firstOffer = edge.offer(1) ? 1 : 0;
    }

    @Actor
    public void producerTwo() {
        secondOffer = edge.offer(2) ? 1 : 0;
    }

    @Actor
    public void producerThree() {
        thirdOffer = edge.offer(4) ? 1 : 0;
    }

    @Arbiter
    public void arbiter(final IIII_Result result) {
        result.r1 = firstOffer;
        result.r2 = secondOffer;
        result.r3 = thirdOffer;
        Object item;
        while ((item = edge.poll()) != null) {
            result.r4 += (Integer) item;
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
}
