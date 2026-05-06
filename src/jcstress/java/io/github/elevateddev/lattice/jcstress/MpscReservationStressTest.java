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
import org.openjdk.jcstress.infra.results.III_Result;

@JCStressTest
@Outcome(id = "1, 1, 3", expect = Expect.ACCEPTABLE, desc = "Both producers reserved distinct slots and both values were consumed.")
@State
public class MpscReservationStressTest {

    private final MpscRingEdge edge = new MpscRingEdge("source", "sink", 2, edgeMetrics(), graphMetrics());

    private int firstOffer;
    private int secondOffer;

    @Actor
    public void producerOne() {
        firstOffer = edge.offer(1) ? 1 : 0;
    }

    @Actor
    public void producerTwo() {
        secondOffer = edge.offer(2) ? 1 : 0;
    }

    @Arbiter
    public void arbiter(final III_Result result) {
        result.r1 = firstOffer;
        result.r2 = secondOffer;
        int sum = 0;
        final Object first = edge.poll();
        final Object second = edge.poll();
        if (first != null) {
            sum += (Integer) first;
        }
        if (second != null) {
            sum += (Integer) second;
        }
        result.r3 = sum;
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
