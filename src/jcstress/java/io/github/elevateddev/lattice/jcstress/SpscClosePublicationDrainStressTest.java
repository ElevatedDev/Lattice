package io.github.elevateddev.lattice.jcstress;

import io.github.elevateddev.lattice.internal.edge.SpscRingEdge;
import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import io.github.elevateddev.lattice.metrics.StageMetrics;
import io.github.elevateddev.lattice.placement.MemoryMode;
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
@Outcome(id = "0, 0, 1", expect = Expect.ACCEPTABLE, desc = "Close won; no value published and the edge drained cleanly.")
@Outcome(id = "1, 1, 1", expect = Expect.ACCEPTABLE, desc = "Offer won; the value was consumed and the edge drained cleanly.")
@Outcome(expect = Expect.FORBIDDEN, desc = "The edge must not retain a close-raced SPSC reservation.")
@State
public class SpscClosePublicationDrainStressTest {

    private final SpscRingEdge edge = new SpscRingEdge(
        "source",
        "sink",
        2,
        MemoryMode.onHeapSlots(),
        edgeMetrics(),
        graphMetrics(),
        true,
        true
    );
    private int offered;

    @Actor
    public void producer() {
        offered = edge.offer(1) ? 1 : 0;
    }

    @Actor
    public void closer() {
        edge.close();
    }

    @Arbiter
    public void arbiter(final III_Result result) {
        final Object[] batch = new Object[2];
        result.r1 = offered;
        result.r2 = edge.drainTo(batch, 0, 2);
        result.r3 = edge.isEmpty() ? 1 : 0;
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
