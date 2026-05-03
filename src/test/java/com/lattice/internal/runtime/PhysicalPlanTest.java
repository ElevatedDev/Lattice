package com.lattice.internal.runtime;

import com.lattice.edge.EdgeSpec;
import com.lattice.internal.graph.EdgeDefinition;
import com.lattice.placement.PinPolicy;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PhysicalPlanTest {

    @Test
    void accessorsExposeDecisionsAndInlineLifecycleParticipantCount() {
        final EdgeDefinition edge = new EdgeDefinition(
            "source",
            "stage",
            String.class,
            EdgeSpec.spscRing(8),
            0,
            0,
            false,
            true
        );
        final FusedSinkPlan fusedSink = new FusedSinkPlan("stage", "sink", edge, PinPolicy.none());
        final FusedStagePlan fusedStage = new FusedStagePlan(
            "stage",
            List.of("next"),
            List.of(edge),
            List.of(edge),
            List.of(),
            fusedSink,
            PinPolicy.none()
        );
        final FusedRouterPlan fusedRouter = new FusedRouterPlan("stage", "router", edge, List.of(edge), PinPolicy.none());
        final PhysicalPlan plan = new PhysicalPlan(
            List.of("stage"),
            Map.of("stage", fusedSink),
            Map.of("stage", fusedStage),
            Map.of("stage", fusedRouter),
            Set.of("stage->next"),
            Map.of("stage", new WorkerDecision(
                "stage",
                WorkerKind.RUNNABLE,
                OperatorKind.STAGE,
                OutputKind.LINEAR_FUSED,
                PinPolicy.none(),
                "",
                null
            )),
            Map.of("source->stage", new EdgeDecision(
                "source->stage",
                EdgeImplementationKind.SPSC_RING,
                EdgeUseKind.NORMAL,
                "stage",
                true,
                null
            )),
            Map.of("source", new SenderDecision(
                "source",
                "source->stage",
                SenderKind.EDGE,
                "",
                OutputKind.EDGE,
                null
            )),
            Map.of("stage", new InlineSourceBinding("stage", "source", "source->stage")),
            List.of(new PlanningFallback("graph", FallbackReason.FUSION_DISABLED))
        );

        assertSame(fusedSink, plan.fusedSink("stage"));
        assertSame(fusedStage, plan.fusedStage("stage"));
        assertSame(fusedRouter, plan.fusedRouter("stage"));
        assertEquals("source", plan.inlineFusedWorkerToSource().get("stage"));
        assertEquals(1, plan.lifecycleParticipantCount());
        assertEquals(EdgeUseKind.NORMAL, plan.edgeDecision("source->stage").useKind());
        assertThrows(UnsupportedOperationException.class, () -> plan.workerOrder().add("other"));
    }
}
