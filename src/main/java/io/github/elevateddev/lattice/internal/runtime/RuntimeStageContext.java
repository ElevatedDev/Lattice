package io.github.elevateddev.lattice.internal.runtime;

import io.github.elevateddev.lattice.graph.GraphState;
import io.github.elevateddev.lattice.metrics.StageMetrics;
import io.github.elevateddev.lattice.stage.StageContext;

final class RuntimeStageContext implements StageContext {

    private final RuntimeCoordinator coordinator;
    private final String stageName;
    private final StageMetrics metrics;

    RuntimeStageContext(
        final RuntimeCoordinator coordinator,
        final String stageName,
        final StageMetrics metrics
    ) {
        this.coordinator = coordinator;
        this.stageName = stageName;
        this.metrics = metrics;
    }

    @Override
    public String graphName() {
        return coordinator.graphName();
    }

    @Override
    public String stageName() {
        return stageName;
    }

    @Override
    public GraphState graphState() {
        return coordinator.state();
    }

    @Override
    public StageMetrics metrics() {
        return metrics;
    }

    @Override
    public boolean isStopping() {
        return coordinator.isStopping();
    }
}
