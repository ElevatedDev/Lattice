package com.lattice.stage;

import com.lattice.placement.PinPolicy;
import com.lattice.wait.WaitSpec;
import java.util.Objects;

public final class StageSpec {

    private final StageExecution execution;
    private final BatchPolicy batchPolicy;
    private final PinPolicy pinPolicy;
    private final WaitSpec waitSpec;

    private StageSpec(
        final StageExecution execution,
        final BatchPolicy batchPolicy,
        final PinPolicy pinPolicy,
        final WaitSpec waitSpec
    ) {
        this.execution = Objects.requireNonNull(execution, "execution");
        this.batchPolicy = Objects.requireNonNull(batchPolicy, "batchPolicy");
        this.pinPolicy = Objects.requireNonNull(pinPolicy, "pinPolicy");
        this.waitSpec = Objects.requireNonNull(waitSpec, "waitSpec");
    }

    public static StageSpec singleThreaded() {
        return new StageSpec(StageExecution.SINGLE_THREADED, BatchPolicy.disabled(), PinPolicy.none(), WaitSpec.phasedDefault());
    }

    public StageSpec batch(final BatchPolicy batchPolicy) {
        return new StageSpec(execution, batchPolicy, pinPolicy, waitSpec);
    }

    public StageSpec pin(final PinPolicy pinPolicy) {
        return new StageSpec(execution, batchPolicy, pinPolicy, waitSpec);
    }

    public StageSpec wait(final WaitSpec waitSpec) {
        return new StageSpec(execution, batchPolicy, pinPolicy, waitSpec);
    }

    public StageExecution execution() {
        return execution;
    }

    public BatchPolicy batchPolicy() {
        return batchPolicy;
    }

    public PinPolicy pinPolicy() {
        return pinPolicy;
    }

    public WaitSpec waitSpec() {
        return waitSpec;
    }

    public enum StageExecution {
        SINGLE_THREADED
    }
}
