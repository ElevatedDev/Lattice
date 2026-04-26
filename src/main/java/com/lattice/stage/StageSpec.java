package com.lattice.stage;

import com.lattice.placement.PinPolicy;
import com.lattice.wait.WaitSpec;
import java.util.Objects;

/**
 * Immutable configuration for a graph worker stage.
 * <p>
 * Stage specs combine execution model, batching, placement, and wait behavior.
 * Builder-style methods return a new spec and leave the original unchanged.
 */
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

    /**
     * Creates the default single-threaded stage spec.
     */
    public static StageSpec singleThreaded() {
        return new StageSpec(StageExecution.SINGLE_THREADED, BatchPolicy.disabled(), PinPolicy.none(), WaitSpec.phasedDefault());
    }

    /**
     * Returns a copy with a different batch policy.
     */
    public StageSpec batch(final BatchPolicy batchPolicy) {
        return new StageSpec(execution, batchPolicy, pinPolicy, waitSpec);
    }

    /**
     * Returns a copy with a different pin policy.
     */
    public StageSpec pin(final PinPolicy pinPolicy) {
        return new StageSpec(execution, batchPolicy, pinPolicy, waitSpec);
    }

    /**
     * Returns a copy with a different wait strategy.
     */
    public StageSpec wait(final WaitSpec waitSpec) {
        return new StageSpec(execution, batchPolicy, pinPolicy, waitSpec);
    }

    /**
     * Returns the stage execution model.
     */
    public StageExecution execution() {
        return execution;
    }

    /**
     * Returns the stage batch policy.
     */
    public BatchPolicy batchPolicy() {
        return batchPolicy;
    }

    /**
     * Returns the stage placement policy.
     */
    public PinPolicy pinPolicy() {
        return pinPolicy;
    }

    /**
     * Returns the wait strategy used by this stage.
     */
    public WaitSpec waitSpec() {
        return waitSpec;
    }

    /**
     * Stage execution models.
     */
    public enum StageExecution {
        /**
         * Runs the stage on one worker thread.
         */
        SINGLE_THREADED
    }
}
