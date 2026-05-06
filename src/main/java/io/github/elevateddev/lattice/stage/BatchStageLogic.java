package io.github.elevateddev.lattice.stage;

/**
 * User callback for batch stage processing.
 *
 * @param <I> input item type
 * @param <O> output item type
 */
@FunctionalInterface
public interface BatchStageLogic<I, O> {

    /**
     * Processes one batch of input items.
     */
    void onBatch(Batch<I> batch, Output<O> output, StageContext context) throws Exception;
}
