package io.github.elevateddev.lattice.stage;

/**
 * User callback for single-message stage processing.
 *
 * @param <I> input item type
 * @param <O> output item type
 */
@FunctionalInterface
public interface StageLogic<I, O> {

    /**
     * Processes one input item.
     */
    void onMessage(I input, Output<O> output, StageContext context) throws Exception;
}
