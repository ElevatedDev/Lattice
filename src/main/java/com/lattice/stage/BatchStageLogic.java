package com.lattice.stage;

@FunctionalInterface
public interface BatchStageLogic<I, O> {

    void onBatch(Batch<I> batch, Output<O> output, StageContext context) throws Exception;
}
