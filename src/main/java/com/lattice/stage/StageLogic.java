package com.lattice.stage;

@FunctionalInterface
public interface StageLogic<I, O> {

    void onMessage(I input, Output<O> output, StageContext context) throws Exception;
}
