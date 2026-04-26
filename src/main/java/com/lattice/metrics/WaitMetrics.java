package com.lattice.metrics;

/**
 * Sink for wait-strategy counters.
 */
public interface WaitMetrics {

    /**
     * Records a spin wait iteration.
     */
    void recordSpin();

    /**
     * Records a thread yield.
     */
    void recordYield();

    /**
     * Records a park wait.
     */
    void recordPark();
}
