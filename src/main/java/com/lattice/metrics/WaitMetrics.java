package com.lattice.metrics;

public interface WaitMetrics {

    void recordSpin();

    void recordYield();

    void recordPark();
}
