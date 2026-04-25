package com.lattice.internal.wait;

import com.lattice.metrics.WaitMetrics;

public interface WaitStrategy {

    int idle(int idleCount, WaitMetrics metrics);
}
