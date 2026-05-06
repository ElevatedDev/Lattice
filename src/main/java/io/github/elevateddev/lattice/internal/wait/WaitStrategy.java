package io.github.elevateddev.lattice.internal.wait;

import io.github.elevateddev.lattice.metrics.WaitMetrics;

public interface WaitStrategy {

    int idle(int idleCount, WaitMetrics metrics);
}
