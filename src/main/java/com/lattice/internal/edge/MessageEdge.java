package com.lattice.internal.edge;

import com.lattice.metrics.EdgeMetrics;
import java.util.function.Function;

public interface MessageEdge {

    String from();

    String to();

    boolean offer(Object item);

    Object poll();

    default int drainTo(final Object[] target, final int limit) {
        return drainTo(target, 0, limit);
    }

    int drainTo(Object[] target, int offset, int limit);

    Object dropOldest();

    boolean tryCoalesce(Object item, Function<Object, ?> keyExtractor);

    void firstTouch(String ownerName);

    boolean isEmpty();

    boolean isClosed();

    void close();

    void abort();

    int capacity();

    EdgeMetrics metrics();
}
