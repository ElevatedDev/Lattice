package io.github.elevateddev.lattice.internal.edge;

/**
 * Cache-line-padded mutable {@code long} container for producer/consumer side scratch
 * variables (head cache on the producer side, tail cache on the consumer side). Unlike
 * {@link PaddedLong}, callers read/write {@link #value} directly: the field is plain,
 * not VarHandle-accessed, because a single-producer (or single-consumer) thread owns it.
 *
 * <p>The padding around {@code value} prevents false sharing with neighbouring fields
 * on the enclosing edge object (the producer head cache must not share a cache line with
 * the consumer tail cache or with the published {@code head}/{@code tail} cursors).
 */
@SuppressWarnings("unused")
final class PaddedLongCache {
    long p01;
    long p02;
    long p03;
    long p04;
    long p05;
    long p06;
    long p07;
    long value;
    long p11;
    long p12;
    long p13;
    long p14;
    long p15;
    long p16;
    long p17;
}

