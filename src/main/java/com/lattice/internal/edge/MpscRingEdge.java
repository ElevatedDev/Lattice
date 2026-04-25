package com.lattice.internal.edge;

import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.placement.MemoryMode;
import com.lattice.routing.Stamped;
import com.lattice.slab.SlabHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Function;

public final class MpscRingEdge implements MessageEdge {

    private static final VarHandle TAIL;
    private static final VarHandle HEAD;
    private static final Object SKIPPED = new Object();

    static {
        try {
            TAIL = MethodHandles.lookup().findVarHandle(MpscRingEdge.class, "tail", long.class);
            HEAD = MethodHandles.lookup().findVarHandle(MpscRingEdge.class, "head", long.class);
        } catch (final ReflectiveOperationException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private final String from;
    private final String to;
    private final int capacity;
    private final int mask;
    private final MemoryMode.MemoryKind memoryKind;
    private final EdgeMetrics metrics;
    private final GraphMetrics graphMetrics;
    private Object[] buffer;
    private LongAccess sequences;
    private LongAccess publishTimes;
    private volatile long head;
    private volatile long tail;
    private volatile boolean closed;

    public MpscRingEdge(
        final String from,
        final String to,
        final int capacity,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics
    ) {
        this(from, to, capacity, MemoryMode.onHeapSlots(), metrics, graphMetrics);
        firstTouch(to);
    }

    public MpscRingEdge(
        final String from,
        final String to,
        final int capacity,
        final MemoryMode memoryMode,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics
    ) {
        this.from = from;
        this.to = to;
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.memoryKind = memoryMode.kind();
        this.metrics = metrics;
        this.graphMetrics = graphMetrics;
    }

    @Override
    public String from() {
        return from;
    }

    @Override
    public String to() {
        return to;
    }

    @Override
    public boolean offer(final Object item) {
        final Object[] localBuffer = buffer;
        final LongAccess localSequences = sequences;
        final LongAccess localPublishTimes = publishTimes;
        long currentTail;
        int index;
        while (true) {
            if (closed) {
                return false;
            }
            currentTail = (long) TAIL.getVolatile(this);
            index = (int) currentTail & mask;
            final long sequence = localSequences.getAcquire(index);
            final long diff = sequence - currentTail;
            if (diff == 0L) {
                if (TAIL.compareAndSet(this, currentTail, currentTail + 1L)) {
                    break;
                }
            } else if (diff < 0L) {
                return false;
            } else {
                Thread.onSpinWait();
            }
        }

        if (closed) {
            localBuffer[index] = SKIPPED;
            localSequences.setRelease(index, currentTail + 1L);
            return false;
        }

        localBuffer[index] = item;
        if (localPublishTimes != null) {
            localPublishTimes.setPlain(index, System.nanoTime());
        }
        if (closed) {
            localBuffer[index] = SKIPPED;
            if (localPublishTimes != null) {
                localPublishTimes.setPlain(index, 0L);
            }
            localSequences.setRelease(index, currentTail + 1L);
            return false;
        }
        localSequences.setRelease(index, currentTail + 1L);
        metrics.recordEmit();
        graphMetrics.recordEmit();
        return true;
    }

    @Override
    public Object poll() {
        final long currentHead = (long) HEAD.get(this);
        final int index = (int) currentHead & mask;
        final Object[] localBuffer = buffer;
        final LongAccess localSequences = sequences;
        final long sequence = localSequences.getAcquire(index);
        final long diff = sequence - (currentHead + 1L);
        if (diff != 0L) {
            return null;
        }

        final Object item = localBuffer[index];
        localBuffer[index] = null;
        final LongAccess localPublishTimes = publishTimes;
        if (item == SKIPPED) {
            if (localPublishTimes != null) {
                localPublishTimes.setPlain(index, 0L);
            }
            HEAD.setRelease(this, currentHead + 1L);
            localSequences.setRelease(index, currentHead + capacity);
            return null;
        }
        if (localPublishTimes != null) {
            metrics.recordResidenceNanos(System.nanoTime() - localPublishTimes.getPlain(index));
            localPublishTimes.setPlain(index, 0L);
        }
        HEAD.setRelease(this, currentHead + 1L);
        localSequences.setRelease(index, currentHead + capacity);
        metrics.recordConsume();
        graphMetrics.recordConsume();
        return item;
    }

    @Override
    public int drainTo(final Object[] target, final int offset, final int limit) {
        int drained = 0;
        final int max = Math.min(limit, target.length - offset);
        if (max <= 0) {
            return 0;
        }

        final long currentHead = (long) HEAD.get(this);
        long nextHead = currentHead;
        int targetIndex = offset;
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        final int localCapacity = capacity;
        final LongAccess localSequences = sequences;
        final LongAccess localPublishTimes = publishTimes;

        if (localPublishTimes == null) {
            while (drained < max) {
                final int index = (int) nextHead & localMask;
                final long sequence = localSequences.getAcquire(index);
                if (sequence - (nextHead + 1L) != 0L) {
                    break;
                }
                final Object item = localBuffer[index];
                localBuffer[index] = null;
                if (item == SKIPPED) {
                    localSequences.setRelease(index, nextHead + localCapacity);
                    nextHead++;
                    continue;
                }
                target[targetIndex++] = item;
                localSequences.setRelease(index, nextHead + localCapacity);
                nextHead++;
                drained++;
            }
        } else {
            while (drained < max) {
                final int index = (int) nextHead & localMask;
                final long sequence = localSequences.getAcquire(index);
                if (sequence - (nextHead + 1L) != 0L) {
                    break;
                }
                final Object item = localBuffer[index];
                localBuffer[index] = null;
                if (item == SKIPPED) {
                    localPublishTimes.setPlain(index, 0L);
                    localSequences.setRelease(index, nextHead + localCapacity);
                    nextHead++;
                    continue;
                }
                target[targetIndex++] = item;
                metrics.recordResidenceNanos(System.nanoTime() - localPublishTimes.getPlain(index));
                localPublishTimes.setPlain(index, 0L);
                localSequences.setRelease(index, nextHead + localCapacity);
                nextHead++;
                drained++;
            }
        }

        if (nextHead > currentHead) {
            HEAD.setRelease(this, nextHead);
        }
        if (drained > 0) {
            recordConsumed(drained);
        }
        return drained;
    }

    private void recordConsumed(final int count) {
        metrics.recordConsume(count);
        graphMetrics.recordConsume(count);
    }

    @Override
    public Object dropOldest() {
        return poll();
    }

    @Override
    public boolean tryCoalesce(final Object item, final Function<Object, ?> keyExtractor) {
        if (keyExtractor == null || item == null) {
            return false;
        }
        final Object key = keyExtractor.apply(item);
        final long currentHead = (long) HEAD.getAcquire(this);
        final long currentTail = (long) TAIL.getVolatile(this);
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        final LongAccess localSequences = sequences;
        for (long sequence = currentHead; sequence < currentTail; sequence++) {
            final int index = (int) sequence & localMask;
            if (localSequences.getAcquire(index) - (sequence + 1L) != 0L) {
                continue;
            }
            final Object existing = localBuffer[index];
            if (existing != null && existing != SKIPPED && keyEquals(key, keyExtractor.apply(existing))) {
                if (containsHandle(existing) || containsHandle(item)) {
                    return false;
                }
                localBuffer[index] = item;
                return true;
            }
        }
        return false;
    }

    private static boolean keyEquals(final Object left, final Object right) {
        return left == null ? right == null : left.equals(right);
    }

    private static boolean containsHandle(final Object item) {
        if (item instanceof SlabHandle<?>) {
            return true;
        }
        return item instanceof Stamped<?> stamped && containsHandle(stamped.value());
    }

    @Override
    public synchronized void firstTouch(final String ownerName) {
        if (buffer != null) {
            return;
        }
        final long started = System.nanoTime();
        buffer = new Object[capacity];
        sequences = LongAccess.create(capacity, memoryKind);
        for (int i = 0; i < capacity; i++) {
            sequences.setPlain(i, i);
        }
        if (EdgeMetrics.residenceTimingEnabled()) {
            publishTimes = LongAccess.create(capacity, memoryKind);
        }
        metrics.recordFirstTouch(System.nanoTime() - started);
    }

    @Override
    public boolean isEmpty() {
        return (long) HEAD.getAcquire(this) >= (long) TAIL.getVolatile(this);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void abort() {
        closed = true;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public EdgeMetrics metrics() {
        return metrics;
    }
}
