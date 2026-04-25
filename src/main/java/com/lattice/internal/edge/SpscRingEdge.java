package com.lattice.internal.edge;

import com.lattice.metrics.EdgeMetrics;
import com.lattice.metrics.GraphMetrics;
import com.lattice.placement.MemoryMode;
import com.lattice.routing.Stamped;
import com.lattice.slab.SlabHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Function;

public final class SpscRingEdge implements MessageEdge {

    private static final VarHandle HEAD;
    private static final VarHandle TAIL;

    static {
        try {
            HEAD = MethodHandles.lookup().findVarHandle(SpscRingEdge.class, "head", long.class);
            TAIL = MethodHandles.lookup().findVarHandle(SpscRingEdge.class, "tail", long.class);
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
    private LongAccess publishTimes;
    private volatile long head;
    private volatile long tail;
    private volatile boolean closed;

    public SpscRingEdge(
        final String from,
        final String to,
        final int capacity,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics
    ) {
        this(from, to, capacity, MemoryMode.onHeapSlots(), metrics, graphMetrics);
        firstTouch(from);
    }

    public SpscRingEdge(
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
        if (closed) {
            return false;
        }
        final long currentTail = (long) TAIL.get(this);
        final long wrapPoint = currentTail - capacity;
        final long currentHead = (long) HEAD.getAcquire(this);
        if (currentHead <= wrapPoint) {
            return false;
        }
        final int index = (int) currentTail & mask;
        final Object[] localBuffer = buffer;
        localBuffer[index] = item;
        final LongAccess localPublishTimes = publishTimes;
        if (localPublishTimes != null) {
            localPublishTimes.setPlain(index, System.nanoTime());
        }
        TAIL.setRelease(this, currentTail + 1L);
        metrics.recordEmit();
        graphMetrics.recordEmit();
        return true;
    }

    @Override
    public Object poll() {
        final long currentHead = (long) HEAD.get(this);
        final long currentTail = (long) TAIL.getAcquire(this);
        if (currentHead >= currentTail) {
            return null;
        }
        final int index = (int) currentHead & mask;
        final Object[] localBuffer = buffer;
        final Object item = localBuffer[index];
        localBuffer[index] = null;
        final LongAccess localPublishTimes = publishTimes;
        if (localPublishTimes != null) {
            metrics.recordResidenceNanos(System.nanoTime() - localPublishTimes.getPlain(index));
            localPublishTimes.setPlain(index, 0L);
        }
        HEAD.setRelease(this, currentHead + 1L);
        metrics.recordConsume();
        graphMetrics.recordConsume();
        return item;
    }

    @Override
    public int drainTo(final Object[] target, final int offset, final int limit) {
        final int max = Math.min(limit, target.length - offset);
        if (max <= 0) {
            return 0;
        }

        final long currentHead = (long) HEAD.get(this);
        final long currentTail = (long) TAIL.getAcquire(this);
        final int drained = (int) Math.min((long) max, currentTail - currentHead);
        if (drained <= 0) {
            return 0;
        }

        long nextHead = currentHead;
        int targetIndex = offset;
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        final LongAccess localPublishTimes = publishTimes;
        if (localPublishTimes == null) {
            for (int i = 0; i < drained; i++) {
                final int index = (int) nextHead & localMask;
                target[targetIndex++] = localBuffer[index];
                localBuffer[index] = null;
                nextHead++;
            }
        } else {
            for (int i = 0; i < drained; i++) {
                final int index = (int) nextHead & localMask;
                target[targetIndex++] = localBuffer[index];
                localBuffer[index] = null;
                metrics.recordResidenceNanos(System.nanoTime() - localPublishTimes.getPlain(index));
                localPublishTimes.setPlain(index, 0L);
                nextHead++;
            }
        }

        HEAD.setRelease(this, currentHead + drained);
        recordConsumed(drained);
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
        final long currentTail = (long) TAIL.getAcquire(this);
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        for (long sequence = currentHead; sequence < currentTail; sequence++) {
            final int index = (int) sequence & localMask;
            final Object existing = localBuffer[index];
            if (existing != null && keyEquals(key, keyExtractor.apply(existing))) {
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
        if (EdgeMetrics.residenceTimingEnabled()) {
            publishTimes = LongAccess.create(capacity, memoryKind);
        }
        metrics.recordFirstTouch(System.nanoTime() - started);
    }

    @Override
    public boolean isEmpty() {
        return (long) HEAD.getAcquire(this) >= (long) TAIL.getAcquire(this);
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
