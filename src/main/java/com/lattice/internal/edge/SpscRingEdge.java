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

    private static final VarHandle CURSOR;
    private static final VarHandle CLOSED;
    private static final VarHandle ELEMENT;
    private static final Object DROPPED = new Object();
    private static final long CLOSED_TAIL_BIT = Long.MIN_VALUE;
    private static final long TAIL_SEQUENCE_MASK = Long.MAX_VALUE;

    static {
        try {
            CURSOR = MethodHandles.lookup().findVarHandle(PaddedLong.class, "value", long.class);
            CLOSED = MethodHandles.lookup().findVarHandle(PaddedBoolean.class, "value", boolean.class);
            ELEMENT = MethodHandles.arrayElementVarHandle(Object[].class);
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
    private final PaddedLong head = new PaddedLong();
    private final PaddedLong tail = new PaddedLong();
    private final PaddedBoolean closed = new PaddedBoolean();

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
        if (closed()) {
            return false;
        }
        final long currentTailRaw = (long) CURSOR.getAcquire(tail);
        if (tailClosed(currentTailRaw)) {
            return false;
        }
        final long currentTail = tailSequence(currentTailRaw);
        final long wrapPoint = currentTail - capacity;
        final long currentHead = (long) CURSOR.getAcquire(head);
        if (currentHead <= wrapPoint) {
            return false;
        }
        if (!CURSOR.compareAndSet(tail, currentTailRaw, currentTail + 1L)) {
            return false;
        }

        final int index = (int) currentTail & mask;
        final Object[] localBuffer = buffer;
        final LongAccess localPublishTimes = publishTimes;
        if (closed()) {
            if (localPublishTimes != null) {
                localPublishTimes.setPlain(index, 0L);
            }
            ELEMENT.setRelease(localBuffer, index, DROPPED);
            return false;
        }
        if (localPublishTimes != null) {
            localPublishTimes.setPlain(index, System.nanoTime());
        }
        ELEMENT.setRelease(localBuffer, index, item);
        metrics.recordEmit();
        graphMetrics.recordEmit();
        return true;
    }

    @Override
    public Object poll() {
        final long currentHead = (long) CURSOR.get(head);
        final long currentTail = tailSequence((long) CURSOR.getAcquire(tail));
        if (currentHead >= currentTail) {
            return null;
        }
        final int index = (int) currentHead & mask;
        final Object[] localBuffer = buffer;
        final Object item = claimReadyItem(localBuffer, index);
        if (item == null) {
            return null;
        }
        final LongAccess localPublishTimes = publishTimes;
        if (item == DROPPED) {
            if (localPublishTimes != null) {
                localPublishTimes.setPlain(index, 0L);
            }
            CURSOR.setRelease(head, currentHead + 1L);
            return null;
        }
        if (localPublishTimes != null) {
            metrics.recordResidenceNanos(System.nanoTime() - localPublishTimes.getPlain(index));
            localPublishTimes.setPlain(index, 0L);
        }
        CURSOR.setRelease(head, currentHead + 1L);
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

        final long currentHead = (long) CURSOR.get(head);
        final long currentTail = tailSequence((long) CURSOR.getAcquire(tail));
        if (currentHead >= currentTail) {
            return 0;
        }

        long nextHead = currentHead;
        int targetIndex = offset;
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        final LongAccess localPublishTimes = publishTimes;
        if (localPublishTimes == null) {
            int claimed = 0;
            while (claimed < max && nextHead < currentTail) {
                final int index = (int) nextHead & localMask;
                final Object item = claimReadyItem(localBuffer, index);
                if (item == null) {
                    break;
                }
                if (item != DROPPED) {
                    target[targetIndex++] = item;
                    claimed++;
                }
                nextHead++;
            }
            if (nextHead == currentHead) {
                return 0;
            }
        } else {
            int claimed = 0;
            while (claimed < max && nextHead < currentTail) {
                final int index = (int) nextHead & localMask;
                final Object item = claimReadyItem(localBuffer, index);
                if (item == null) {
                    break;
                }
                if (item != DROPPED) {
                    target[targetIndex++] = item;
                    metrics.recordResidenceNanos(System.nanoTime() - localPublishTimes.getPlain(index));
                    claimed++;
                }
                localPublishTimes.setPlain(index, 0L);
                nextHead++;
            }
            if (nextHead == currentHead) {
                return 0;
            }
        }

        CURSOR.setRelease(head, nextHead);
        final int consumed = targetIndex - offset;
        recordConsumed(consumed);
        return consumed;
    }

    private void recordConsumed(final int count) {
        metrics.recordConsume(count);
        graphMetrics.recordConsume(count);
    }

    @Override
    public Object dropOldest() {
        final long currentHead = (long) CURSOR.getAcquire(head);
        final long currentTail = tailSequence((long) CURSOR.getAcquire(tail));
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        for (long sequence = currentHead; sequence < currentTail; sequence++) {
            final int index = (int) sequence & localMask;
            final Object item = (Object) ELEMENT.getAcquire(localBuffer, index);
            if (item != null && item != DROPPED && ELEMENT.compareAndSet(localBuffer, index, item, DROPPED)) {
                reclaimDroppedPrefix();
                return item;
            }
        }
        return null;
    }

    @Override
    public boolean tryCoalesce(final Object item, final Function<Object, ?> keyExtractor) {
        if (keyExtractor == null || item == null) {
            return false;
        }
        final Object key = keyExtractor.apply(item);
        final long currentHead = (long) CURSOR.getAcquire(head);
        final long currentTail = tailSequence((long) CURSOR.getAcquire(tail));
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        for (long sequence = currentHead; sequence < currentTail; sequence++) {
            final int index = (int) sequence & localMask;
            final Object existing = (Object) ELEMENT.getAcquire(localBuffer, index);
            if (existing != null && existing != DROPPED && keyEquals(key, keyExtractor.apply(existing))) {
                if (containsHandle(existing) || containsHandle(item)) {
                    return false;
                }
                return ELEMENT.compareAndSet(localBuffer, index, existing, item);
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
        return (long) CURSOR.getAcquire(head) >= tailSequence((long) CURSOR.getAcquire(tail));
    }

    @Override
    public int inFlight() {
        final long currentHead = (long) CURSOR.getAcquire(head);
        final long currentTail = tailSequence((long) CURSOR.getAcquire(tail));
        final long depth = Math.max(0L, currentTail - currentHead);
        return depth > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) depth;
    }

    @Override
    public boolean isClosed() {
        return closed();
    }

    @Override
    public void close() {
        closeFlag();
    }

    @Override
    public void abort() {
        closeFlag();
        drainAndRelease();
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public EdgeMetrics metrics() {
        return metrics;
    }

    private Object claimReadyItem(final Object[] localBuffer, final int index) {
        while (true) {
            final Object item = (Object) ELEMENT.getAcquire(localBuffer, index);
            if (item == null) {
                return null;
            }
            if (ELEMENT.compareAndSet(localBuffer, index, item, null)) {
                return item;
            }
        }
    }

    private void drainAndRelease() {
        if (buffer == null) {
            return;
        }
        final long targetTail = tailSequence((long) CURSOR.getAcquire(tail));
        while (true) {
            final long currentHead = (long) CURSOR.get(head);
            if (currentHead >= targetTail) {
                return;
            }
            final int index = (int) currentHead & mask;
            final Object item = claimReadyItem(buffer, index);
            if (item == null) {
                Thread.onSpinWait();
                continue;
            }
            final LongAccess localPublishTimes = publishTimes;
            if (localPublishTimes != null) {
                localPublishTimes.setPlain(index, 0L);
            }
            CURSOR.setRelease(head, currentHead + 1L);
            if (item != DROPPED) {
                releaseIfHandle(item);
            }
        }
    }

    private void reclaimDroppedPrefix() {
        final Object[] localBuffer = buffer;
        if (localBuffer == null) {
            return;
        }
        final LongAccess localPublishTimes = publishTimes;
        while (true) {
            final long currentHead = (long) CURSOR.getAcquire(head);
            if (currentHead >= tailSequence((long) CURSOR.getAcquire(tail))) {
                return;
            }
            final int index = (int) currentHead & mask;
            final Object item = (Object) ELEMENT.getAcquire(localBuffer, index);
            if (item != DROPPED) {
                return;
            }
            if (CURSOR.compareAndSet(head, currentHead, currentHead + 1L)) {
                ELEMENT.compareAndSet(localBuffer, index, DROPPED, null);
                if (localPublishTimes != null) {
                    localPublishTimes.setPlain(index, 0L);
                }
            }
        }
    }

    private boolean closed() {
        return (boolean) CLOSED.getVolatile(closed);
    }

    private void closeFlag() {
        CLOSED.setVolatile(closed, true);
        closeTail();
    }

    private void closeTail() {
        while (true) {
            final long currentTail = (long) CURSOR.getVolatile(tail);
            if (tailClosed(currentTail)) {
                return;
            }
            if (CURSOR.compareAndSet(tail, currentTail, currentTail | CLOSED_TAIL_BIT)) {
                return;
            }
        }
    }

    private static long tailSequence(final long tailValue) {
        return tailValue & TAIL_SEQUENCE_MASK;
    }

    private static boolean tailClosed(final long tailValue) {
        return (tailValue & CLOSED_TAIL_BIT) != 0L;
    }

    private static void releaseIfHandle(final Object item) {
        if (item instanceof SlabHandle<?> handle) {
            handle.release();
        } else if (item instanceof Stamped<?> stamped) {
            releaseIfHandle(stamped.value());
        }
    }
}
