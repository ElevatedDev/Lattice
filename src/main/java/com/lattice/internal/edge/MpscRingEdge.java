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

    private static final VarHandle CURSOR;
    private static final VarHandle CLOSED;
    private static final VarHandle ELEMENT;
    private static final Object SKIPPED = new Object();
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
    private final boolean plainClaim;
    private Object[] buffer;
    private LongAccess publishedSequences;
    private LongAccess publishTimes;
    private final PaddedLong head = new PaddedLong();
    private final PaddedLong tail = new PaddedLong();
    private final PaddedLong producerHeadCache = new PaddedLong();
    private final PaddedBoolean closed = new PaddedBoolean();

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
        this(from, to, capacity, memoryMode, metrics, graphMetrics, true);
    }

    public MpscRingEdge(
        final String from,
        final String to,
        final int capacity,
        final MemoryMode memoryMode,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics,
        final boolean plainClaim
    ) {
        validateCapacity(capacity);
        this.from = from;
        this.to = to;
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.memoryKind = memoryMode.kind();
        this.metrics = metrics;
        this.graphMetrics = graphMetrics;
        this.plainClaim = plainClaim;
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
        final LongAccess localPublishedSequences = publishedSequences;
        final LongAccess localPublishTimes = publishTimes;
        final int localCapacity = capacity;
        final int localMask = mask;
        long currentTailRaw;
        long currentTail;
        int index;
        while (true) {
            currentTailRaw = (long) CURSOR.getAcquire(tail);
            if (tailClosed(currentTailRaw)) {
                return false;
            }
            currentTail = tailSequence(currentTailRaw);
            final long wrapPoint = currentTail - localCapacity;
            if ((long) CURSOR.getAcquire(producerHeadCache) <= wrapPoint) {
                final long currentHead = (long) CURSOR.getAcquire(head);
                if (currentHead <= wrapPoint) {
                    return false;
                }
                CURSOR.setRelease(producerHeadCache, currentHead);
            }
            index = (int) currentTail & localMask;
            if (CURSOR.compareAndSet(tail, currentTailRaw, currentTail + 1L)) {
                break;
            }
        }

        if (closed()) {
            localBuffer[index] = SKIPPED;
            if (localPublishTimes != null) {
                localPublishTimes.setPlain(index, 0L);
            }
            localPublishedSequences.setRelease(index, currentTail);
            return false;
        }
        localBuffer[index] = item;
        if (localPublishTimes != null) {
            localPublishTimes.setPlain(index, System.nanoTime());
        }
        localPublishedSequences.setRelease(index, currentTail);
        if (EdgeMetrics.hotCountersEnabled()) {
            metrics.recordEmit();
            graphMetrics.recordEmit();
        }
        return true;
    }

    @Override
    public Object poll() {
        final long currentHead = (long) CURSOR.get(head);
        final int index = (int) currentHead & mask;
        final Object[] localBuffer = buffer;
        final LongAccess localPublishedSequences = publishedSequences;
        if (localPublishedSequences.getAcquire(index) != currentHead) {
            return null;
        }

        final Object item = claimReadyItem(localBuffer, index);
        if (item == null) {
            return null;
        }
        final LongAccess localPublishTimes = publishTimes;
        if (item == SKIPPED || item == DROPPED) {
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
        if (EdgeMetrics.hotCountersEnabled()) {
            metrics.recordConsume();
            graphMetrics.recordConsume();
        }
        return item;
    }

    @Override
    public int drainTo(final Object[] target, final int offset, final int limit) {
        int drained = 0;
        final int max = Math.min(limit, target.length - offset);
        if (max <= 0) {
            return 0;
        }

        final long currentHead = (long) CURSOR.get(head);
        long nextHead = currentHead;
        int targetIndex = offset;
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        final LongAccess localPublishedSequences = publishedSequences;
        final LongAccess localPublishTimes = publishTimes;

        if (localPublishTimes == null) {
            while (drained < max) {
                final int index = (int) nextHead & localMask;
                if (localPublishedSequences.getAcquire(index) != nextHead) {
                    break;
                }
                final Object item = claimReadyItem(localBuffer, index);
                if (item == null) {
                    break;
                }
                if (item == SKIPPED || item == DROPPED) {
                    nextHead++;
                    continue;
                }
                target[targetIndex++] = item;
                nextHead++;
                drained++;
            }
        } else {
            while (drained < max) {
                final int index = (int) nextHead & localMask;
                if (localPublishedSequences.getAcquire(index) != nextHead) {
                    break;
                }
                final Object item = claimReadyItem(localBuffer, index);
                if (item == null) {
                    break;
                }
                if (item == SKIPPED || item == DROPPED) {
                    localPublishTimes.setPlain(index, 0L);
                    nextHead++;
                    continue;
                }
                target[targetIndex++] = item;
                metrics.recordResidenceNanos(System.nanoTime() - localPublishTimes.getPlain(index));
                localPublishTimes.setPlain(index, 0L);
                nextHead++;
                drained++;
            }
        }

        if (nextHead > currentHead) {
            CURSOR.setRelease(head, nextHead);
        }
        if (drained > 0) {
            recordConsumed(drained);
        }
        return drained;
    }

    @Override
    public int drainToProcessor(final ItemProcessor processor, final int limit) throws Exception {
        if (limit <= 0) {
            return 0;
        }
        if (plainClaim) {
            return drainPlainToProcessor(processor, limit);
        }

        final long currentHead = (long) CURSOR.get(head);
        long nextHead = currentHead;
        int processed = 0;
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        final LongAccess localPublishedSequences = publishedSequences;
        final LongAccess localPublishTimes = publishTimes;

        try {
            if (localPublishTimes == null) {
                while (processed < limit) {
                    final int index = (int) nextHead & localMask;
                    if (localPublishedSequences.getAcquire(index) != nextHead) {
                        break;
                    }
                    final Object item = claimReadyItem(localBuffer, index);
                    if (item == null) {
                        break;
                    }
                    nextHead++;
                    releaseHeadAtLeast(nextHead);
                    if (item == SKIPPED || item == DROPPED) {
                        continue;
                    }
                    processed++;
                    processor.process(item);
                }
            } else {
                while (processed < limit) {
                    final int index = (int) nextHead & localMask;
                    if (localPublishedSequences.getAcquire(index) != nextHead) {
                        break;
                    }
                    final Object item = claimReadyItem(localBuffer, index);
                    if (item == null) {
                        break;
                    }
                    if (item == SKIPPED || item == DROPPED) {
                        localPublishTimes.setPlain(index, 0L);
                        nextHead++;
                        releaseHeadAtLeast(nextHead);
                        continue;
                    }
                    metrics.recordResidenceNanos(System.nanoTime() - localPublishTimes.getPlain(index));
                    localPublishTimes.setPlain(index, 0L);
                    nextHead++;
                    releaseHeadAtLeast(nextHead);
                    processed++;
                    processor.process(item);
                }
            }
        } finally {
            if (nextHead > currentHead) {
                releaseHeadAtLeast(nextHead);
            }
            recordConsumed(processed);
        }
        return processed;
    }

    private int drainPlainToProcessor(final ItemProcessor processor, final int limit) throws Exception {
        final long currentHead = (long) CURSOR.get(head);
        long nextHead = currentHead;
        int processed = 0;
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        final LongAccess localPublishedSequences = publishedSequences;
        final LongAccess localPublishTimes = publishTimes;

        try {
            if (localPublishTimes == null) {
                while (processed < limit) {
                    final int index = (int) nextHead & localMask;
                    if (localPublishedSequences.getAcquire(index) != nextHead) {
                        break;
                    }
                    final Object item = localBuffer[index];
                    if (item == null) {
                        break;
                    }
                    localBuffer[index] = null;
                    nextHead++;
                    CURSOR.setRelease(head, nextHead);
                    if (item == SKIPPED || item == DROPPED) {
                        continue;
                    }
                    processed++;
                    processor.process(item);
                }
            } else {
                while (processed < limit) {
                    final int index = (int) nextHead & localMask;
                    if (localPublishedSequences.getAcquire(index) != nextHead) {
                        break;
                    }
                    final Object item = localBuffer[index];
                    if (item == null) {
                        break;
                    }
                    localBuffer[index] = null;
                    if (item == SKIPPED || item == DROPPED) {
                        localPublishTimes.setPlain(index, 0L);
                        nextHead++;
                        CURSOR.setRelease(head, nextHead);
                        continue;
                    }
                    metrics.recordResidenceNanos(System.nanoTime() - localPublishTimes.getPlain(index));
                    localPublishTimes.setPlain(index, 0L);
                    nextHead++;
                    CURSOR.setRelease(head, nextHead);
                    processed++;
                    processor.process(item);
                }
            }
        } finally {
            if (nextHead > currentHead) {
                releasePlainHeadAtLeast(nextHead);
            }
            recordConsumed(processed);
        }
        return processed;
    }

    private void releasePlainHeadAtLeast(final long nextHead) {
        final long currentHead = (long) CURSOR.get(head);
        if (currentHead < nextHead) {
            CURSOR.setRelease(head, nextHead);
        }
    }

    private void releaseHeadAtLeast(final long nextHead) {
        while (true) {
            final long currentHead = (long) CURSOR.getAcquire(head);
            if (currentHead >= nextHead) {
                return;
            }
            if (CURSOR.compareAndSet(head, currentHead, nextHead)) {
                return;
            }
        }
    }

    private void recordConsumed(final int count) {
        if (EdgeMetrics.hotCountersEnabled()) {
            metrics.recordConsume(count);
            graphMetrics.recordConsume(count);
        }
    }

    @Override
    public Object dropOldest() {
        final long currentHead = (long) CURSOR.getAcquire(head);
        final long currentTail = tailSequence((long) CURSOR.getAcquire(tail));
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        final LongAccess localPublishedSequences = publishedSequences;
        for (long sequence = currentHead; sequence < currentTail; sequence++) {
            final int index = (int) sequence & localMask;
            if (localPublishedSequences.getAcquire(index) != sequence) {
                continue;
            }
            final Object item = (Object) ELEMENT.getAcquire(localBuffer, index);
            if (item != null
                && item != SKIPPED
                && item != DROPPED
                && ELEMENT.compareAndSet(localBuffer, index, item, DROPPED)) {
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
        final LongAccess localPublishedSequences = publishedSequences;
        for (long sequence = currentHead; sequence < currentTail; sequence++) {
            final int index = (int) sequence & localMask;
            if (localPublishedSequences.getAcquire(index) != sequence) {
                continue;
            }
            final Object existing = (Object) ELEMENT.getAcquire(localBuffer, index);
            if (existing != null
                && existing != SKIPPED
                && existing != DROPPED
                && keyEquals(key, keyExtractor.apply(existing))) {
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
        publishedSequences = LongAccess.create(capacity, memoryKind);
        for (int i = 0; i < capacity; i++) {
            publishedSequences.setPlain(i, i - (long) capacity);
        }
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
        if (plainClaim) {
            final Object item = localBuffer[index];
            if (item != null) {
                localBuffer[index] = null;
            }
            return item;
        }
        return (Object) ELEMENT.getAndSetAcquire(localBuffer, index, null);
    }

    private void drainAndRelease() {
        if (buffer == null || publishedSequences == null) {
            return;
        }
        final long targetTail = tailSequence((long) CURSOR.getAcquire(tail));
        final LongAccess localPublishedSequences = publishedSequences;
        while (true) {
            final long currentHead = (long) CURSOR.get(head);
            if (currentHead >= targetTail) {
                return;
            }
            final int index = (int) currentHead & mask;
            if (localPublishedSequences.getAcquire(index) != currentHead) {
                Thread.onSpinWait();
                continue;
            }
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
            if (item != SKIPPED && item != DROPPED) {
                releaseIfHandle(item);
            }
        }
    }

    private void reclaimDroppedPrefix() {
        final Object[] localBuffer = buffer;
        final LongAccess localPublishedSequences = publishedSequences;
        if (localBuffer == null || localPublishedSequences == null) {
            return;
        }
        final LongAccess localPublishTimes = publishTimes;
        while (true) {
            final long currentHead = (long) CURSOR.getAcquire(head);
            final int index = (int) currentHead & mask;
            if (localPublishedSequences.getAcquire(index) != currentHead) {
                return;
            }
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

    private static void validateCapacity(final int capacity) {
        if (capacity < 2) {
            throw new IllegalArgumentException("MPSC edge capacity must be at least 2");
        }
        if (Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("MPSC edge capacity must be a power of two: " + capacity);
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
