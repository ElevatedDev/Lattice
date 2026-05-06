package io.github.elevateddev.lattice.internal.edge;

import io.github.elevateddev.lattice.metrics.EdgeMetrics;
import io.github.elevateddev.lattice.metrics.GraphMetrics;
import io.github.elevateddev.lattice.placement.MemoryMode;
import io.github.elevateddev.lattice.routing.Stamped;
import io.github.elevateddev.lattice.slab.SlabHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.function.Function;

public final class SpscRingEdge implements MessageEdge {

    private static final VarHandle CURSOR;
    private static final VarHandle CLOSED;
    private static final VarHandle PRODUCER_ACTIVE;
    private static final VarHandle ELEMENT;
    private static final Object DROPPED = new Object();

    static {
        try {
            CURSOR = MethodHandles.lookup().findVarHandle(PaddedLong.class, "value", long.class);
            CLOSED = MethodHandles.lookup().findVarHandle(PaddedBoolean.class, "value", boolean.class);
            PRODUCER_ACTIVE = MethodHandles.lookup().findVarHandle(PaddedBoolean.class, "value", boolean.class);
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
    private final boolean closeGuard;
    private final boolean hotCountersEnabled;
    private final boolean residenceTimingEnabled;
    private Object[] buffer;
    private LongAccess publishTimes;
    private final PaddedLong head = new PaddedLong();
    private final PaddedLong tail = new PaddedLong();
    private final PaddedBoolean closed = new PaddedBoolean();
    private final PaddedBoolean producerActive = new PaddedBoolean();

    private final PaddedLongCache producerHeadCacheBox = new PaddedLongCache();
    private final PaddedLongCache consumerTailCacheBox = new PaddedLongCache();

    public SpscRingEdge(
        final String from,
        final String to,
        final int capacity,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics
    ) {
        this(from, to, capacity, MemoryMode.onHeapSlots(), metrics, graphMetrics, true);
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
        this(from, to, capacity, memoryMode, metrics, graphMetrics, true);
    }

    public SpscRingEdge(
        final String from,
        final String to,
        final int capacity,
        final MemoryMode memoryMode,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics,
        final boolean plainClaim
    ) {
        this(from, to, capacity, memoryMode, metrics, graphMetrics, plainClaim, false);
    }

    public SpscRingEdge(
        final String from,
        final String to,
        final int capacity,
        final MemoryMode memoryMode,
        final EdgeMetrics metrics,
        final GraphMetrics graphMetrics,
        final boolean plainClaim,
        final boolean closeGuard
    ) {
        validateCapacity(capacity);
        final MemoryMode.MemoryKind selectedMemoryKind =
            Objects.requireNonNull(memoryMode, "memoryMode").kind();
        this.from = Objects.requireNonNull(from, "from");
        this.to = Objects.requireNonNull(to, "to");
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.memoryKind = selectedMemoryKind;
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.graphMetrics = Objects.requireNonNull(graphMetrics, "graphMetrics");
        this.plainClaim = plainClaim;
        this.closeGuard = closeGuard;
        this.hotCountersEnabled = metrics.hotCounters();
        this.residenceTimingEnabled = metrics.residenceTiming();
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
        if (closeGuard) {
            PRODUCER_ACTIVE.setVolatile(producerActive, true);
            try {
                if (closed()) {
                    return false;
                }
                return offerAfterOpenCheck(item);
            } finally {
                PRODUCER_ACTIVE.setVolatile(producerActive, false);
            }
        }
        if (closed()) {
            return false;
        }
        return offerAfterOpenCheck(item);
    }

    private boolean offerAfterOpenCheck(final Object item) {
        final long currentTail = (long) CURSOR.getOpaque(tail);
        final long wrapPoint = currentTail - capacity;
        final PaddedLongCache producerHeadCacheBoxLocal = producerHeadCacheBox;
        if (producerHeadCacheBoxLocal.value <= wrapPoint) {
            producerHeadCacheBoxLocal.value = (long) CURSOR.getAcquire(head);
            if (producerHeadCacheBoxLocal.value <= wrapPoint) {
                return false;
            }
        }

        final int index = (int) currentTail & mask;
        final Object[] localBuffer = buffer;
        final LongAccess localPublishTimes = publishTimes;
        if (localPublishTimes != null) {
            localPublishTimes.setPlain(index, System.nanoTime());
        }
        localBuffer[index] = item;
        CURSOR.setRelease(tail, currentTail + 1L);
        if (hotCountersEnabled) {
            metrics.recordEmit();
            graphMetrics.recordEmit();
        }
        return true;
    }

    @Override
    public Object poll() {
        final long currentHead = (long) CURSOR.getOpaque(head);
        final PaddedLongCache consumerTailCacheBoxLocal = consumerTailCacheBox;
        long currentTail = consumerTailCacheBoxLocal.value;
        if (currentHead >= currentTail) {
            currentTail = (long) CURSOR.getAcquire(tail);
            consumerTailCacheBoxLocal.value = currentTail;
        }
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
        if (hotCountersEnabled) {
            metrics.recordConsume();
            graphMetrics.recordConsume();
        }
        return item;
    }

    @Override
    public int drainTo(final Object[] target, final int offset, final int limit) {
        final int max = Math.min(limit, target.length - offset);
        if (max <= 0) {
            return 0;
        }

        final long currentHead = (long) CURSOR.getOpaque(head);
        final PaddedLongCache consumerTailCacheBoxLocal = consumerTailCacheBox;
        long currentTail = consumerTailCacheBoxLocal.value;
        if (currentHead >= currentTail) {
            currentTail = (long) CURSOR.getAcquire(tail);
            consumerTailCacheBoxLocal.value = currentTail;
        }
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

    @Override
    public int drainToProcessor(final ItemProcessor processor, final int limit) throws Exception {
        if (limit <= 0) {
            return 0;
        }

        final long currentHead = (long) CURSOR.getOpaque(head);
        final PaddedLongCache consumerTailCacheBoxLocal = consumerTailCacheBox;
        long currentTail = consumerTailCacheBoxLocal.value;
        if (currentHead >= currentTail) {
            currentTail = (long) CURSOR.getAcquire(tail);
            consumerTailCacheBoxLocal.value = currentTail;
        }
        if (currentHead >= currentTail) {
            return 0;
        }

        long nextHead = currentHead;
        int processed = 0;
        final Object[] localBuffer = buffer;
        final int localMask = mask;
        final LongAccess localPublishTimes = publishTimes;
        try {
            while (processed < limit && nextHead < currentTail) {
                final int index = (int) nextHead & localMask;
                final Object item = claimReadyItem(localBuffer, index);
                if (item == null) {
                    break;
                }
                if (localPublishTimes != null) {
                    if (item != DROPPED) {
                        metrics.recordResidenceNanos(System.nanoTime() - localPublishTimes.getPlain(index));
                    }
                    localPublishTimes.setPlain(index, 0L);
                }
                nextHead++;
                if (item != DROPPED) {
                    processed++;
                    processor.process(item);
                }
            }
        } finally {
            if (nextHead > currentHead) {
                CURSOR.setRelease(head, nextHead);
            }
            recordConsumed(processed);
        }
        return processed;
    }

    private void recordConsumed(final int count) {
        if (hotCountersEnabled) {
            metrics.recordConsume(count);
            graphMetrics.recordConsume(count);
        }
    }

    @Override
    public Object dropOldest() {
        final long currentHead = (long) CURSOR.getAcquire(head);
        final long currentTail = (long) CURSOR.getAcquire(tail);
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
        final long currentTail = (long) CURSOR.getAcquire(tail);
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
        if (residenceTimingEnabled) {
            publishTimes = LongAccess.create(capacity, memoryKind);
        }
        metrics.recordFirstTouch(System.nanoTime() - started);
    }

    @Override
    public boolean isEmpty() {
        return (long) CURSOR.getAcquire(head) >= (long) CURSOR.getAcquire(tail);
    }

    @Override
    public int inFlight() {
        final long currentHead = (long) CURSOR.getAcquire(head);
        final long currentTail = (long) CURSOR.getAcquire(tail);
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
    public void releaseRemainingAfterQuiescence() {
        closeFlag();
        drainAndRelease();
    }

    /**
     * Kept for direct edge tests and older internal callers. Runtime cleanup should use
     * releaseRemainingAfterQuiescence() so the quiescence invariant is visible at call sites.
     */
    public void abort() {
        releaseRemainingAfterQuiescence();
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
        if (buffer == null) {
            return;
        }
        final long targetTail = (long) CURSOR.getAcquire(tail);
        while (true) {
            final long currentHead = (long) CURSOR.getOpaque(head);
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
            if (currentHead >= (long) CURSOR.getAcquire(tail)) {
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
        return (boolean) CLOSED.getAcquire(closed);
    }

    private void closeFlag() {
        CLOSED.setRelease(closed, true);
        if (closeGuard) {
            while ((boolean) PRODUCER_ACTIVE.getVolatile(producerActive)) {
                Thread.onSpinWait();
            }
        }
    }

    private static void validateCapacity(final int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("SPSC edge capacity must be positive");
        }
        if (Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("SPSC edge capacity must be a power of two: " + capacity);
        }
    }

    private static void releaseIfHandle(final Object item) {
        if (item instanceof SlabHandle<?> handle) {
            handle.release();
        } else if (item instanceof Stamped<?> stamped) {
            releaseIfHandle(stamped.value());
        }
    }
}
