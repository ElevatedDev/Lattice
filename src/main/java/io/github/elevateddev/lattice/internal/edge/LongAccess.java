package io.github.elevateddev.lattice.internal.edge;

import io.github.elevateddev.lattice.placement.MemoryMode;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

interface LongAccess {
    VarHandle HEAP_LONGS = MethodHandles.arrayElementVarHandle(long[].class);
    VarHandle DIRECT_LONGS = MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

    static LongAccess create(final int capacity, final MemoryMode.MemoryKind memoryKind) {
        if (memoryKind == MemoryMode.MemoryKind.OFF_HEAP_HANDLES) {
            return new Direct(capacity);
        }
        return new Heap(capacity);
    }

    long getPlain(int index);

    void setPlain(int index, long value);

    long getAcquire(int index);

    void setRelease(int index, long value);

    final class Heap implements LongAccess {
        private final long[] values;

        Heap(final int capacity) {
            this.values = new long[capacity];
        }

        @Override
        public long getPlain(final int index) {
            return values[index];
        }

        @Override
        public void setPlain(final int index, final long value) {
            values[index] = value;
        }

        @Override
        public long getAcquire(final int index) {
            return (long) HEAP_LONGS.getAcquire(values, index);
        }

        @Override
        public void setRelease(final int index, final long value) {
            HEAP_LONGS.setRelease(values, index, value);
        }
    }

    final class Direct implements LongAccess {
        private final ByteBuffer values;

        Direct(final int capacity) {
            this.values = ByteBuffer
                .allocateDirect(Math.multiplyExact(capacity, Long.BYTES))
                .order(ByteOrder.nativeOrder());
        }

        @Override
        public long getPlain(final int index) {
            return (long) DIRECT_LONGS.get(values, offset(index));
        }

        @Override
        public void setPlain(final int index, final long value) {
            DIRECT_LONGS.set(values, offset(index), value);
        }

        @Override
        public long getAcquire(final int index) {
            return (long) DIRECT_LONGS.getAcquire(values, offset(index));
        }

        @Override
        public void setRelease(final int index, final long value) {
            DIRECT_LONGS.setRelease(values, offset(index), value);
        }

        private static int offset(final int index) {
            return index << 3;
        }
    }
}
