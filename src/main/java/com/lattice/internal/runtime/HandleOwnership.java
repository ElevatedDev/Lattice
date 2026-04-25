package com.lattice.internal.runtime;

import com.lattice.routing.Stamped;
import com.lattice.slab.SlabHandle;

final class HandleOwnership {

    private static final ThreadLocal<OwnershipContext> CONTEXT = new ThreadLocal<>();

    private HandleOwnership() {
    }

    static Scope scope(final Object input) {
        return context().activate(input);
    }

    static Scope scope(final Object[] inputs, final int count) {
        return context().activate(inputs, count);
    }

    static Scope scope(final Iterable<?> inputs) {
        return context().activate(inputs);
    }

    static Object prepareForEnqueue(final Object item) {
        final OwnershipContext context = CONTEXT.get();
        if (context == null || !context.active) {
            return item;
        }
        return context.retainIfActive(item);
    }

    static void releaseIfHandle(final Object item) {
        if (item instanceof SlabHandle<?> handle) {
            handle.release();
        } else if (item instanceof Stamped<?> stamped) {
            releaseIfHandle(stamped.value());
        }
    }

    private static Object retainIfActive(final Object item, final OwnershipContext context) {
        if (item instanceof SlabHandle<?> handle) {
            return context.containsSameHandle(handle) ? handle.retain() : item;
        }
        if (item instanceof Stamped<?> stamped) {
            final Object retainedValue = retainIfActive(stamped.value(), context);
            if (retainedValue != stamped.value()) {
                return Stamped.of(stamped.stamp(), retainedValue);
            }
        }
        return item;
    }

    private static OwnershipContext context() {
        OwnershipContext context = CONTEXT.get();
        if (context == null) {
            context = new OwnershipContext();
            CONTEXT.set(context);
        }
        return context;
    }

    private static boolean containsSameHandle(final Object item, final SlabHandle<?> handle) {
        if (item == handle) {
            return true;
        }
        return item instanceof Stamped<?> stamped && containsSameHandle(stamped.value(), handle);
    }

    private static final class OwnershipContext {
        private final ReusableScope reusableScope = new ReusableScope(this);
        private boolean active;
        private Object single;
        private Object[] array;
        private int arrayCount;
        private Iterable<?> iterable;

        Scope activate(final Object input) {
            return activate(input, null, 0, null);
        }

        Scope activate(final Object[] inputs, final int count) {
            return activate(null, inputs, Math.min(Math.max(0, count), inputs.length), null);
        }

        Scope activate(final Iterable<?> inputs) {
            return activate(null, null, 0, inputs);
        }

        Object retainIfActive(final Object item) {
            return HandleOwnership.retainIfActive(item, this);
        }

        boolean containsSameHandle(final SlabHandle<?> handle) {
            if (single != null && HandleOwnership.containsSameHandle(single, handle)) {
                return true;
            }
            final Object[] activeArray = array;
            for (int i = 0; i < arrayCount; i++) {
                if (HandleOwnership.containsSameHandle(activeArray[i], handle)) {
                    return true;
                }
            }
            final Iterable<?> activeIterable = iterable;
            if (activeIterable != null) {
                for (final Object input : activeIterable) {
                    if (HandleOwnership.containsSameHandle(input, handle)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private Scope activate(
            final Object input,
            final Object[] inputs,
            final int count,
            final Iterable<?> iterableInputs
        ) {
            if (active) {
                return new NestedScope(this).activate(input, inputs, count, iterableInputs);
            }
            active = true;
            single = input;
            array = inputs;
            arrayCount = Math.max(0, count);
            iterable = iterableInputs;
            return reusableScope;
        }

        private void deactivate() {
            active = false;
            single = null;
            array = null;
            arrayCount = 0;
            iterable = null;
        }
    }

    interface Scope extends AutoCloseable {

        @Override
        void close();
    }

    private static final class ReusableScope implements Scope {
        private final OwnershipContext context;

        private ReusableScope(final OwnershipContext context) {
            this.context = context;
        }

        @Override
        public void close() {
            context.deactivate();
        }
    }

    private static final class NestedScope implements Scope {
        private final OwnershipContext context;
        private boolean previousActive;
        private Object previousSingle;
        private Object[] previousArray;
        private int previousArrayCount;
        private Iterable<?> previousIterable;

        private NestedScope(final OwnershipContext context) {
            this.context = context;
        }

        Scope activate(final Object input, final Object[] inputs, final int count, final Iterable<?> iterableInputs) {
            previousActive = context.active;
            previousSingle = context.single;
            previousArray = context.array;
            previousArrayCount = context.arrayCount;
            previousIterable = context.iterable;
            context.active = true;
            context.single = input;
            context.array = inputs;
            context.arrayCount = Math.max(0, count);
            context.iterable = iterableInputs;
            return this;
        }

        @Override
        public void close() {
            context.active = previousActive;
            context.single = previousSingle;
            context.array = previousArray;
            context.arrayCount = previousArrayCount;
            context.iterable = previousIterable;
        }
    }
}
