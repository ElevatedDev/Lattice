package com.lattice.routing;

import java.util.Objects;
import java.util.function.Function;

public final class BroadcastSpec<T> {

    private final BroadcastKind kind;
    private final Function<? super T, ? extends T> copier;
    private final boolean isolateSlowBranches;

    private BroadcastSpec(
        final BroadcastKind kind,
        final Function<? super T, ? extends T> copier,
        final boolean isolateSlowBranches
    ) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.copier = copier;
        this.isolateSlowBranches = isolateSlowBranches;
    }

    public static <T> BroadcastSpec<T> copy() {
        return new BroadcastSpec<>(BroadcastKind.COPY, null, false);
    }

    public static <T> BroadcastSpec<T> copy(final Function<? super T, ? extends T> copier) {
        return new BroadcastSpec<>(BroadcastKind.COPY, Objects.requireNonNull(copier, "copier"), false);
    }

    public static <T> BroadcastSpec<T> slabHandles() {
        return new BroadcastSpec<>(BroadcastKind.SLAB_HANDLE, null, false);
    }

    public BroadcastSpec<T> withBranchIsolation() {
        return new BroadcastSpec<>(kind, copier, true);
    }

    public BroadcastKind kind() {
        return kind;
    }

    public Function<? super T, ? extends T> copier() {
        return copier;
    }

    public boolean isolateSlowBranches() {
        return isolateSlowBranches;
    }

    public enum BroadcastKind {
        COPY,
        SLAB_HANDLE
    }
}
