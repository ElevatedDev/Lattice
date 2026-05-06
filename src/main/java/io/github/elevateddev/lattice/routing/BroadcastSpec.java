package io.github.elevateddev.lattice.routing;

import java.util.Objects;
import java.util.function.Function;

/**
 * Configures a broadcast routing stage.
 * <p>
 * Broadcast sends each input to every outgoing branch. Copy broadcasts either
 * pass the original reference or invoke a caller-supplied copier per branch.
 * Slab-handle broadcasts retain and release shared handles instead of copying
 * payloads.
 *
 * @param <T> message type
 */
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

    /**
     * Broadcasts the same object reference to every branch.
     */
    public static <T> BroadcastSpec<T> copy() {
        return new BroadcastSpec<>(BroadcastKind.COPY, null, false);
    }

    /**
     * Broadcasts branch-local copies produced by the supplied copier.
     */
    public static <T> BroadcastSpec<T> copy(final Function<? super T, ? extends T> copier) {
        return new BroadcastSpec<>(BroadcastKind.COPY, Objects.requireNonNull(copier, "copier"), false);
    }

    /**
     * Broadcasts retained slab handles to every branch.
     */
    public static <T> BroadcastSpec<T> slabHandles() {
        return new BroadcastSpec<>(BroadcastKind.SLAB_HANDLE, null, false);
    }

    /**
     * Returns a copy that records and isolates slow-branch pressure when the
     * runtime can do so.
     */
    public BroadcastSpec<T> withBranchIsolation() {
        return new BroadcastSpec<>(kind, copier, true);
    }

    /**
     * Returns the broadcast mode.
     */
    public BroadcastKind kind() {
        return kind;
    }

    /**
     * Returns the branch copier for copy broadcasts, or {@code null} when the
     * original reference is reused.
     */
    public Function<? super T, ? extends T> copier() {
        return copier;
    }

    /**
     * Returns whether slow-branch isolation is requested.
     */
    public boolean isolateSlowBranches() {
        return isolateSlowBranches;
    }

    /**
     * Broadcast delivery modes.
     */
    public enum BroadcastKind {
        /**
         * Send object references or copied payloads.
         */
        COPY,
        /**
         * Retain and fan out slab handles.
         */
        SLAB_HANDLE
    }
}
