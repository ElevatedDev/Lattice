package com.lattice.edge;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

public final class OverflowPolicy {

    private final OverflowKind kind;
    private final Duration timeout;
    private final Function<Object, ?> coalescingKey;
    private final String redirectTarget;

    private OverflowPolicy(final OverflowKind kind, final Duration timeout) {
        this(kind, timeout, null, null);
    }

    private OverflowPolicy(
        final OverflowKind kind,
        final Duration timeout,
        final Function<Object, ?> coalescingKey,
        final String redirectTarget
    ) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.timeout = timeout;
        this.coalescingKey = coalescingKey;
        this.redirectTarget = redirectTarget;
    }

    public static OverflowPolicy block() {
        return new OverflowPolicy(OverflowKind.BLOCK, null);
    }

    public static OverflowPolicy failFast() {
        return new OverflowPolicy(OverflowKind.FAIL_FAST, null);
    }

    public static OverflowPolicy dropNewest() {
        return dropLatest();
    }

    public static OverflowPolicy dropLatest() {
        return new OverflowPolicy(OverflowKind.DROP_LATEST, null);
    }

    public static OverflowPolicy dropOldest() {
        return new OverflowPolicy(OverflowKind.DROP_OLDEST, null);
    }

    @SuppressWarnings("unchecked")
    public static <T> OverflowPolicy coalesceBy(final Function<? super T, ?> keyExtractor) {
        Objects.requireNonNull(keyExtractor, "keyExtractor");
        return new OverflowPolicy(OverflowKind.COALESCE, null, item -> keyExtractor.apply((T) item), null);
    }

    public static OverflowPolicy redirectTo(final String targetNode) {
        final String target = Objects.requireNonNull(targetNode, "targetNode").trim();
        if (target.isEmpty()) {
            throw new IllegalArgumentException("targetNode must not be blank");
        }
        return new OverflowPolicy(OverflowKind.REDIRECT, null, null, target);
    }

    public static OverflowPolicy blockFor(final Duration timeout) {
        final Duration value = Objects.requireNonNull(timeout, "timeout");
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        return new OverflowPolicy(OverflowKind.BLOCK_FOR, value);
    }

    public OverflowKind kind() {
        return kind;
    }

    public Duration timeout() {
        return timeout;
    }

    public Function<Object, ?> coalescingKey() {
        return coalescingKey;
    }

    public String redirectTarget() {
        return redirectTarget;
    }

    public enum OverflowKind {
        BLOCK,
        FAIL_FAST,
        BLOCK_FOR,
        DROP_LATEST,
        DROP_OLDEST,
        COALESCE,
        REDIRECT
    }
}
