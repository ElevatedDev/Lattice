package io.github.elevateddev.lattice.internal.runtime;

import io.github.elevateddev.lattice.stage.Output;
import java.time.Duration;

final class EdgeOutput<T> implements Output<T> {

    private final EdgeSender sender;

    EdgeOutput(final EdgeSender sender) {
        this.sender = sender;
    }

    @Override
    public void push(final T item) {
        sender.emit(item);
    }

    @Override
    public boolean push(final T item, final Duration timeout) {
        return sender.emit(item, timeout.toNanos());
    }

    @Override
    public boolean tryPush(final T item) {
        return sender.tryEmit(item);
    }
}
