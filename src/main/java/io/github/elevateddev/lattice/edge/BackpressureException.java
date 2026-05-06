package io.github.elevateddev.lattice.edge;

/**
 * Thrown when an edge cannot accept an item under a fail-fast pressure policy.
 */
public final class BackpressureException extends RuntimeException {

    /**
     * Creates a backpressure exception with a message.
     */
    public BackpressureException(final String message) {
        super(message);
    }
}
