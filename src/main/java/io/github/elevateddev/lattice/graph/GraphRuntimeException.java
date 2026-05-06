package io.github.elevateddev.lattice.graph;

/**
 * Thrown when a built graph rejects an invalid runtime operation.
 */
public final class GraphRuntimeException extends RuntimeException {

    /**
     * Creates a runtime exception with a message.
     */
    public GraphRuntimeException(final String message) {
        super(message);
    }

    /**
     * Creates a runtime exception with a message and cause.
     */
    public GraphRuntimeException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
