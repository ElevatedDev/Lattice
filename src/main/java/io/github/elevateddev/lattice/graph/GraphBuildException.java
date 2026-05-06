package io.github.elevateddev.lattice.graph;

/**
 * Thrown when a graph plan cannot be compiled or validated.
 */
public final class GraphBuildException extends RuntimeException {

    /**
     * Creates a build exception with a validation message.
     */
    public GraphBuildException(final String message) {
        super(message);
    }
}
