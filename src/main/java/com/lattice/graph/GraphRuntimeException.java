package com.lattice.graph;

public final class GraphRuntimeException extends RuntimeException {

    public GraphRuntimeException(final String message) {
        super(message);
    }

    public GraphRuntimeException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
