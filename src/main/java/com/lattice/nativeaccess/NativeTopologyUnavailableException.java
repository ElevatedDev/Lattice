package com.lattice.nativeaccess;

/**
 * Raised when the static topology native library cannot be loaded or the host
 * platform does not implement the requested native operation.
 */
public final class NativeTopologyUnavailableException extends NativeTopologyException {
    public NativeTopologyUnavailableException(final String message) {
        super(message);
    }

    public NativeTopologyUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
