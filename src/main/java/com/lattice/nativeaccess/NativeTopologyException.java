package com.lattice.nativeaccess;

/**
 * Raised when a native topology operation is available but the operating system
 * rejects it, for example because a CPU is outside the process cpuset.
 */
public class NativeTopologyException extends RuntimeException {
    public NativeTopologyException(final String message) {
        super(message);
    }

    public NativeTopologyException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
