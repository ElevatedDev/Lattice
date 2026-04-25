package com.staticgraph.runtime.nativeaccess;

final class NativeTopologyNatives {
    static final int CPU_MASK_WORDS = 16;
    private static final String LIBRARY_NAME = "static_topology_native";

    private static final UnsatisfiedLinkError LOAD_ERROR = loadLibrary();

    private NativeTopologyNatives() {
    }

    static boolean loaded() {
        return LOAD_ERROR == null;
    }

    static String loadFailureMessage() {
        if (LOAD_ERROR == null) {
            return "";
        }
        final String message = LOAD_ERROR.getMessage();
        return message == null || message.isBlank() ? LOAD_ERROR.getClass().getSimpleName() : message;
    }

    static void ensureLoaded() {
        if (LOAD_ERROR != null) {
            throw new NativeTopologyUnavailableException(
                "Native topology library '" + LIBRARY_NAME + "' is not loaded",
                LOAD_ERROR
            );
        }
    }

    private static UnsatisfiedLinkError loadLibrary() {
        try {
            System.loadLibrary(LIBRARY_NAME);
            return null;
        } catch (final UnsatisfiedLinkError error) {
            return error;
        }
    }

    static native long nativeCapabilities0();

    static native int maxCpuCount0();

    static native int configuredCpuCount0();

    static native int onlineCpuCount0();

    static native int currentCpu0();

    static native int currentNumaNode0();

    static native int numaNodeOfCpu0(int cpu);

    static native int pinCurrentThreadToCpu0(int cpu);

    static native int pinCurrentThreadToCpuMask0(
        long word0,
        long word1,
        long word2,
        long word3,
        long word4,
        long word5,
        long word6,
        long word7,
        long word8,
        long word9,
        long word10,
        long word11,
        long word12,
        long word13,
        long word14,
        long word15
    );

    static native int setLocalAllocationPolicy0();

    static native int firstTouchMemory0(long address, long bytes);
}
