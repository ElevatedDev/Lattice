package com.staticgraph.runtime.nativeaccess;

final class NativeTopologyNatives {
    static final int CPU_MASK_WORDS = 16;
    static final String ENABLED_PROPERTY = "lattice.native.enabled";
    static final String LIBRARY_PATH_PROPERTY = "lattice.native.library.path";
    private static final String LIBRARY_NAME = "static_topology_native";

    private static final LoadResult LOAD_RESULT = loadLibrary(System::getProperty, SystemNativeLoader.INSTANCE);

    private NativeTopologyNatives() {
    }

    static boolean loaded() {
        return LOAD_RESULT.loaded();
    }

    static String loadFailureMessage() {
        return LOAD_RESULT.failureMessage();
    }

    static void ensureLoaded() {
        if (!LOAD_RESULT.loaded()) {
            throw new NativeTopologyUnavailableException(
                "Native topology library '" + LIBRARY_NAME + "' is not loaded: " + LOAD_RESULT.failureMessage(),
                LOAD_RESULT.failure()
            );
        }
    }

    static LoadResult loadLibrary(final PropertyLookup properties, final NativeLoader loader) {
        final String enabled = properties.get(ENABLED_PROPERTY);
        if (enabled != null && "false".equalsIgnoreCase(enabled.trim())) {
            return LoadResult.unavailable("disabled by -" + propertySetting(ENABLED_PROPERTY, enabled), null);
        }

        final String configuredPath = properties.get(LIBRARY_PATH_PROPERTY);
        final String libraryPath = configuredPath == null ? "" : configuredPath.trim();
        try {
            if (libraryPath.isEmpty()) {
                loader.loadLibrary(LIBRARY_NAME);
                return LoadResult.success();
            }
            loader.load(libraryPath);
            return LoadResult.success();
        } catch (final UnsatisfiedLinkError | SecurityException error) {
            return LoadResult.unavailable(loadFailureMessage(libraryPath, error), error);
        }
    }

    private static String loadFailureMessage(final String libraryPath, final Throwable failure) {
        final String source = libraryPath.isEmpty()
            ? "System.loadLibrary(\"" + LIBRARY_NAME + "\")"
            : "System.load(\"" + libraryPath + "\") from -" + propertySetting(LIBRARY_PATH_PROPERTY, libraryPath);
        return source + " failed on " + platformName() + ": " + throwableMessage(failure);
    }

    private static String throwableMessage(final Throwable failure) {
        final String message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
    }

    private static String platformName() {
        return System.getProperty("os.name", "unknown-os") + "/" + System.getProperty("os.arch", "unknown-arch");
    }

    private static String propertySetting(final String key, final String value) {
        return "D" + key + "=" + value;
    }

    static native long nativeCapabilities0();

    static native int maxCpuCount0();

    static native int configuredCpuCount0();

    static native int onlineCpuCount0();

    static native int currentCpu0();

    static native int currentNumaNode0();

    static native int numaNodeOfCpu0(int cpu);

    static native int isCpuAllowed0(int cpu);

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

    record LoadResult(boolean loaded, Throwable failure, String failureMessage) {
        static LoadResult success() {
            return new LoadResult(true, null, "");
        }

        static LoadResult unavailable(final String failureMessage, final Throwable failure) {
            return new LoadResult(false, failure, failureMessage);
        }
    }

    @FunctionalInterface
    interface PropertyLookup {
        String get(String key);
    }

    interface NativeLoader {
        void load(String path);

        void loadLibrary(String libraryName);
    }

    private enum SystemNativeLoader implements NativeLoader {
        INSTANCE;

        @Override
        public void load(final String path) {
            System.load(path);
        }

        @Override
        public void loadLibrary(final String libraryName) {
            System.loadLibrary(libraryName);
        }
    }
}
