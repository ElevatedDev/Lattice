package io.github.elevateddev.lattice.nativeaccess;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

final class NativeTopologyNatives {
    static final int CPU_MASK_WORDS = 16;
    static final String ENABLED_PROPERTY = "lattice.native.enabled";
    static final String LIBRARY_PATH_PROPERTY = "lattice.native.library.path";
    private static final String LIBRARY_NAME = "static_topology_native";
    private static final String RESOURCE_PREFIX = "META-INF/native/lattice";

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
        return loadLibrary(
            properties,
            loader,
            NativeTopologyNatives::openBundledResource,
            System.getProperty("os.name", "unknown-os"),
            System.getProperty("os.arch", "unknown-arch")
        );
    }

    static LoadResult loadLibrary(
        final PropertyLookup properties,
        final NativeLoader loader,
        final ResourceLookup resources,
        final String osName,
        final String osArch
    ) {
        return loadLibrary(
            properties,
            loader,
            resources,
            osName,
            osArch,
            Path.of(System.getProperty("java.io.tmpdir"))
        );
    }

    static LoadResult loadLibrary(
        final PropertyLookup properties,
        final NativeLoader loader,
        final ResourceLookup resources,
        final String osName,
        final String osArch,
        final Path temporaryRoot
    ) {
        final String enabled = properties.get(ENABLED_PROPERTY);
        if (enabled != null && "false".equalsIgnoreCase(enabled.trim())) {
            return LoadResult.unavailable("disabled by -" + propertySetting(ENABLED_PROPERTY, enabled), null);
        }

        final String configuredPath = properties.get(LIBRARY_PATH_PROPERTY);
        final String libraryPath = configuredPath == null ? "" : configuredPath.trim();
        if (!libraryPath.isEmpty()) {
            try {
                loader.load(libraryPath);
                return LoadResult.success();
            } catch (final UnsatisfiedLinkError | SecurityException error) {
                return LoadResult.unavailable(exactPathFailureMessage(libraryPath, osName, osArch, error), error);
            }
        }

        final String resourceName = bundledLibraryResourceName(osName, osArch);
        final StringBuilder failures = new StringBuilder();
        Throwable failure = null;
        if (resourceName != null) {
            try {
                final Path extracted = extractBundledLibrary(resourceName, resources, temporaryRoot);
                loader.load(extracted.toAbsolutePath().toString());
                return LoadResult.success();
            } catch (final IOException | UnsatisfiedLinkError | SecurityException error) {
                failure = error;
                failures.append("bundled resource ")
                    .append(resourceName)
                    .append(" failed on ")
                    .append(platformName(osName, osArch))
                    .append(": ")
                    .append(throwableMessage(error))
                    .append("; ");
            }
        } else {
            failures.append("no bundled native resource for ")
                .append(platformName(osName, osArch))
                .append("; ");
        }

        try {
            loader.loadLibrary(LIBRARY_NAME);
            return LoadResult.success();
        } catch (final UnsatisfiedLinkError | SecurityException error) {
            failure = error;
            failures.append(loadLibraryFailureMessage(osName, osArch, error));
            return LoadResult.unavailable(failures.toString(), failure);
        }
    }

    static String bundledLibraryResourceName(final String osName, final String osArch) {
        final String os = normalizeOs(osName);
        final String arch = normalizeArch(osArch);
        if (os == null || arch == null) {
            return null;
        }
        if (!isBundledPlatform(os, arch)) {
            return null;
        }

        final String libraryFileName = switch (os) {
            case "linux" -> "libstatic_topology_native.so";
            case "windows" -> "static_topology_native.dll";
            case "macos" -> "libstatic_topology_native.dylib";
            default -> null;
        };
        if (libraryFileName == null) {
            return null;
        }
        return RESOURCE_PREFIX + "/" + os + "-" + arch + "/" + libraryFileName;
    }

    private static boolean isBundledPlatform(final String os, final String arch) {
        return switch (os) {
            case "linux", "macos" -> "x86_64".equals(arch) || "aarch64".equals(arch);
            case "windows" -> "x86_64".equals(arch);
            default -> false;
        };
    }

    private static String normalizeOs(final String osName) {
        final String value = osName == null ? "" : osName.toLowerCase(java.util.Locale.ROOT);
        if (value.contains("linux")) {
            return "linux";
        }
        if (value.contains("windows")) {
            return "windows";
        }
        if (value.contains("mac") || value.contains("darwin")) {
            return "macos";
        }
        return null;
    }

    private static String normalizeArch(final String osArch) {
        final String value = osArch == null
            ? ""
            : osArch.toLowerCase(java.util.Locale.ROOT).replace("-", "").replace("_", "");
        return switch (value) {
            case "amd64", "x8664" -> "x86_64";
            case "aarch64", "arm64" -> "aarch64";
            default -> null;
        };
    }

    private static InputStream openBundledResource(final String resourceName) {
        final ClassLoader loader = NativeTopologyNatives.class.getClassLoader();
        return loader == null
            ? ClassLoader.getSystemResourceAsStream(resourceName)
            : loader.getResourceAsStream(resourceName);
    }

    private static Path extractBundledLibrary(
        final String resourceName,
        final ResourceLookup resources,
        final Path temporaryRoot
    ) throws IOException {
        try (InputStream input = resources.open(resourceName)) {
            if (input == null) {
                throw new IOException("resource not found");
            }
            final byte[] bytes = input.readAllBytes();
            if (bytes.length == 0) {
                throw new IOException("resource is empty");
            }

            final String digest = sha256(bytes);
            final String libraryFileName = resourceName.substring(resourceName.lastIndexOf('/') + 1);
            final Path targetDirectory = Path.of(
                temporaryRoot.toString(),
                "lattice-native",
                digest.substring(0, 16)
            );
            Files.createDirectories(targetDirectory);
            final Path target = targetDirectory.resolve(libraryFileName);
            if (!matchesDigest(target, digest)) {
                final Path temporary = Files.createTempFile(targetDirectory, libraryFileName, ".tmp");
                try {
                    Files.write(temporary, bytes);
                    moveExtractedLibrary(temporary, target);
                } catch (final IOException error) {
                    if (!matchesDigest(target, digest)) {
                        throw error;
                    }
                } finally {
                    Files.deleteIfExists(temporary);
                }
            }
            return target;
        }
    }

    private static boolean matchesDigest(final Path path, final String digest) throws IOException {
        return Files.isRegularFile(path) && digest.equals(sha256(Files.readAllBytes(path)));
    }

    private static void moveExtractedLibrary(final Path temporary, final Path target) throws IOException {
        try {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (final AtomicMoveNotSupportedException unsupported) {
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static String sha256(final byte[] bytes) throws IOException {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            final byte[] hashed = digest.digest(bytes);
            final StringBuilder builder = new StringBuilder(hashed.length * 2);
            for (final byte value : hashed) {
                builder.append(String.format("%02x", value));
            }
            return builder.toString();
        } catch (final NoSuchAlgorithmException error) {
            throw new IOException("SHA-256 is not available", error);
        }
    }

    private static String exactPathFailureMessage(
        final String libraryPath,
        final String osName,
        final String osArch,
        final Throwable failure
    ) {
        return "System.load(\"" + libraryPath + "\") from -" + propertySetting(LIBRARY_PATH_PROPERTY, libraryPath)
            + " failed on " + platformName(osName, osArch) + ": " + throwableMessage(failure);
    }

    private static String loadLibraryFailureMessage(
        final String osName,
        final String osArch,
        final Throwable failure
    ) {
        return "System.loadLibrary(\"" + LIBRARY_NAME + "\") failed on "
            + platformName(osName, osArch) + ": " + throwableMessage(failure);
    }

    private static String throwableMessage(final Throwable failure) {
        final String message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
    }

    private static String platformName(final String osName, final String osArch) {
        return osName + "/" + osArch;
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

    static native int pinCurrentThreadToNumaNode0(int numaNode);

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

    @FunctionalInterface
    interface ResourceLookup {
        InputStream open(String resourceName) throws IOException;
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
