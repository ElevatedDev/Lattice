package io.github.elevateddev.lattice.nativeaccess;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class NativeTopologyNativesTest {

    @TempDir
    private Path temporaryRoot;

    @Test
    void disabledPropertySkipsNativeLoad() {
        final Map<String, String> properties = Map.of(NativeTopologyNatives.ENABLED_PROPERTY, "false");

        final NativeTopologyNatives.LoadResult result = NativeTopologyNatives.loadLibrary(
            properties::get,
            new FailingLoader()
        );

        assertFalse(result.loaded());
        assertNull(result.failure());
        assertTrue(result.failureMessage().contains("-Dlattice.native.enabled=false"));
    }

    @Test
    void configuredLibraryPathUsesExactPath() {
        final Map<String, String> properties = Map.of(
            NativeTopologyNatives.LIBRARY_PATH_PROPERTY,
            " C:\\native\\static_topology_native.dll "
        );
        final RecordingLoader loader = new RecordingLoader();

        final NativeTopologyNatives.LoadResult result = NativeTopologyNatives.loadLibrary(properties::get, loader);

        assertTrue(result.loaded());
        assertEquals("C:\\native\\static_topology_native.dll", loader.loadedPath);
        assertNull(loader.loadedLibraryName);
    }

    @Test
    void explicitLibraryPathBypassesBundledResourceLookup() {
        final Map<String, String> properties = Map.of(
            NativeTopologyNatives.LIBRARY_PATH_PROPERTY,
            "/opt/lattice/libstatic_topology_native.so"
        );
        final RecordingLoader loader = new RecordingLoader();

        final NativeTopologyNatives.LoadResult result = NativeTopologyNatives.loadLibrary(
            properties::get,
            loader,
            resourceName -> fail("resource lookup should be skipped"),
            "Linux",
            "amd64"
        );

        assertTrue(result.loaded());
        assertEquals("/opt/lattice/libstatic_topology_native.so", loader.loadedPath);
    }

    @Test
    void bundledResourceLoadsBeforeSystemLibraryFallback() throws IOException {
        final RecordingLoader loader = new RecordingLoader();
        final String resourceName = NativeTopologyNatives.bundledLibraryResourceName("Linux", "amd64");
        final byte[] libraryBytes = new byte[] {1, 2, 3, 4};

        final NativeTopologyNatives.LoadResult result = NativeTopologyNatives.loadLibrary(
            key -> null,
            loader,
            requested -> resourceName.equals(requested)
                ? new ByteArrayInputStream(libraryBytes)
                : null,
            "Linux",
            "amd64",
            temporaryRoot
        );

        assertTrue(result.loaded());
        assertTrue(loader.loadedPath.endsWith("libstatic_topology_native.so"));
        assertArrayEquals(libraryBytes, Files.readAllBytes(Path.of(loader.loadedPath)));
        assertNull(loader.loadedLibraryName);
    }

    @Test
    void bundledResourceFailureFallsBackToSystemLibrary() {
        final ResourceFirstThenSystemLoader loader = new ResourceFirstThenSystemLoader();
        final String resourceName = NativeTopologyNatives.bundledLibraryResourceName("Linux", "amd64");

        final NativeTopologyNatives.LoadResult result = NativeTopologyNatives.loadLibrary(
            key -> null,
            loader,
            requested -> resourceName.equals(requested)
                ? new ByteArrayInputStream(new byte[] {1, 2, 3, 4})
                : null,
            "Linux",
            "amd64"
        );

        assertTrue(result.loaded());
        assertTrue(loader.resourceLoadAttempted);
        assertEquals("static_topology_native", loader.loadedLibraryName);
    }

    @Test
    void missingBundledResourceFallsBackToSystemLibrary() {
        final RecordingLoader loader = new RecordingLoader();

        final NativeTopologyNatives.LoadResult result = NativeTopologyNatives.loadLibrary(
            key -> null,
            loader,
            requested -> null,
            "Linux",
            "amd64",
            temporaryRoot
        );

        assertTrue(result.loaded());
        assertNull(loader.loadedPath);
        assertEquals("static_topology_native", loader.loadedLibraryName);
    }

    @Test
    void emptyBundledResourceFallsBackToSystemLibrary() {
        final RecordingLoader loader = new RecordingLoader();
        final String resourceName = NativeTopologyNatives.bundledLibraryResourceName("Linux", "amd64");

        final NativeTopologyNatives.LoadResult result = NativeTopologyNatives.loadLibrary(
            key -> null,
            loader,
            requested -> resourceName.equals(requested)
                ? new ByteArrayInputStream(new byte[0])
                : null,
            "Linux",
            "amd64",
            temporaryRoot
        );

        assertTrue(result.loaded());
        assertNull(loader.loadedPath);
        assertEquals("static_topology_native", loader.loadedLibraryName);
    }

    @Test
    void bundledResourceNamesFollowSupportedPlatformLayout() {
        assertEquals(
            "META-INF/native/lattice/linux-x86_64/libstatic_topology_native.so",
            NativeTopologyNatives.bundledLibraryResourceName("Linux", "amd64")
        );
        assertEquals(
            "META-INF/native/lattice/linux-aarch64/libstatic_topology_native.so",
            NativeTopologyNatives.bundledLibraryResourceName("Linux", "aarch64")
        );
        assertEquals(
            "META-INF/native/lattice/windows-x86_64/static_topology_native.dll",
            NativeTopologyNatives.bundledLibraryResourceName("Windows 11", "x86_64")
        );
        assertEquals(
            "META-INF/native/lattice/macos-aarch64/libstatic_topology_native.dylib",
            NativeTopologyNatives.bundledLibraryResourceName("Mac OS X", "aarch64")
        );
        assertEquals(
            "META-INF/native/lattice/macos-x86_64/libstatic_topology_native.dylib",
            NativeTopologyNatives.bundledLibraryResourceName("Mac OS X", "x86_64")
        );
        assertNull(NativeTopologyNatives.bundledLibraryResourceName("Windows 11", "arm64"));
        assertNull(NativeTopologyNatives.bundledLibraryResourceName("Solaris", "sparc"));
    }

    @Test
    void libraryNameFailureIncludesPlatformAndCause() {
        final UnsatisfiedLinkError failure = new UnsatisfiedLinkError("missing from java.library.path");

        final NativeTopologyNatives.LoadResult result = NativeTopologyNatives.loadLibrary(
            key -> null,
            new ThrowingLoader(failure)
        );

        assertFalse(result.loaded());
        assertEquals(failure, result.failure());
        assertTrue(result.failureMessage().contains("System.loadLibrary(\"static_topology_native\")"));
        assertTrue(result.failureMessage().contains(System.getProperty("os.name")));
        assertTrue(result.failureMessage().contains(System.getProperty("os.arch")));
        assertTrue(result.failureMessage().contains("missing from java.library.path"));
    }

    @Test
    void pathFailureNamesConfiguredProperty() {
        final SecurityException failure = new SecurityException("denied");
        final Map<String, String> properties = new HashMap<>();
        properties.put(NativeTopologyNatives.LIBRARY_PATH_PROPERTY, "/opt/lattice/libstatic_topology_native.so");

        final NativeTopologyNatives.LoadResult result = NativeTopologyNatives.loadLibrary(
            properties::get,
            new ThrowingLoader(failure)
        );

        assertFalse(result.loaded());
        assertEquals(failure, result.failure());
        assertTrue(result.failureMessage().contains("System.load(\"/opt/lattice/libstatic_topology_native.so\")"));
        assertTrue(result.failureMessage().contains("-Dlattice.native.library.path=/opt/lattice/libstatic_topology_native.so"));
        assertTrue(result.failureMessage().contains("denied"));
    }

    private static final class RecordingLoader implements NativeTopologyNatives.NativeLoader {
        private String loadedPath;
        private String loadedLibraryName;

        @Override
        public void load(final String path) {
            loadedPath = path;
        }

        @Override
        public void loadLibrary(final String libraryName) {
            loadedLibraryName = libraryName;
        }
    }

    private static final class ResourceFirstThenSystemLoader implements NativeTopologyNatives.NativeLoader {
        private boolean resourceLoadAttempted;
        private String loadedLibraryName;

        @Override
        public void load(final String path) {
            resourceLoadAttempted = true;
            throw new UnsatisfiedLinkError("resource load failed");
        }

        @Override
        public void loadLibrary(final String libraryName) {
            loadedLibraryName = libraryName;
        }
    }

    private static final class ThrowingLoader implements NativeTopologyNatives.NativeLoader {
        private final Throwable failure;

        private ThrowingLoader(final Throwable failure) {
            this.failure = failure;
        }

        @Override
        public void load(final String path) {
            throwFailure();
        }

        @Override
        public void loadLibrary(final String libraryName) {
            throwFailure();
        }

        private void throwFailure() {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new AssertionError(failure);
        }
    }

    private static final class FailingLoader implements NativeTopologyNatives.NativeLoader {
        @Override
        public void load(final String path) {
            fail("native path load should be skipped");
        }

        @Override
        public void loadLibrary(final String libraryName) {
            fail("native library load should be skipped");
        }
    }
}
