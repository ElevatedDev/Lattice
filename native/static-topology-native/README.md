# Static Topology Native JNI Backend

This module contains the Rust JNI backend that the Java runtime already loads
through `System.loadLibrary("static_topology_native")`.

The Java wrapper stays in:

```text
src/main/java/com/staticgraph/runtime/nativeaccess
```

The native implementation now lives in-repo:

```text
native/static-topology-native
```

## Current Backend Choice

The shipped native backend is JNI, not FFM/jextract.

That is deliberate for the current Java 21 baseline:

- JNI is stable and does not require preview flags.
- The runtime uses native access only during worker bootstrap.
- No native call belongs in the per-message hot path.

An FFM/jextract backend is still a gap relative to the longer-term plan. It is
not implemented in this repository today.

## What It Exposes

The shared library exports JNI methods for:

- CPU affinity for a single CPU or explicit CPU mask;
- current CPU and current NUMA node observation;
- CPU-to-NUMA lookup through Linux sysfs;
- local NUMA allocation policy via `set_mempolicy(MPOL_LOCAL)` where supported;
- first-touching an already allocated native memory range page by page.

On non-Linux or unsupported targets, the crate still builds but reports
`ENOSYS`-style unavailability through the Java wrapper.

## Build

Direct Cargo build:

```bash
cd native/static-topology-native
cargo build --release
```

Gradle helper from the repo root:

```bash
./gradlew nativeBuildRelease
```

On Windows hosts using Rust's default MSVC toolchain, Cargo also needs Visual
C++ Build Tools on `PATH` so `link.exe` is available.

Linux release artifact:

```text
native/static-topology-native/target/release/libstatic_topology_native.so
```

Windows and macOS can build host-native shared libraries, but the production
placement/NUMA behavior is Linux-specific.

## Run With Java

Point `java.library.path` at the Rust target directory that contains the shared
library:

```bash
java -Djava.library.path=native/static-topology-native/target/release ...
```

The Java wrapper name is already fixed to `static_topology_native`, so the
shared library basename must stay aligned with that.

## Runtime Notes

- Default behavior is fallback: placement failures degrade to metrics and
  diagnostics instead of crashing startup.
- `-Dlattice.placement.strict=true` changes that behavior and fails graph
  startup when requested placement or local memory policy setup cannot be
  applied.
- Placement validation is currently surfaced through `GraphPlan` and
  `StageMetrics`, not through a separate startup report artifact.
- First-touch placement is controlled by `-Dlattice.firstTouch.enabled=true|false`.
