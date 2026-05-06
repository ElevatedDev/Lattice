# Compatibility Matrix

## Java

| JDK | Status | Notes |
| --- | --- | --- |
| 21 | **Supported** | Build baseline. JPMS module name `io.github.elevateddev.lattice`. |
| 22 | Best-effort | Compiles and tests cleanly on `--release 21`. |
| 23 | Best-effort | Same. |
| 24 | Best-effort | Same. |
| 25 (LTS) | Planned for 1.1 | Validation pending. |
| 26 | Tracking | GA on 2026-03-17; validation pending, no known blockers. |
| <= 20 | **Unsupported** | Lattice uses Java 21 language and APIs. |

The release jar contains:

- `module-info.class` for `io.github.elevateddev.lattice`.
- `Automatic-Module-Name: io.github.elevateddev.lattice` as a fallback.
- Bundled native libraries under `META-INF/native/lattice/<os>-<arch>/` when
  built by the release workflow.
- Reproducible archive flags (`preserveFileTimestamps=false`,
  `reproducibleFileOrder=true`).

## Operating Systems

| OS | Java runtime | Native backend |
| --- | --- | --- |
| Linux x86_64 | Supported | Supported (`libstatic_topology_native.so`): CPU affinity, current CPU, NUMA query, local memory policy, first-touch. |
| Linux aarch64 | Supported | Supported (build from source): CPU affinity, current CPU, NUMA query, local memory policy, first-touch. |
| macOS (Intel/Apple Silicon) | Supported | Partial (`libstatic_topology_native.dylib`): CPU counts and first-touch. Affinity and NUMA operations report unavailable. |
| Windows x86_64 | Supported | Partial (`static_topology_native.dll`): processor-group-aware CPU counts/current CPU, thread affinity within supported processor groups, first-touch. NUMA/local memory policy report unavailable. |
| Other POSIX | Best-effort | Stub JNI exports build where the target supports them; placement operations report unavailable. |

Native placement features are capability-gated at runtime. A host-native shared
library does not imply Linux-equivalent NUMA behavior; check
`NativeTopology.capabilities()` or `GraphMetrics.placementReport()` before
making placement or benchmark claims for a platform.

The Java loader uses this order: `-Dlattice.native.enabled=false` disables
native loading, `-Dlattice.native.library.path=...` loads an exact file, bundled
jar resources are tried next, and `System.loadLibrary("static_topology_native")`
is the final fallback.

On Linux, single-CPU pinning is exact. CPU-set pinning is any-of placement: the
native backend intersects the requested set with the worker thread's current
allowed affinity before applying it. This keeps Lattice aligned with cgroups,
`taskset`, and service managers; an empty effective set is reported as a
placement failure.

## Native Build Toolchain

The Rust JNI backend uses Rust edition 2024 and requires Rust/Cargo 1.85 or
newer. Java-only consumers do not need Rust.

## Disruptor

Disruptor 4.0.0 is on the **JMH classpath only**. It is not a runtime
dependency of the Lattice release jar and never appears on a consumer
classpath.

## HdrHistogram

HdrHistogram 2.2.2 is an API dependency because public metrics accessors return
defensive `Histogram` copies. The hot path does not call into HdrHistogram
unless histograms are explicitly enabled.

## Versioning Policy

- SemVer 2.0.0.
- Stable contracts (the public stable-surface types listed in repository
  [CONTRIBUTING.md](../CONTRIBUTING.md)
  and the stable packages listed in [API Reference](api.md)) cannot break in
  MINOR or PATCH releases after 1.0.0.
- Experimental surfaces listed in [API Reference](api.md) may break in MINOR.
- Non-exported `io.github.elevateddev.lattice.internal.*` packages may break at any time; do not
  depend on them.
- The native library's basename `static_topology_native` is part of the
  contract and only changes on MAJOR.
- Raising the JDK floor requires a MAJOR bump.
