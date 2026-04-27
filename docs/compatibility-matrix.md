# Compatibility Matrix

## Java

| JDK | Status | Notes |
| --- | --- | --- |
| 21 | **Supported** | Build baseline. JPMS module name `com.lattice`. |
| 22 | Best-effort | Compiles and tests cleanly on `--release 21`. |
| 23 | Best-effort | Same. |
| 24 | Best-effort | Same. |
| 25 (LTS) | Planned for 1.1 | Validation pending. |
| 26 EA | Tracking | No known blockers. |
| ≤ 20 | **Unsupported** | Lattice uses Java 21 language and APIs. |

The published jar contains:

- `module-info.class` for `com.lattice`.
- `Automatic-Module-Name: com.lattice` as a fallback.
- Reproducible archive flags (`preserveFileTimestamps=false`,
  `reproducibleFileOrder=true`).

## Operating Systems

| OS | Java runtime | Native backend |
| --- | --- | --- |
| Linux x86_64 | Supported | Supported (`libstatic_topology_native.so`). |
| Linux aarch64 | Supported | Supported (build from source). |
| macOS (Intel/Apple Silicon) | Supported | Supported (`libstatic_topology_native.dylib`); affinity APIs are advisory. |
| Windows x86_64 | Supported | Build-only (`static_topology_native.dll`); affinity APIs are best-effort. |
| Other POSIX | Best-effort | Build-from-source. |

## Disruptor

Disruptor 4.0.0 is on the **JMH classpath only**. It is not a runtime
dependency of the published Lattice jar and never appears on a consumer
classpath.

## HdrHistogram

HdrHistogram 2.2.2 is a runtime dependency, used for optional stage latency
histograms. The hot path does not call into HdrHistogram unless histograms
are explicitly enabled.

## Versioning Policy

- SemVer 2.0.0.
- Stable contracts (the 17 types listed in repository
  [CONTRIBUTING.md](https://github.com/ElevatedDev/Lattice/blob/main/CONTRIBUTING.md)
  and the stable packages listed in [API Reference](api.md)) cannot break in
  MINOR or PATCH releases after 1.0.0.
- Experimental surfaces listed in [API Reference](api.md) may break in MINOR.
- Non-exported `com.lattice.internal.*` packages may break at any time; do not
  depend on them.
- The native library's basename `static_topology_native` is part of the
  contract and only changes on MAJOR.
- Raising the JDK floor requires a MAJOR bump.
