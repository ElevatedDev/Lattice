# Linux Benchmark Validation Notes

Use this procedure when reproducing the checked-in Lattice benchmark
methodology on a dedicated Linux host, especially when placement, affinity, or
NUMA behavior is part of the claim.

## Host Requirements

- Bare-metal Linux (or a hypervisor with CPU pinning), kernel >= 5.15.
- At least 8 isolated cores via `isolcpus=` on the kernel command line, plus
  `nohz_full=` and `rcu_nocbs=` covering the same range.
- `cpupower frequency-set -g performance` on every isolated core.
- THP set to `madvise` or `never`; SMT disabled or explicitly accounted for.
- The native library built with `cargo build --release` and present on
  `java.library.path`.

## JVM

- JDK 21 Temurin (the Gradle toolchain default).
- Recommended JVM args (already set in `build.gradle` for the JMH suite):

  ```text
  -Xms2g -Xmx2g
  -XX:+AlwaysPreTouch
  -XX:+UnlockDiagnosticVMOptions
  -XX:+UseParallelGC
  ```

  Graph runtime behavior is configured per benchmark graph through
  `FusionSpec`, `MetricsSpec`, `GraphPlacementSpec`, and `DiagnosticsSpec`,
  not through process-global runtime properties.

## Run

```bash
./gradlew nativeBuildRelease
./gradlew jmh \
  -PjmhInclude='com\.lattice\.benchmark\..*(Disruptor|OptimalPath).*' \
  -PjmhJvmArgs='-Djava.library.path=native/static-topology-native/target/release'
```

For steady-state comparison numbers use at least the checked-in head-to-head
profile: 3 forks, 5 x 5 s warmup, and 8 x 5 s measurement. Longer campaigns
such as 3 x 10 s warmup and 5 x 10 s measurement are appropriate for release
attachments and hardware-specific reports.

## What To Capture

For every benchmark JSON checked into `docs/benchmark-results/<version>/`:

- The JMH JSON output verbatim.
- A `host.md` next to it with: `lscpu`, `uname -a`, JDK version, Gradle
  version, kernel cmdline, microcode, isolated CPU set, the exact `taskset`
  invocation if used, and whether the native library was loaded.
- A short `notes.md` with anything anomalous (thermal throttling, shared
  hardware, etc.).

Use the same payload model, completion semantics, observability toggles, JVM
flags, and Disruptor wait strategy as the checked-in baseline when extending a
"vs Disruptor" claim to a new host.
