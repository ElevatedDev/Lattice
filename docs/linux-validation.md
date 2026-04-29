# Linux Validation Notes

Publication-grade Lattice benchmarks must be produced on Linux with the
native backend loaded. This page documents the procedure.

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
  -Dlattice.metrics.hotCounters=false
  -Dlattice.metrics.residence=false
  -Dlattice.metrics.stageHistograms=false
  -Dlattice.runtime.fusedLogicalEdgeMetrics=false
  ```

## Run

```bash
./gradlew nativeBuildRelease
./gradlew jmh \
  -PjmhInclude='com\.lattice\.benchmark\..*(Disruptor|OptimalPath).*' \
  -PjmhJvmArgs='-Djava.library.path=native/static-topology-native/target/release'
```

For publication steady-state numbers use at least 3 x 10 s warmup + 5 x 10 s
measurement. Smoke runs (1 x 3 s) are useful for
pre-merge gating but should never be cited as performance evidence in
documentation.

## What To Capture

For every benchmark JSON checked into `docs/benchmark-results/<version>/`:

- The JMH JSON output verbatim.
- A `host.md` next to it with: `lscpu`, `uname -a`, JDK version, Gradle
  version, kernel cmdline, microcode, isolated CPU set, the exact `taskset`
  invocation if used, and whether the native library was loaded.
- A short `notes.md` with anything anomalous (thermal throttling, shared
  hardware, etc.).

This procedure is the only one that produces numbers we are willing to put
behind a "vs Disruptor" claim in the README. Smoke results from a developer
laptop (including the `benchmarks/baseline/` set captured on WSL2) are
explicitly labeled as orientation, not evidence.
