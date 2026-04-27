# v1.0.0 Baseline Summary

Summary of the tracked `benchmarks/baseline/` notes at the time of the 1.0.0
release hardening pass.

| File | Profile | Description |
| --- | --- | --- |
| `post-aggressive-opts.log` | 3x5s warmup + 5x5s measurement | Warmed apples-to-apples pipeline and SPSC preallocated comparison after the aggressive optimization pass. |
| `post-inline-fusion.log` | 3x5s warmup + 5x5s measurement | Warmed apples-to-apples comparison after trusted emit and inline source fusion work. |

## Host

```text
OS:   Ubuntu 24.04 (WSL2)
CPU:  Intel i7-7700 @ 3.6 GHz, 4c/8t
JDK:  Temurin 21.x
JVM:  -Xms2g -Xmx2g -XX:+AlwaysPreTouch -XX:+UseParallelGC
      -Dlattice.metrics.hotCounters=false
      -Dlattice.metrics.residence=false
      -Dlattice.metrics.stageHistograms=false
      -Dlattice.runtime.fusedLogicalEdgeMetrics=false
Native: not loaded (Java-only baseline)
```

See [`../../linux-validation.md`](../../linux-validation.md) for the procedure
that produces publication-grade numbers; the WSL2 baseline is for orientation
only.
