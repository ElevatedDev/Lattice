# v1.0.0 Baseline Snapshot

Mirror of `benchmarks/baseline/` at the time of the 1.0.0 release.

| File | Profile | Description |
| --- | --- | --- |
| `smoke-all.json` | 1×3s | Full smoke sweep (apples-to-apples + topology shape + Disruptor baseline + Phase4 + edge-pair). |
| `edge-pair.json` | 3×10s + 5×10s | Warmed steady-state edge primitives. |
| `all-benchmarks.json` | 1×3s | Full smoke; superset of smoke-all. |
| `topology.json` | 1×3s | TopologyShapeDisruptorBenchmark only. |
| `pipeline-fused-fullwarmup.json` | 3×10s + 5×10s | Warmed fused-pipeline measurement. |
| `phase4-topology.json` | 1×3s | Phase4 routing micros (broadcast / partition / dispatch / join). |
| `batch-topology.json` | 1×3s | Batch-stage topology micros. |

## Host

```
OS:   Ubuntu 24.04 (WSL2)
CPU:  Intel i7-7700 @ 3.6 GHz, 4c/8t
JDK:  Temurin 21.0.10
JVM:  -Xms2g -Xmx2g -XX:+AlwaysPreTouch -XX:+UseParallelGC
      -Dlattice.metrics.hotCounters=false
      -Dlattice.metrics.residence=false
      -Dlattice.metrics.stageHistograms=false
      -Dlattice.runtime.fusedLogicalEdgeMetrics=false
Native: not loaded (Java-only baseline)
```

See `../../linux-validation.md` for the procedure that produces
publication-grade numbers; the WSL2 baseline is for orientation only.
