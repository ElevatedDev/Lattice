# v1.0.0 Baseline Summary

Summary of the tracked [`benchmarks/baseline/`](../../../benchmarks/baseline/)
artifact set refreshed on 2026-04-29 for the 1.0.0 open-source hardening pass.

| File | Profile | Description |
| --- | --- | --- |
| `three-stage-vs-disruptor.json` / `.log` | 3 forks, 5x5s warmup + 8x5s measurement | Broad three-stage Lattice physical/inline-fused vs Disruptor physical/manual-fused matrix retained for audit history. |
| `three-stage-isolated-physical.json` / `.log` | 2 forks, 3x3s warmup + 5x3s measurement | Isolated Lattice physical three-stage vs Disruptor physical three-handler publish throughput. |
| `three-stage-isolated-fused-copy.json` / `.log` | 2 forks, 3x3s warmup + 5x3s measurement | Isolated Lattice inline-fused three-stage vs Disruptor manually fused copy-payload publish throughput. |
| `three-stage-isolated-reference.json` / `.log` | 2 forks, 3x3s warmup + 5x3s measurement | Isolated reference-payload and equal-call-site Lattice rows vs Disruptor manually fused reference publish throughput. |
| `optimal-path-completed.json` / `.log` | 3 forks, 5x5s warmup + 8x5s measurement | Completion-gated Lattice inline-fused path vs Disruptor busy-spin/manual-fused equivalent. |
| `lattice-core-basics.json` / `.log` | 1 fork, 3x5s warmup + 5x5s measurement | Source/sink paths, batched topology, routing/topology rows, and raw edge regression rows. |
| `lattice-placement.json` / `.log` | 1 fork, 3x5s warmup + 5x5s measurement | Portable placement subset, `pinning=false`, first-touch on/off, on-heap slots. |

## Validation Profile

```text
CPU:  Intel i7-7700 @ 3.6 GHz, 4c/8t, 1 NUMA node
JDK:  OpenJDK 21.0.10+7-Ubuntu-124.04
JVM:  -Xms2g -Xmx2g -XX:+AlwaysPreTouch -XX:+UseParallelGC
      -Dlattice.metrics.hotCounters=false
      -Dlattice.metrics.residence=false
      -Dlattice.metrics.stageHistograms=false
      -Dlattice.runtime.fusedLogicalEdgeMetrics=false
      -Dlattice.runtime.inlineDepthTracking=false
Native: not loaded; placement rows use pinning=false
```

## Figures

- [Three-stage publish throughput](../../assets/perf-pipeline.svg)
- [Lattice vs Disruptor ratios](../../assets/disruptor-comparison.svg)
- [End-to-end latency percentiles](../../assets/latency-percentiles.svg)

## Headline Rows

| Row | Lattice | Disruptor | Ratio |
| --- | ---: | ---: | ---: |
| Physical three-stage publish throughput | 27,660,948 ops/s | 26,377,465 ops/s | 1.05x |
| Inline/manual fused, copy payload | 61,838,846 ops/s | 45,888,659 ops/s | 1.35x |
| Manual fused reference payload, equal call-site | 92,094,463 ops/s | 44,045,374 ops/s | 2.09x |
| Completed optimal path | 29,903,291 ops/s | 4,742,326 ops/s | 6.31x |

The rows above deduplicate isolated and full-matrix repeats, then use the best
checked-in point estimate from each side for each published workload. The
`latticeManuallyFusedReference` row puts Lattice on Disruptor's call-site
footing and measures 2.09x the strongest logged Disruptor manually fused
reference row.

The completed optimal path waits for sink/handler completion for the same
sequence number. It should be used when comparing end-to-end operation
completion rather than enqueue/publish rate.

See [`../../../BENCHMARK_BASELINE.md`](../../../BENCHMARK_BASELINE.md) for the
full table and [`../../linux-validation.md`](../../linux-validation.md) for the
procedure used to reproduce the methodology on another Linux host.
