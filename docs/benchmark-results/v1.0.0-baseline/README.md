# v1.0.0 Baseline Summary

Summary of the tracked [`benchmarks/baseline/`](../../../benchmarks/baseline/)
artifact set refreshed on 2026-04-29 for the 1.0.0 open-source hardening pass.

| File | Profile | Description |
| --- | --- | --- |
| `three-stage-vs-disruptor.json` / `.log` | 3 forks, 5x5s warmup + 8x5s measurement | Three-stage Lattice physical/inline-fused vs Disruptor physical/manual-fused publish throughput. |
| `optimal-path-completed.json` / `.log` | 3 forks, 5x5s warmup + 8x5s measurement | Completion-gated Lattice inline-fused path vs Disruptor busy-spin/manual-fused equivalent. |
| `lattice-core-basics.json` / `.log` | 1 fork, 3x5s warmup + 5x5s measurement | Raw edge sanity, SPSC source paths, batched topology, and representative routing rows. |
| `lattice-edge-pair-mpsc-ingress.json` / `.log` | 1 fork, 3x5s warmup + 5x5s measurement | SPSC/MPSC edge-pair groups plus 4-producer MPSC ingress with latency. |
| `lattice-placement.json` / `.log` | 1 fork, 3x5s warmup + 5x5s measurement | Portable placement subset, `pinning=false`, first-touch on/off, on-heap slots. |

## Host

```text
OS:   Ubuntu on WSL2, Linux 6.6.87.2-microsoft-standard-WSL2
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

## Headline Rows

| Row | Score |
| --- | ---: |
| Lattice inline-fused three-stage publish throughput | 51,037,862 ops/s |
| Lattice inline-fused three-stage, reference framing (same wiring) | 49,783,065 ops/s |
| Lattice manually fused, reference payload (1 stage, 3 increments inline) | 92,094,463 ops/s |
| Disruptor manually fused three-stage, copy payload | 45,888,659 ops/s |
| Disruptor manually fused three-stage, reference payload | 44,045,374 ops/s |
| Lattice completed optimal path | 29,903,291 ops/s |
| Disruptor completed optimal path | 4,742,326 ops/s |

Lattice's inline-fused path already passes the payload by reference; the
difference between the 3-stage Lattice rows (~50M ops/s) and the
`latticeManuallyFusedReference` row (~92M ops/s) is call-site shape (3 logical
stages vs 1 collapsed handler), not payload semantics. The
`latticeManuallyFusedReference` row puts Lattice on Disruptor's call-site
footing and measures 2.09x the Disruptor manually fused reference row on this
WSL2 host.

The completed optimal path waits for sink/handler completion for the same
sequence number. It should be used when comparing end-to-end operation
completion rather than enqueue/publish rate.

See [`../../../BENCHMARK_BASELINE.md`](../../../BENCHMARK_BASELINE.md) for the
full table and [`../../linux-validation.md`](../../linux-validation.md) for the
procedure that produces publication-grade Linux numbers. The WSL2 baseline is
for orientation only.
