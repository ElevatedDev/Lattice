# 2026-05-02 Per-Graph Runtime API Refresh

This snapshot refreshes the public comparison figures after moving fusion,
metrics, placement, and diagnostics controls from process-global runtime flags
to per-graph builder specs.

See the [device comparison page](../../devices.md) for this host alongside the
older i7 publication baseline.

## Validation Profile

| Property | Value |
| --- | --- |
| Host | WSL2 Linux `6.6.87.2-microsoft-standard-WSL2` |
| CPU | Intel Core i9-14900HX, 16 cores / 32 threads, 1 NUMA node |
| JDK | OpenJDK 21.0.10+7-Ubuntu-124.04 |
| JMH | 1.36 |
| Disruptor | 4.0.0 on the JMH classpath only |
| Native backend | Loaded for Lattice strict-topology and explicit CPU-pinned latency rows |
| JVM flags | `-Xms2g -Xmx2g -XX:+AlwaysPreTouch -XX:+UnlockDiagnosticVMOptions -XX:+UseParallelGC` |

## Artifacts

Raw JMH JSON and stdout logs are checked in under
[`benchmarks/baseline/`](../../../benchmarks/baseline/):

| Artifact | Profile | Purpose |
| --- | --- | --- |
| `three-stage-scoped-2026-05-02.json` / `.log` | 3 forks, 5x5s warmup, 8x5s measurement | Three-stage publish rows: physical, inline/manual fused, and equal-call-site reference. |
| `end-to-end-scoped-2026-05-02.json` / `.log` | 2 forks, 5x3s warmup, 7x3s measurement | Completed-operation source/sink, pipeline, broadcast, and dependency shapes. |
| `optimal-path-completed-2026-05-02.json` / `.log` | 3 forks, 5x5s warmup, 8x5s measurement | Completion-gated optimal path used for the README headline row. |
| `optimal-path-latency-2026-05-02.json` / `.log` | 2 forks, 5x3s warmup, 5x3s measurement, JMH sample-time mode | Original optimal-path latency subset retained for audit history. |
| `latency-isolated-*-2026-05-02.json` | 3 forks, 5x3s warmup, 5x3s measurement, JMH sample-time mode | Individually isolated end-to-end Lattice and Disruptor latency paths. |
| `optimal-path-gc-2026-05-02.json` / `.log` | 2 forks, 5x3s warmup, 7x3s measurement, `-prof gc` | Allocation rate, normalized allocation, and GC count for the optimal path. |

## Figures

- [Headline throughput](../../assets/perf-pipeline.svg)
- [Headline ratios](../../assets/disruptor-comparison.svg)
- [Isolated end-to-end latency percentiles](../../assets/latency-percentiles.svg)
- [Isolated end-to-end p99 latency](../../assets/latency-p99.svg)
- [End-to-end throughput matrix](../../assets/end-to-end-throughput.svg)
- [Optimal path allocation and GC](../../assets/optimal-path-gc.svg)

## Scoped Headline Rows

| Scoped headline row | Lattice | Disruptor | Ratio |
| --- | ---: | ---: | ---: |
| Three-stage physical publish throughput | 31,938,529 ops/s | 21,698,059 ops/s | 1.47x |
| Three-stage inline/manual fused publish | 127,875,286 ops/s | 35,697,152 ops/s | 3.58x |
| Manually fused reference payload, equal call-site | 209,168,722 ops/s | 31,091,239 ops/s | 6.73x |
| Source-inline completed path | 77,868,589 ops/s | 3,620,353 ops/s | 21.51x |

The first three rows are publish-throughput measurements from
`ApplesToApplesDisruptorBenchmark`. The optimal-path row is stricter: each
operation waits until the sink/handler completes the same sequence. The
Lattice side of that row is source-inline elided: the eligible fused chain runs
on the producer thread and the physical source edge is removed.

## End-To-End Throughput

| Workload | Lattice | Disruptor | Ratio |
| --- | ---: | ---: | ---: |
| Source/sink completed | 3,870,781 ops/s | 5,324,832 ops/s | 0.73x |
| Physical pipeline completed | 1,229,655 ops/s | 1,701,728 ops/s | 0.72x |
| Inline/manual fused pipeline completed | 78,108,324 ops/s | 4,399,426 ops/s | 17.75x |
| Broadcast two-branch completed | 2,135,888 ops/s | 3,700,906 ops/s | 0.58x |
| Dependency/join completed | 1,362,877 ops/s | 2,381,730 ops/s | 0.57x |

The broader matrix is intentionally not a single winner-takes-all claim.
Physical source/sink and routing-heavy shapes still favor Disruptor on this
host, while the inline-fused static pipeline is the Lattice fast path.

## Isolated End-To-End Latency

JMH sample-time rows report completed-operation latency in `ns/op` for the same
parse/enrich/risk/serialize workload. See
[`latency.md`](../../latency.md) for profile definitions and interpretation.

| Variant | p50 | p90 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: |
| Lattice source-inline elided | 30 | 39 | 51 | 295 |
| Disruptor manual fused | 231 | 291 | 393 | 9,531 |
| Lattice physical strict topology | 775 | 874 | 1,434 | 21,152 |
| Disruptor physical | 617 | 728 | 3,632 | 30,240 |
| Lattice physical pinned CPU | 774 | 868 | 1,620 | 24,466 |

The p99.9 values are more sensitive to sampling length and host noise than the
p50/p99 values. Treat them as a checked-in run artifact, not as a platform
latency guarantee.

## Allocation And GC

Rows from `optimal-path-gc-2026-05-02.json`:

| Benchmark | Allocation rate | Normalized allocation | GC count |
| --- | ---: | ---: | ---: |
| Lattice inline-fused completed path | 0.000378 MB/s | 0.00000511 B/op | 0 |
| Disruptor manually fused completed path | 0.001070 MB/s | 0.000321 B/op | 0 |

Both rows are effectively allocation-free in steady state for this pass. The
GC-profiler throughput numbers are not used for the headline throughput claim.
