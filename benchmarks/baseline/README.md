# Benchmark Baseline

This directory contains the refreshed benchmark baseline captured on
2026-05-02, plus the older 2026-04-29 publication artifacts retained for audit
history. The current public figures use the `*-2026-05-02.json` artifacts.

This is the checked-in public result set for regression review, open-source
transparency, and research comparison. See `env.txt` for the validation
profile, JVM flags, and exact include patterns.

## Figures

| Figure | File |
| --- | --- |
| Three-stage throughput | `../../docs/assets/perf-pipeline.svg` |
| Lattice vs Disruptor ratios | `../../docs/assets/disruptor-comparison.svg` |
| Optimal-path latency percentiles | `../../docs/assets/latency-percentiles.svg` |
| End-to-end throughput matrix | `../../docs/assets/end-to-end-throughput.svg` |
| Optimal path allocation and GC | `../../docs/assets/optimal-path-gc.svg` |

## Artifact Map

| Artifact | Profile | Purpose |
| --- | --- | --- |
| `three-stage-scoped-2026-05-02.json` / `.log` | 3 forks, 5x5s warmup, 8x5s measure | Current scoped three-stage Lattice physical/inline-fused/reference vs Disruptor physical/manual-fused/reference publish rows. |
| `end-to-end-scoped-2026-05-02.json` / `.log` | 2 forks, 5x3s warmup, 7x3s measure | Current completion-gated source/sink, pipeline, broadcast, and dependency rows. |
| `optimal-path-completed-2026-05-02.json` / `.log` | 3 forks, 5x5s warmup, 8x5s measure | Current completion-gated optimal path headline row. |
| `optimal-path-latency-2026-05-02.json` / `.log` | 2 forks, 5x3s warmup, 5x3s measure, sample-time mode | Current JMH latency percentiles for Lattice fused, Lattice source-inline, and Disruptor manual-fused optimal paths. |
| `optimal-path-gc-2026-05-02.json` / `.log` | 2 forks, 5x3s warmup, 7x3s measure, `-prof gc` | Current optimal-path allocation and GC profiler rows. |
| `three-stage-vs-disruptor.json` / `.log` | 3 forks, 5x5s warmup, 8x5s measure | Broad three-stage Lattice physical/inline-fused vs Disruptor physical/manual-fused matrix retained for audit history. |
| `three-stage-isolated-physical.json` / `.log` | 2 forks, 3x3s warmup, 5x3s measure | Isolated Lattice physical three-stage vs Disruptor physical three-handler publish throughput. |
| `three-stage-isolated-fused-copy.json` / `.log` | 2 forks, 3x3s warmup, 5x3s measure | Isolated Lattice inline-fused three-stage vs Disruptor manually fused copy-payload publish throughput. |
| `three-stage-isolated-reference.json` / `.log` | 2 forks, 3x3s warmup, 5x3s measure | Isolated reference-payload and equal-call-site Lattice rows vs Disruptor manually fused reference publish throughput. |
| `optimal-path-completed.json` / `.log` | 3 forks, 5x5s warmup, 8x5s measure | Completion-gated optimal path: each operation waits for the sink/handler to complete the same sequence. |
| `lattice-core-basics.json` / `.log` | 1 fork, 3x5s warmup, 5x5s measure | Source/sink paths, batched topology, routing/topology rows, and raw edge regression rows with latency logs. |
| `lattice-placement.json` / `.log` | 1 fork, 3x5s warmup, 5x5s measure | Portable placement subset: `containerCpuset`, `pinning=false`, first-touch on/off, on-heap slots. |
| `env.txt` | n/a | Host, toolchain, JVM flags, and include patterns. |

## Top Checked-In Head-To-Head

Rows from the current 2026-05-02 scoped artifacts.

| Comparison | Lattice (ops/s) | Lattice source | Disruptor (ops/s) | Disruptor source | Ratio |
| --- | ---: | --- | ---: | --- | ---: |
| Physical three-stage publish | 31,938,529 | `three-stage-scoped-2026-05-02.json` | 21,698,059 | `three-stage-scoped-2026-05-02.json` | 1.47x |
| Inline/manual fused copy publish | 127,875,286 | `three-stage-scoped-2026-05-02.json` | 35,697,152 | `three-stage-scoped-2026-05-02.json` | 3.58x |
| Manual fused reference publish, equal call-site | 209,168,722 | `three-stage-scoped-2026-05-02.json` | 31,091,239 | `three-stage-scoped-2026-05-02.json` | 6.73x |

Interpretation: the reference row uses equal call-site footing: one Lattice
stage performs the same three increments inline as Disruptor's manually fused
handler. The physical three-stage point estimate remains above parity, and the
completed path is the strict end-to-end row below.

## Completed Optimal Path

Rows from `optimal-path-completed-2026-05-02.json`. Unlike the publish-throughput rows,
each benchmark operation waits until the sink/handler publishes completion for
the same sequence number.

| Benchmark | Score (ops/s) | Error | Notes |
| --- | ---: | ---: | --- |
| Lattice inline-fused completed path | 77,868,589 | +-598,524 | Three logical stages plus sink complete on producer thread. |
| Disruptor manually fused completed path | 3,620,353 | +-78,946 | One busy-spin handler; benchmark waits for handler completion. |

Ratio: Lattice completed path / Disruptor completed path = 21.51x on this host.

## Broader End-To-End Rows

Rows from `end-to-end-scoped-2026-05-02.json`.

| Workload | Lattice | Disruptor | Ratio |
| --- | ---: | ---: | ---: |
| Source/sink completed | 3,870,781 ops/s | 5,324,832 ops/s | 0.73x |
| Physical pipeline completed | 1,229,655 ops/s | 1,701,728 ops/s | 0.72x |
| Inline/manual fused pipeline completed | 78,108,324 ops/s | 4,399,426 ops/s | 17.75x |
| Broadcast two-branch completed | 2,135,888 ops/s | 3,700,906 ops/s | 0.58x |
| Dependency/join completed | 1,362,877 ops/s | 2,381,730 ops/s | 0.57x |

Rows with very wide error bars retain their JMH error bars in the table. Do
not rank close results without checking the raw JSON confidence intervals and
matching topology semantics.

## Latency Excerpts

Rows from `optimal-path-latency-2026-05-02.json` in JMH sample-time mode.

| Variant | p50 | p90 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: |
| Lattice fused owner worker | 296 | 335 | 738 | 12,944 |
| Lattice source-inline elided | 30 | 40 | 58 | 290 |
| Disruptor manual fused | 243 | 310 | 491 | 11,172 |

## Allocation And GC

Rows from `optimal-path-gc-2026-05-02.json`.

| Benchmark | Allocation rate | Normalized allocation | GC count |
| --- | ---: | ---: | ---: |
| Lattice inline-fused completed path | 0.000378 MB/s | 0.00000511 B/op | 0 |
| Disruptor manually fused completed path | 0.001070 MB/s | 0.000321 B/op | 0 |

## Interpretation Rules

- JMH uses compiler blackholes on this JVM; keep the raw JMH notes with any
  quoted number.
- The three-stage head-to-head class measures publish throughput. Use
  `OptimalPathBenchmark` when completed-operation throughput matters.
- The completed-path benchmark avoids comparing synchronous inline completion
  with asynchronous enqueue-only rates.
- Native placement and cross-socket behavior are not part of this portable
  baseline because the placement rows use `pinning=false`.
