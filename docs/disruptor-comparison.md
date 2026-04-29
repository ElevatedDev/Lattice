# Disruptor Comparison

Lattice and [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) both
target low-latency in-process messaging on the JVM, but they make very
different design choices. This page is the methodology and honest-numbers
companion to the repository
[Performance Snapshot](../README.md#performance-snapshot).

## Conceptual Differences

| Concern | Disruptor | Lattice |
| --- | --- | --- |
| Topology shape | One ring with sequence barriers; multicast & dependency are barrier patterns. | Explicit DAG of nodes (source / stage / route / join / sink) connected by point-to-point edges. |
| Backpressure | `WaitStrategy` on the ring. | Per-edge `OverflowPolicy` (block, timed, fail-fast, lossy, coalescing, redirect). |
| Ordering | Single global sequence domain. | Local per-edge ordering. |
| Payloads | Pre-allocated event slots, mutated in place. | Either user-owned POJOs or slab-pool handles. |
| Routing | Application code consumes a slice of the ring. | DSL primitives: `dispatch`, `broadcast`, `partition`. |
| Joins | Sequence barriers on multiple producers. | Stamp-correlated `join` with explicit duplicate / timeout / missing policy. |
| Fusion | Manual: collapse multiple handlers into one. | Automatic for eligible linear chains; preserves the logical graph. |
| Native placement | Out of scope. | Optional Rust JNI backend for Linux affinity / NUMA. |

## Methodology

The checked-in benchmark notes under [`benchmark-results/`](benchmark-results/)
and [`benchmarks/baseline/`](../benchmarks/baseline/) follow these rules:

- JMH 1.36 (via the `me.champeau.jmh` Gradle plugin 0.7.3).
- JDK 21.0.10.
- JVM args (also set in `build.gradle`): `-Xms2g -Xmx2g -XX:+AlwaysPreTouch
  -XX:+UseParallelGC`, inline source fusion enabled, hot metrics disabled,
  fused logical edge metrics disabled, and inline-depth tracking disabled for
  the benchmark hot path.
- Disruptor 4.0.0 on the JMH classpath only (never bundled in the published
  jar).
- The longer head-to-head matrix uses 3 forks, 5 x 5 s warmup, and 8 x 5 s
  measurement. Isolated stage groups use 2 forks, 3 x 3 s warmup, and 5 x 3 s
  measurement. The headline table picks the best checked-in Lattice point
  estimate per shape and keeps the same-artifact Disruptor comparator for that
  row.
- Broader lattice basics use 1 fork, 3 x 5 s warmup, and 5 x 5 s measurement.
- Numbers are reported in `ops/s` from the original JMH output, never
  hand-massaged. Raw JSON artifacts and stdout logs are checked in next to the
  summary so the published ratios can be audited.

## Current Publication Baseline

The 2026-04-29 baseline records both publish-throughput and completion-gated
comparisons. The stage table uses the best checked-in Lattice point estimate
for each published shape and compares it with the Disruptor row from the same
benchmark artifact.

![Three-stage publish throughput](assets/perf-pipeline.svg)

![Lattice vs Disruptor ratios](assets/disruptor-comparison.svg)

| Workload | Lattice | Disruptor | Ratio |
| --- | ---: | ---: | ---: |
| Three-stage physical publish throughput | 27,660,948 ops/s | 26,377,465 ops/s | 1.05x |
| Three-stage inline/manual fused, copy payload | 61,838,846 ops/s | 35,200,599 ops/s | 1.76x |
| Three-stage inline/manual fused, reference payload (3 logical stages) | 52,698,325 ops/s | 38,713,479 ops/s | 1.36x |
| Manually fused reference payload (1 logical stage, 3 increments inline) | 92,094,463 ops/s | 44,045,374 ops/s | 2.09x |
| Completed optimal path | 29,903,291 ops/s | 4,742,326 ops/s | 6.31x |

The "3 logical stages" reference-payload row and the "1 logical stage"
manually-fused row are the same Disruptor handler measured against two
different Lattice shapes. Disruptor's reference-payload benchmark collapses
three increments into a single `EventHandler` call; Lattice's inline-fused
3-stage row keeps three logical stages with their own call sites. Lattice's
inline-fused path already passes the payload by reference end-to-end (no
per-slot field copy and no ring-buffer event-slot store). When the comparison
is forced to equal call-site footing - one Lattice stage doing the three
increments inline - Lattice reaches 92.1M ops/s and is 2.09x the Disruptor
manually fused reference row in the longer checked-in matrix. The physical
three-stage point estimate is also above parity at 1.05x in the isolated
physical artifact.

The completed optimal path is the strictest row for end-to-end operation
completion: every benchmark operation waits until the sink/handler publishes
completion for the same sequence. The publish-throughput rows remain useful
for comparing enqueue/publish hot paths, but they should not be used as a
completion-rate claim.

## What The Baseline Set Measures

- **3-stage pipeline (physical)** keeps Lattice's three logical stages as
  physical workers; Disruptor uses three sequenced handlers on one ring.
- **3-stage pipeline (fused)** lets Lattice's compiler inline-fuse the chain;
  Disruptor's copy-payload column shows a manually-fused handler writing event
  slot fields, and the reference-payload column shows the same handler with an
  event slot reference. A second Lattice row (`latticeManuallyFusedReference`)
  reduces the chain to a single logical stage performing the same three
  increments inline, mirroring Disruptor's call-site shape so the comparison
  isolates payload semantics from call-site count.
- **Completed optimal path** compares Lattice inline source fusion with a
  Disruptor busy-spin/manual-fused handler and waits for completion on both
  sides.
- **SPSC/MPSC basics** are tracked in the lattice baseline for regression
  context, not as a Disruptor superiority claim.

## Scope

The checked-in notes show Lattice strongest when the benchmark uses the graph
contract: single-producer source specialization and eligible linear fusion.
The manually fused reference row gives Lattice the same one-call-site shape as
Disruptor and measures a clear Lattice win. The isolated physical three-stage
row is above parity by point estimate, with overlapping error bars. **Lattice's
value is the graph contract**:
preallocated payload paths, semantic joins, deterministic backpressure, and
fusion that preserves the logical graph.

If your workload is "one ring, one hand-fused handler doing the work of many
stages", include the one-stage Lattice row before drawing conclusions. If your
workload is "a fixed DAG with explicit joins, partitions, and backpressure",
Lattice is what that DAG looks like.

Generated working copies may live under `results/` locally. Public
documentation should cite tracked notes under `benchmarks/baseline/`, the
versioned summary under `docs/benchmark-results/`, or raw artifacts attached to
the matching GitHub Release.

See also [Linux Validation Notes](linux-validation.md) for how to reproduce
the methodology on another Linux host.
