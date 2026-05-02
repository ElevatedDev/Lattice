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
| Producer pressure | Bounded ring capacity and gating sequences; `WaitStrategy` controls how processors wait on sequences. | Per-edge `OverflowPolicy` (block, timed, fail-fast, drop-latest, drop-oldest, coalescing, redirect). |
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
- JVM args:
  `-Xms2g -Xmx2g -XX:+AlwaysPreTouch -XX:+UnlockDiagnosticVMOptions -XX:+UseParallelGC`.
  Fusion, source inline, source-path elision, metrics, placement, and JFR are
  configured per benchmark graph through `FusionSpec`, `MetricsSpec`,
  `GraphPlacementSpec`, and `DiagnosticsSpec`.
- Disruptor 4.0.0 on the JMH classpath only (never bundled in the Lattice
  release jar).
- The 2026-05-02 three-stage and optimal-path headline rows use 3 forks, 5 x
  5 s warmup, and 8 x 5 s measurement. The broader end-to-end matrix uses 2
  forks, 5 x 3 s warmup, and 7 x 3 s measurement. The retained optimal-path
  latency subset uses 2 forks; the isolated latency rows used for the public
  latency page use 3 forks, 5 x 3 s warmup, and 5 x 3 s measurement.
- Numbers are reported in `ops/s` from the original JMH output, never
  hand-massaged. Raw JSON artifacts and stdout logs are checked in next to the
  summary so the published ratios can be audited.

## Current Publication Baseline

The 2026-05-02 refresh records both publish-throughput and completion-gated
comparisons after the per-graph runtime API migration. The table uses matching
scoped artifacts for each workload.

The result is a positive Lattice story for static graphs, not just a list of
caveats. Lattice leads the scoped physical publish, fused publish,
equal-call-site reference, source-inline completed, and physical p99 latency
rows. The remaining Disruptor-favorable rows are useful boundaries for the
current implementation, especially where the benchmark is closer to a simple
single-ring handoff than to the static graph shapes Lattice is built for.

![Three-stage publish throughput](assets/perf-pipeline.svg)

![Lattice vs Disruptor ratios](assets/disruptor-comparison.svg)

| Scoped headline row | Lattice | Disruptor | Ratio |
| --- | ---: | ---: | ---: |
| Three-stage physical publish throughput | 31,938,529 ops/s | 21,698,059 ops/s | 1.47x |
| Three-stage inline/manual fused publish | 127,875,286 ops/s | 35,697,152 ops/s | 3.58x |
| Manually fused reference payload (1 logical stage, 3 increments inline) | 209,168,722 ops/s | 31,091,239 ops/s | 6.73x |
| Source-inline completed path | 77,868,589 ops/s | 3,620,353 ops/s | 21.51x |

Disruptor's reference-payload benchmark collapses three increments into a
single `EventHandler` call, so the published reference row now uses the
equal-call-site Lattice shape: one Lattice stage doing the three increments
inline. That row reaches 209.2M ops/s and is 6.73x the scoped Disruptor
manually fused reference row. The fused publish row compares Lattice inline
fusion against a manually fused Disruptor copy-payload handler and measures
3.58x. The physical three-stage point estimate is also above parity at 1.47x.

The source-inline completed path is the strictest row for end-to-end operation
completion: every benchmark operation waits until the sink/handler publishes
completion for the same sequence. The publish-throughput rows remain useful
for comparing enqueue/publish hot paths, but they should not be used as a
completion-rate claim. The Lattice side of the completed headline is an
optimized source-inline graph-specialization path: the eligible fused chain
runs on the producer thread and the physical source edge is removed.

The broader end-to-end matrix is more mixed:

![End-to-end completed-operation throughput](assets/end-to-end-throughput.svg)

| Workload | Lattice | Disruptor | Ratio |
| --- | ---: | ---: | ---: |
| Source/sink completed | 3,870,781 ops/s | 5,324,832 ops/s | 0.73x |
| Physical pipeline completed | 1,229,655 ops/s | 1,701,728 ops/s | 0.72x |
| Inline/manual fused pipeline completed | 78,108,324 ops/s | 4,399,426 ops/s | 17.75x |
| Broadcast two-branch completed | 2,135,888 ops/s | 3,700,906 ops/s | 0.58x |
| Dependency/join completed | 1,362,877 ops/s | 2,381,730 ops/s | 0.57x |

That broader matrix is the boundary of the current implementation: physical
one-ring or routing-heavy completed-throughput cases can favor Disruptor, while
the static linear pipeline and graph-specialized paths are Lattice's fast
path.

## What The Baseline Set Measures

- **3-stage pipeline (physical)** keeps Lattice's three logical stages as
  physical workers; Disruptor uses three sequenced handlers on one ring.
- **3-stage pipeline (fused)** lets Lattice's compiler inline-fuse the chain;
  the copy-payload row shows a manually-fused Disruptor handler writing event
  slot fields, and the reference row uses `latticeManuallyFusedReference` to
  mirror Disruptor's one-call-site shape.
- **Source-inline completed path** compares Lattice inline source fusion with a
  Disruptor busy-spin/manual-fused handler and waits for completion on both
  sides.

## Choosing Between Them

Choose Disruptor when the application is actually a single sequenced stream:
one preallocated ring, event-slot mutation, and sequence-barrier dependencies.
It is excellent at that problem.

Choose Lattice when the application is a fixed typed DAG. That is the center
of this project, and it is where the benchmark evidence is strongest: local
overflow policy per edge, declared routing and stamp-correlated joins,
graph-local fusion, metrics and placement settings, inspectable plan and
metrics, compiler-checked payload reuse, and automatic specialization of the
physical runtime plan.

## Scope

The checked-in notes show Lattice strongest when the benchmark uses the graph
contract: single-producer source specialization, physical graph execution,
eligible linear fusion, source-inline execution, and compiler-checked payload
reuse. The manually fused reference row gives Lattice the same one-call-site
shape as Disruptor and measures a clear Lattice win. The broader end-to-end
matrix also shows that not every physical or routing-heavy shape wins against
Disruptor, but that boundary does not weaken the core claim:
**Lattice's value is the graph contract**. Preallocated payload paths, semantic
joins, deterministic backpressure, and fusion that preserves the logical graph
are first-class runtime features, not conventions layered on top of a queue.

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
