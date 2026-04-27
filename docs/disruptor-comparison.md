# Disruptor Comparison

Lattice and [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) both
target low-latency in-process messaging on the JVM, but they make very
different design choices. This page is the methodology and honest-numbers
companion to the repository
[Performance Snapshot](https://github.com/ElevatedDev/Lattice/blob/main/README.md#performance-snapshot).

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

The checked-in snapshots under [`benchmark-results/`](benchmark-results/)
follow these rules:

- JMH 1.37 (via the `me.champeau.jmh` Gradle plugin 0.7.3).
- JDK 21.0.10, Temurin.
- JVM args (also set in `build.gradle`): `-Xms2g -Xmx2g -XX:+AlwaysPreTouch
  -XX:+UseParallelGC` plus the four `-Dlattice.metrics.*=false` flags.
- Disruptor 4.0.0 on the JMH classpath only (never bundled in the published
  jar).
- Single forked JVM per benchmark.
- "Smoke" runs use 1 iteration × 3 s for a fast end-to-end signal; "warmed"
  runs use 3 × 10 s warmup + 5 × 10 s measurement.
- Numbers are reported in `ops/s` from the original JMH JSON, never
  hand-massaged.

## What The Baseline Set Measures

- **SPSC preallocated (apples-to-apples)** uses the same payload class,
  same producer, same consumer, with Lattice's preallocated source on one
  side and Disruptor's pre-allocated event slot on the other.
- **3-stage pipeline (physical)** keeps Lattice's three logical stages as
  three physical workers; Disruptor uses three sequenced handlers on one ring.
  This is the apples-to-apples shape.
- **3-stage pipeline (fused)** lets Lattice's compiler fuse the chain;
  Disruptor's column shows the *manually-fused* equivalent (one handler
  doing all three steps). This row exists to be transparent: Disruptor wins
  this micro because manual fusion on a single ring is hard to beat.
- **SPSC source→sink (bare)** is the bare-ring primitive shape; Disruptor's
  ring is famously well-tuned for this exact workload.
- **MPSC reference** is a primitive multi-producer handoff; Disruptor's
  multi-producer ring remains stronger on this primitive shape.

## Honest Framing

Lattice is competitive with Disruptor on SPSC preallocated payloads
(≈1.03× in the baseline) and the SPSC edge-pair primitive
(34.4 M ops/s warmed). Disruptor remains stronger on bare-ring micros and
manually-fused pipelines. **Lattice's value is the graph contract** —
preallocated payload paths, semantic joins, deterministic backpressure, and
fusion that preserves the logical graph — not "faster than Disruptor" on
every shape.

If your workload is "one ring, many handlers, manual fan-out", use
Disruptor. If your workload is "a fixed DAG with explicit joins, partitions,
and backpressure", Lattice is what that DAG looks like.

Generated working copies may live under `benchmarks/baseline/` locally. Public
documentation should cite the versioned snapshot under `docs/benchmark-results/`.

See also [Linux Validation Notes](linux-validation.md) for how to produce
publication-grade benchmarks.
