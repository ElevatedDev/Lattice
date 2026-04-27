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

- JMH 1.37 (via the `me.champeau.jmh` Gradle plugin 0.7.3).
- JDK 21.0.10, Temurin.
- JVM args (also set in `build.gradle`): `-Xms2g -Xmx2g -XX:+AlwaysPreTouch
  -XX:+UseParallelGC` plus the four `-Dlattice.metrics.*=false` flags.
- Disruptor 4.0.0 on the JMH classpath only (never bundled in the published
  jar).
- Single forked JVM per benchmark.
- Smoke runs use 1 iteration x 3 s for a fast end-to-end signal; warmed runs
  use 3 x 10 s warmup + 5 x 10 s measurement.
- Numbers are reported in `ops/s` from the original JMH output, never
  hand-massaged. Raw JSON artifacts should be attached to the matching GitHub
  Release before making publication-grade claims.

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
- **SPSC source->sink (bare)** is the bare-ring primitive shape; Disruptor's
  ring is famously well-tuned for this exact workload.
- **MPSC reference** is a primitive multi-producer handoff; Disruptor's
  multi-producer ring remains stronger on this primitive shape.

## Honest Framing

The current checked-in WSL2 notes show Lattice strongest on graph-shaped
pipelines where the runtime can specialize source ingress and fuse eligible
linear chains. Disruptor remains stronger on some bare-ring micros and
manually-fused single-handler shapes. **Lattice's value is the graph contract**:
preallocated payload paths, semantic joins, deterministic backpressure, and
fusion that preserves the logical graph, not "faster than Disruptor" on every
shape.

If your workload is "one ring, many handlers, manual fan-out", use
Disruptor. If your workload is "a fixed DAG with explicit joins, partitions,
and backpressure", Lattice is what that DAG looks like.

Generated working copies may live under `results/` locally. Public
documentation should cite tracked notes under `benchmarks/baseline/`, the
versioned summary under `docs/benchmark-results/`, or raw artifacts attached to
the matching GitHub Release.

See also [Linux Validation Notes](linux-validation.md) for how to produce
publication-grade benchmarks.
