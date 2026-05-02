# Benchmark Results

Tracked benchmark summaries captured for each release.

| Snapshot | Profile | Host | Notes |
| --- | --- | --- | --- |
| [`2026-05-02-per-graph-refresh/`](2026-05-02-per-graph-refresh/) | Per-graph runtime API refresh | Intel i9-14900HX under WSL2, JDK 21.0.10 | Scoped three-stage, end-to-end completion, isolated sample-time latency, and optimal-path GC-profiler artifacts. |
| [`v1.0.0-baseline/`](v1.0.0-baseline/) | Historical publication baseline | Intel i7-7700, JDK 21.0.10 | Tracks the 2026-04-29 artifact set retained for audit history. |

For a side-by-side device view, see [Benchmark Devices](../devices.md).

The repository's [`benchmarks/baseline/`](../../benchmarks/baseline/) directory
holds the checked-in baseline used for regression review. It includes raw
JMH JSON and stdout logs for the selected public baseline rows. Larger
benchmark campaigns, JFR files, generated HTML, and profiler output belong in
local `results/` directories or as GitHub Release attachments unless a release
deliberately chooses to check them in.

## Figures

- [Three-stage publish throughput](../assets/perf-pipeline.svg)
- [Lattice vs Disruptor ratios](../assets/disruptor-comparison.svg)
- [Isolated end-to-end latency percentiles](../assets/latency-percentiles.svg)
- [Isolated end-to-end p99 latency](../assets/latency-p99.svg)
- [End-to-end throughput matrix](../assets/end-to-end-throughput.svg)
- [Optimal path allocation and GC](../assets/optimal-path-gc.svg)
- [Runtime guarantee map](../assets/guarantees-map.svg)

## How To Cite A Number

When quoting a Lattice number, always include:

1. The benchmark class and method (e.g. `OptimalPathBenchmark.latticeInlineFusedCompleted`).
2. The profile (`3 forks, 5x5s warmup, 8x5s measurement` for the 2026-05-02
   three-stage and optimal-path headline rows; `2 forks, 5x3s warmup, 7x3s
   measurement` for the broader end-to-end throughput matrix; `3 forks, 5x3s
   warmup, 5x3s measurement` for isolated sample-time latency rows).
3. The host (`uname -a`, JDK version, isolated cores).
4. Whether the native library was loaded.
5. A link to the tracked benchmark note or attached raw artifact.

This is the same standard the [Disruptor Comparison](../disruptor-comparison.md)
table follows.
