# Benchmark Results

Tracked benchmark summaries captured for each release.

| Snapshot | Profile | Host | Notes |
| --- | --- | --- | --- |
| [`v1.0.0-baseline/`](v1.0.0-baseline/) | Publication baseline | Intel i7-7700, JDK 21.0.10 | Tracks the 2026-04-29 `benchmarks/baseline/` artifact set and the generated figures under `docs/assets/`. |

The repository's [`benchmarks/baseline/`](../../benchmarks/baseline/) directory
holds the checked-in baseline used for regression review. It includes raw
JMH JSON and stdout logs for the selected public baseline rows. Larger
benchmark campaigns, JFR files, generated HTML, and profiler output belong in
local `results/` directories or as GitHub Release attachments unless a release
deliberately chooses to check them in.

## Figures

- [Three-stage publish throughput](../assets/perf-pipeline.svg)
- [Lattice vs Disruptor ratios](../assets/disruptor-comparison.svg)
- [End-to-end latency percentiles](../assets/latency-percentiles.svg)
- [Runtime guarantee map](../assets/guarantees-map.svg)

## How To Cite A Number

When quoting a Lattice number, always include:

1. The benchmark class and method (e.g. `OptimalPathBenchmark.latticeInlineFusedCompleted`).
2. The profile (`2 forks, 3x3s warmup, 5x3s measurement` for isolated stage
   artifacts; `3 forks, 5x5s warmup, 8x5s measurement` for the longer
   Disruptor matrix, the 92.1M manual-fused reference row, and the completed
   path; `1 fork, 3x5s warmup, 5x5s measurement` for broader Lattice rows).
3. The host (`uname -a`, JDK version, isolated cores).
4. Whether the native library was loaded.
5. A link to the tracked benchmark note or attached raw artifact.

This is the same standard the [Disruptor Comparison](../disruptor-comparison.md)
table follows.
