# Benchmark Results

Tracked benchmark summaries captured for each release.

| Snapshot | Profile | Host | Notes |
| --- | --- | --- | --- |
| [`v1.0.0-baseline/`](v1.0.0-baseline/) | Warmed WSL2 notes | WSL2 Ubuntu 24.04, i7-7700, JDK 21.x | Orientation only. Production claims require [Linux validation](../linux-validation.md). |

The repository's [`benchmarks/baseline/`](../../benchmarks/baseline/) directory
holds the tracked working notes used while developing 1.0.0. Raw JMH JSON,
JFR, and generated HTML artifacts belong in local `results/` directories or as
GitHub Release attachments unless a release deliberately chooses to check them
in.

## How To Cite A Number

When quoting a Lattice number, always include:

1. The benchmark class and method (e.g. `ApplesToApplesDisruptorBenchmark.spscPreallocated`).
2. The profile (smoke `1x3s` vs warmed `3x10s + 5x10s`).
3. The host (`uname -a`, JDK version, isolated cores).
4. Whether the native library was loaded.
5. A link to the tracked benchmark note or attached raw artifact.

This is the same standard the [Disruptor Comparison](../disruptor-comparison.md)
table follows.
