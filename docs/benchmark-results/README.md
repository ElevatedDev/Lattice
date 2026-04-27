# Benchmark Results

JMH JSON snapshots captured for each release.

| Snapshot | Profile | Host | Notes |
| --- | --- | --- | --- |
| [`v1.0.0-baseline/`](v1.0.0-baseline/) | Smoke + warmed mix | WSL2 Ubuntu 24.04, i7-7700, JDK 21.0.10 | Orientation only. Production claims require [Linux validation](../linux-validation.md). |

The repository's `benchmarks/baseline/` directory holds the working copy of
the same JSON files used while developing 1.0.0; they are mirrored under
this directory so README links are stable across releases.

## How To Cite A Number

When quoting a Lattice number, always include:

1. The benchmark class and method (e.g. `ApplesToApplesDisruptorBenchmark.spscPreallocated`).
2. The profile (smoke `1×3s` vs warmed `3×10s + 5×10s`).
3. The host (`uname -a`, JDK version, isolated cores).
4. Whether the native library was loaded.
5. A link to the underlying JSON file in this directory.

This is the same standard the [Disruptor Comparison](../disruptor-comparison.md)
table follows.
