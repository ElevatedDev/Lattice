# Lattice Documentation

Lattice is a Java 21 runtime for bounded, low-latency, in-process processing
graphs with static topology, explicit backpressure, and inspectable runtime
state.

It is aimed at services whose processing shape is known before startup:
validate, enrich, route, join, and sink inside one JVM without pretending to be
a broker or distributed stream processor.

When browsing the repository, the same material is available through the root
[README](https://github.com/ElevatedDev/Lattice/blob/main/README.md).

## Start Here

| Page | Purpose |
| --- | --- |
| [Getting Started](getting-started.md) | Build the project and run a first graph. |
| [Examples](examples/README.md) | Runnable examples backed by public APIs. |
| [Graph DSL](graph-dsl.md) | Builder methods for sources, stages, routes, joins, sinks, and edges. |
| [Architecture](architecture.md) | Build-time compile path and runtime worker plan. |

## Runtime Contract

| Page | Purpose |
| --- | --- |
| [Edge Semantics](edge-semantics.md) | SPSC/MPSC behavior, capacity, overflow, and close/drain rules. |
| [Ordering Guarantees](ordering-guarantees.md) | The ordering contract across edges, fan-out, partitioning, and joins. |
| [Backpressure](backpressure.md) | Overflow policies, sizing, producer behavior, and metrics. |
| [Failure Modes](failure-modes.md) | Build failures, runtime failures, stage exceptions, abort, and slab leaks. |

## Operations And Release

| Page | Purpose |
| --- | --- |
| [Observability](observability.md) | Metrics, hot-counter toggles, JFR, and placement reports. |
| [Operations Runbook](operations-runbook.md) | Startup checks, lifecycle operations, and diagnostics. |
| [Compatibility Matrix](compatibility-matrix.md) | JDK, OS, dependency, and versioning policy. |
| [API Reference](api.md) | Public package summary and planned Maven coordinate. |
| [Generated Javadocs](api/latest/index.html) | Checked-in generated Javadoc HTML. It becomes normal navigable Javadocs when served by GitHub Pages or another static host. |
| [Release Process](release.md) | Maintainer checklist for release verification and publishing. |

## Performance

| Page | Purpose |
| --- | --- |
| [Performance Tuning](https://github.com/ElevatedDev/Lattice/blob/main/PERFORMANCE_TUNING.md) | Tuning guidance and benchmark methodology. |
| [Latency Profile](latency.md) | Isolated end-to-end latency paths, p99 view, profile definitions, and caveats. |
| [Benchmark Results](benchmark-results/README.md) | Checked-in benchmark snapshots. |
| [Benchmark Devices](devices.md) | Device-specific benchmark snapshots and cross-device caveats. |
| [Disruptor Comparison](disruptor-comparison.md) | Methodology and honest framing against LMAX Disruptor. |
| [Linux Validation Notes](linux-validation.md) | Procedure for reproducing benchmark evidence on another Linux host. |

## Figures

| Figure | Purpose |
| --- | --- |
| [Three-stage publish throughput](assets/perf-pipeline.svg) | Throughput bars with JMH error bars. |
| [Lattice vs Disruptor ratios](assets/disruptor-comparison.svg) | Ratio view for the comparable Disruptor rows. |
| [Isolated end-to-end latency percentiles](assets/latency-percentiles.svg) | JMH sample-time latency curve for the isolated Lattice and Disruptor completed paths. |
| [Isolated end-to-end p99 latency](assets/latency-p99.svg) | P99-only view for isolated end-to-end latency paths with p99 range whiskers. |
| [End-to-end throughput matrix](assets/end-to-end-throughput.svg) | Completed-operation throughput across source/sink, pipeline, fanout, and dependency shapes. |
| [Optimal path allocation and GC](assets/optimal-path-gc.svg) | JMH GC-profiler allocation and GC-count summary for the optimal path. |
| [Runtime guarantee map](assets/guarantees-map.svg) | Summary of the runtime guarantees and non-goals. |

## Hosting Note

This page is prepared as the GitHub Pages entry point for the documentation
set. Until Pages is enabled, GitHub shows these as repository files rather than
as a rendered documentation site.

## Project Files

- [Changelog](https://github.com/ElevatedDev/Lattice/blob/main/CHANGELOG.md)
- [Contributing](https://github.com/ElevatedDev/Lattice/blob/main/CONTRIBUTING.md)
- [Code of Conduct](https://github.com/ElevatedDev/Lattice/blob/main/CODE_OF_CONDUCT.md)
- [Security Policy](https://github.com/ElevatedDev/Lattice/blob/main/SECURITY.md)
- [Notice](https://github.com/ElevatedDev/Lattice/blob/main/NOTICE)
- [License](https://github.com/ElevatedDev/Lattice/blob/main/LICENSE)
