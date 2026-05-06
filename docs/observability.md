# Observability

Lattice exposes runtime state through plain Java APIs and optional JFR events.
There is no logging on the hot path.

## Build-Time Compilation Report

`graph.compilationReport()` returns an immutable `GraphCompilationReport`
created during `build()`. It is not a metric collector and does not add work to
the steady-state path. The report lists:

- worker decisions such as `RUNNABLE`, `FUSED_INTO_STAGE`, and
  `INLINE_SOURCE_CHAIN`;
- declared and effective edge kinds, including source specialization;
- sender strategies and redirect edges;
- positive merge decisions such as `STAGE_CHAIN` and `STAGE_TO_SINK`;
- fallback rows with stable reason codes such as
  `fusion.non_fusible_edge.overflow`.

Use the structured rows for tooling and `toSummaryString()` for support output.

## Metric Surfaces

| Surface | Source | Highlights |
| --- | --- | --- |
| `GraphCompilationReport` | `graph.compilationReport()` | Build-time worker, edge, sender, merge, and fallback decisions. |
| `GraphMetrics` | `graph.metrics()` | Aggregate counters, lifecycle state, placement report. |
| `StageMetrics` | per stage | Invocations, in/out counts, exceptions, optional histograms. |
| `EdgeMetrics`  | per edge  | Accepted/consumed counts, failed and blocked offers, drop/coalesce/redirect counters, depth, high-water mark. |
| `StageMetrics` wait counters | per stage | `spinCount()`, `yieldCount()`, and `parkCount()` when hot counters are enabled. |
| `GraphMetrics.StagePlacement` | `graph.metrics().placementReport()` | Requested and observed CPU/NUMA ids, placement status, and violation counters. |

## Per-Graph Metrics Controls

Metrics are off by default. Enable only the surfaces needed by the graph:

```java
StaticGraph graph = StaticGraph.builder("orders")
    .metrics(MetricsSpec.off()
        .hotCounters(true)
        .fusedLogicalEdgeCounters(true)
        .stageHistograms(true)
        .residenceTiming(true))
    .build();
```

`hotCounters(true)` turns on graph, stage, edge, wait, and routing counters.
`fusedLogicalEdgeCounters(true)` records logical traffic for edges removed by
fusion or source-path elision; it is dormant unless hot counters are also on.
`stageHistograms(true)` allocates HdrHistogram instances for batch/service
timing. `residenceTiming(true)` adds timestamp reads on edge offer/poll paths.

For max-throughput runs, keep `MetricsSpec.off()` so the default fused hot path
uses the no-observability executor.

## JFR

JFR event emission is also per graph:

```java
StaticGraph graph = StaticGraph.builder("orders")
    .diagnostics(DiagnosticsSpec.off().jfr(true))
    .build();
```

Capture those events with normal JVM recording options, for example:

```bash
-XX:StartFlightRecording=filename=lattice.jfr,settings=profile,dumponexit=true
```

The emitted event types include:

- `io.github.elevateddev.lattice.GraphStarted`, `io.github.elevateddev.lattice.GraphStopped`
- `io.github.elevateddev.lattice.StageException`, `io.github.elevateddev.lattice.EdgeBackpressure`,
  `io.github.elevateddev.lattice.EdgeStall`
- `io.github.elevateddev.lattice.WorkerBlocked`, `io.github.elevateddev.lattice.WorkerParked`,
  `io.github.elevateddev.lattice.BatchProcessed`
- `io.github.elevateddev.lattice.WorkerPlacement`, `io.github.elevateddev.lattice.AffinityMismatch`,
  `io.github.elevateddev.lattice.NumaMismatch`

Events are designed to be cheap and non-allocating. JFR remains the right
profiling channel; per-event histograms are not.

## Placement Report

`graph.metrics().placementReport()` returns one row per worker with the
requested CPU/NUMA ids, observed CPU/NUMA ids, placement status, diagnostic
message, and violation counters. Without the native backend, placement
requests are recorded as advisory unless
`GraphPlacementSpec.off().strict(true)` is set on the graph, in which case
`start()` fails.

See also [Operations Runbook](operations-runbook.md).
