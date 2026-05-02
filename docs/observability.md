# Observability

Lattice exposes runtime state through plain Java APIs and optional JFR events.
There is no logging on the hot path.

## Metric Surfaces

| Surface | Source | Highlights |
| --- | --- | --- |
| `GraphMetrics` | `graph.metrics()` | Aggregate counters, lifecycle state, placement report. |
| `StageMetrics` | per stage | Invocations, in/out counts, exceptions, optional histograms. |
| `EdgeMetrics`  | per edge  | Offered, accepted, rejected, dropped, redirected, queue depth samples. |
| `WaitMetrics`  | per worker | Park reasons, spins, yields, parks, wake counts. |
| `PlacementStatus` | per worker | Requested vs effective CPU set, NUMA node, native lib status. |

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

- `lattice.GraphLifecycle`, `lattice.WorkerStart`, `lattice.WorkerStop`
- `lattice.EdgeOverflow`, `lattice.StageException`
- `lattice.Placement` (one event per worker, recorded once at bootstrap)

Events are designed to be cheap and non-allocating. JFR remains the right
profiling channel; per-event histograms are not.

## Placement Report

`graph.metrics().placementStatus()` returns one row per worker with the
requested affinity, the effective CPU set the OS allowed, the NUMA node, and
whether the native backend was loaded. Without the native backend, placement
requests are recorded as advisory unless
`GraphPlacementSpec.off().strict(true)` is set on the graph, in which case
`start()` fails.

See also [Operations Runbook](operations-runbook.md).
