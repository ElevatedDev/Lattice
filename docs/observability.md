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

## Hot-Counter Toggles

Hot counters and per-stage histograms are gated by static final flags so
the JIT can fold them away when they are off:

```bash
-Dlattice.metrics.hotCounters=false       # default true
-Dlattice.metrics.stageHistograms=false   # default false
-Dlattice.metrics.residence=false         # default false
-Dlattice.runtime.fusedLogicalEdgeMetrics=false
```

Recommended max-throughput profile (used by the JMH suite) turns all four
off; the entire metrics call shell folds away on the fused hot path.

## JFR

`-Dlattice.jfr=true` enables Lattice's JFR event types:

- `lattice.GraphLifecycle`, `lattice.WorkerStart`, `lattice.WorkerStop`
- `lattice.EdgeOverflow`, `lattice.StageException`
- `lattice.Placement` (one event per worker, recorded once at bootstrap)

Events are designed to be cheap and non-allocating. JFR remains the right
profiling channel; per-event histograms are not.

## Placement Report

`graph.metrics().placementStatus()` returns one row per worker with the
requested affinity, the effective CPU set the OS allowed, the NUMA node, and
whether the native backend was loaded. Without the native backend, placement
requests are recorded as advisory unless `-Dlattice.placement.strict=true` is
set, in which case `start()` fails.

See also [Operations Runbook](operations-runbook.md).

