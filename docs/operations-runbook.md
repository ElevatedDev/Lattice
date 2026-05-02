# Operations Runbook

Day-2 guidance for running Lattice graphs in production.

## Startup Checks

1. Graph runtime specs set as documented in [Observability](observability.md):
   keep `MetricsSpec.off()` and `DiagnosticsSpec.off()` unless you actively
   need counters, histograms, residence timing, logical fused-edge counters, or
   JFR events.
2. If using the native backend, confirm `java.library.path` resolves
   `libstatic_topology_native.{so,dylib,dll}` and that
   `graph.metrics().placementReport()` reports the requested CPU/NUMA ids.
3. Use `GraphPlacementSpec.off().strict(true)` in production when affinity is
   required; otherwise placement requests are advisory.
4. Verify edge capacities are power-of-two and slab pools are sized per
   [Backpressure §Sizing](backpressure.md).

## Healthy Steady-State Signals

- `EdgeMetrics.depth()` and `highWaterMark()` stay well below capacity.
- `EdgeMetrics.failedOffers()`, `droppedLatest()`, `droppedOldest()`, and
  `redirectedOffers()` are flat or growing proportionally to known load.
- Stage `spinCount()`, `yieldCount()`, and `parkCount()` are consistent with
  the configured wait strategy.
- `StageMetrics.stageExceptions()` stays flat.
- `GraphMetrics.placementReport()` shows the expected CPU/NUMA ids, observed
  CPU/NUMA ids, and zero affinity/NUMA violations when strict placement is
  enabled.

## Lifecycle Operations

| Action | API | Behavior |
| --- | --- | --- |
| Graceful stop | `graph.close()` | Closes sources, drains accepted items, awaits workers. |
| Drain wait | `graph.awaitTermination(Duration)` | Blocks until workers stop or deadline elapses. |
| Quiesce / resume | `graph.quiesce(Duration)` / `graph.resume()` | Pauses source acceptance and waits for in-flight work to drain, then resumes without rebuilding. |
| Fail-stop | automatic on stage exception | Workers transition to `FAILED`; metrics record cause. |
| Abort | `graph.abort()` | Closes edges, interrupts workers, awaits termination, *then* drains. No drain promise. |

## Common Operational Tasks

- **Rolling restart**: not supported in-process. Build the new graph, start
  it side-by-side, drain the old, then close.
- **Resizing capacity**: not supported at runtime. Capacity is part of the
  static topology contract; rebuild the graph.
- **Hot reload of stage logic**: not supported. Stage logic is captured at
  build time.

## Diagnostics

- `graph.metrics().placementReport()` — placement / NUMA / native lib
  reporting.
- `DiagnosticsSpec.off().jfr(true)` plus async-profiler / Mission Control for
  hot-path visibility.
- `./gradlew jcstress` to validate edge memory ordering after concurrent
  changes.
