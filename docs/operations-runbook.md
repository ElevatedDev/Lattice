# Operations Runbook

Day-2 guidance for running Lattice graphs in production.

## Startup Checks

1. JVM args set as documented in [Observability](observability.md) — turn off
   hot counters and stage histograms unless you actively need them.
2. If using the native backend, confirm `java.library.path` resolves
   `libstatic_topology_native.{so,dylib,dll}` and that
   `graph.metrics().placementStatus()` reports the requested CPU sets.
3. Use `-Dlattice.placement.strict=true` in production when affinity is
   required; otherwise placement requests are advisory.
4. Verify edge capacities are power-of-two and slab pools are sized per
   [Backpressure §Sizing](backpressure.md).

## Healthy Steady-State Signals

- `EdgeMetrics.queueDepthSample()` stays well below capacity.
- `EdgeMetrics.rejected()` / `dropped()` / `redirected()` flat or growing
  proportionally to known load.
- `WaitMetrics.parks()` consistent with the configured wait strategy.
- `StageMetrics.exceptions()` flat.
- `PlacementStatus.effectiveCpuSet()` matches `requestedCpuSet()` on every
  worker (when strict placement is enabled).

## Lifecycle Operations

| Action | API | Behavior |
| --- | --- | --- |
| Graceful stop | `graph.close()` | Closes sources, drains accepted items, awaits workers. |
| Drain wait | `graph.awaitTermination(Duration)` | Blocks until workers stop or deadline elapses. |
| Quiesce / resume | `graph.quiesce()` / `graph.resume()` | Pauses worker progress without tearing down. |
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

- `graph.metrics().placementStatus()` — placement / NUMA / native lib
  reporting.
- `-Dlattice.jfr=true` plus async-profiler / Mission Control for hot-path
  visibility.
- `./gradlew jcstress` to validate edge memory ordering after concurrent
  changes.
