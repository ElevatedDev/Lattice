# Backpressure

Backpressure in Lattice is configured per edge through
`EdgeSpec.overflow(...)` and per source through `SourceMode`. There is no
hidden unbounded buffering.

## Overflow Policies

| Policy | Behavior | Use when |
| --- | --- | --- |
| `block()` | Park the producer until a slot frees up. | Throughput-first, producer can wait. |
| `blockFor(Duration)` | Block up to a deadline; surface `BackpressureException` on timeout. | Latency-bounded producer. |
| `failFast()` | Throw `BackpressureException` immediately on a full ring. | Caller decides what to do with rejected items. |
| `dropLatest()` / `dropNewest()` | Drop the offered item; surface a metric. | Telemetry where staleness beats backpressure. |
| `dropOldest()` | Drop the oldest queued item to admit the offered item. | Latest-value streams on MPSC ingress. |
| `coalesceBy(Function)` | Merge full-edge offers by caller-supplied key. | Latest-value or accumulator semantics. |
| `redirectTo(target)` | Push the rejected item onto a sibling node. | Two-tier services. |

Example:

```java
EdgeSpec.mpscRing(1024)
    .overflow(OverflowPolicy.blockFor(Duration.ofMillis(1)));
```

## Sizing

- Pick capacity from the producer's burst tolerance, not steady-state rate.
  Steady-state rate is bounded by the consumer; capacity only buys time.
- Capacity must be a power of two. The graph build fails with
  `GraphBuildException` rather than rounding silently; MPSC capacity must be
  at least `2`.
- Slab pools should be sized to `sum(edge capacities along the longest
  retaining path) + 1` per concurrent emitter.

## Producer Side

- `Emitter.emit(T)` honors the configured policy.
- `Emitter.tryEmit(T)` is a non-blocking offer that returns `false` instead of
  throwing on a full ring; useful when the policy is `block()` but the caller
  wants to make a decision.
- `Emitter.emit(T, Duration)` is a time-bounded variant.

## Visibility

When graph hot counters are enabled through `MetricsSpec.hotCounters(true)`,
overflow events update `EdgeMetrics` counters such as `failedOffers()`,
`blockedOffers()`, `droppedLatest()`, `droppedOldest()`,
`coalescedOffers()`, and `redirectedOffers()`. With graph JFR enabled through
`DiagnosticsSpec.off().jfr(true)`, you also get per-event JFR records suitable
for Mission Control / async-profiler post-processing. See
[Observability](observability.md).
