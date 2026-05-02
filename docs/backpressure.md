# Backpressure

Backpressure in Lattice is configured per edge through `EdgeSpec.withOverflow(...)`
and per source through `SourceMode`. There is no hidden unbounded buffering.

## Overflow Policies

| Policy | Behavior | Use when |
| --- | --- | --- |
| `block()` | Park the producer until a slot frees up. | Throughput-first, producer can wait. |
| `blockFor(Duration)` | Block up to a deadline; surface `BackpressureException` on timeout. | Latency-bounded producer. |
| `failFast()` | Throw `BackpressureException` immediately on a full ring. | Caller decides what to do with rejected items. |
| `lossy()` | Drop the offered item; surface a metric. | Telemetry where staleness beats backpressure. |
| `coalescing(Combiner)` | Merge the offered item into an existing slot. | Latest-value or accumulator semantics. |
| `redirect(target)` | Push the rejected item onto a sibling edge (e.g. a slow path). | Two-tier services. |

## Sizing

- Pick capacity from the producer's burst tolerance, not steady-state rate.
  Steady-state rate is bounded by the consumer; capacity only buys time.
- Power-of-two only. Round up.
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
overflow events update `EdgeMetrics` counters (offers, accepted, rejected,
dropped, redirected). With graph JFR enabled through
`DiagnosticsSpec.off().jfr(true)`, you also get per-event JFR records suitable
for Mission Control / async-profiler post-processing. See
[Observability](observability.md).
