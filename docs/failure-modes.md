# Failure Modes

Lattice has a small set of well-defined failure modes. Each maps to an
explicit lifecycle transition or a typed exception.

## Build-Time Failures

`StaticGraph.builder(...).build()` throws `GraphBuildException` for:

- duplicate node ids;
- type mismatch between an edge's producer output and consumer input;
- unsupported preallocation reuse topologies;
- invalid capacities;
- missing edges, dangling sinks, or cycles in nodes that disallow them;
- unsafe broadcast copy shapes or oversized weighted dispatch schedules.

Build-time failures never start workers and never load the native library.

## Runtime Failures

`GraphRuntimeException` wraps non-build-time failures observed by the runtime.
Common causes:

- a stage failed and the exception handler selected fail-graph semantics;
- the native backend was requested with strict placement but is not loadable;
- an edge or emitter was used after `close()` returned.

## Stage Exceptions

A user stage that throws transitions through its configured
`StageExceptionHandler`:

- `FAIL_GRAPH`: the graph transitions to `FAILED`; workers stop and accepted
  items are cleaned up after quiescence.
- `POISON_STAGE`: the failing stage is stopped, its edges are closed, and
  queued cleanup is deferred until workers are stopped. This is a control-plane
  transition, not a concurrent queue drain.

Inline source fusion is disabled when a custom handler is installed. With the
default fail-graph handler, inline producer-thread failures are attributed to
the actual fused stage and transition the graph to `FAILED`.

## Backpressure

`BackpressureException` is thrown by `Emitter.emit(...)` when the configured
`OverflowPolicy` is `failFast()` or `blockFor(Duration)` and the deadline
elapses. It is not a runtime failure of the graph; it is a signal to the
caller. See [Backpressure](backpressure.md).

## Abort

`graph.abort()` is the documented escape hatch:

1. Close every edge so subsequent offers reject.
2. Interrupt all workers.
3. Await termination.
4. Drain and release remaining items only after workers are quiescent.

If workers do not terminate, abort returns without draining; cleanup is left to
the final worker-stop path. For `SourceMode.SINGLE_PRODUCER`, application
producers must also be externally serialized with close, stop, and abort. Use
the default multi-producer source mode when foreign lifecycle calls may race
emits.

## Native Library Unavailable

Without `libstatic_topology_native`, placement requests are recorded as
advisory in `PlacementStatus` but the graph starts. With
`-Dlattice.placement.strict=true`, `start()` instead fails with
`GraphRuntimeException` and the graph never reaches `RUNNING`.

## Slab Leaks

`SlabPool.leakedCount()` returns acquired-minus-released. A persistent non-zero
value after `close()` plus `awaitTermination` indicates a custom stage holding
a handle past its lifetime, or a routing topology the compiler should have
rejected.
