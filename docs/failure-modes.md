# Failure Modes

Lattice has a small set of well-defined failure modes. Each maps to an
explicit lifecycle transition or a typed exception.

## Build-Time Failures

`StaticGraph.builder(...).build()` throws `GraphBuildException` for:

- duplicate node ids;
- type mismatch between an edge's producer output and consumer input;
- unsupported preallocation reuse topologies (e.g. retaining payload escaping
  an unbalanced broadcast);
- invalid capacities (non-positive, not representable as power-of-two);
- missing edges, dangling sinks, or cycles in nodes that disallow them.

Build-time failures never start workers and never load the native library.

## Runtime Failures

`GraphRuntimeException` wraps non-build-time failures observed by the
runtime. Common causes:

- A stage threw a checked exception not handled by its `StageExceptionHandler`.
- The native backend was requested with strict placement but is not loadable.
- An edge was used after `close()` returned.

## Stage Exceptions

A user stage that throws transitions through its configured
`StageExceptionHandler`:

- `CONTINUE`: the offending item is dropped, metrics record the exception, the
  graph stays running.
- `STOP`: the graph transitions to `FAILED` (fail-stop). Other workers
  proceed to a clean shutdown of accepted items.
- `RETHROW`: re-raise on the worker; for fused chains the exception is
  rethrown to the producer caller as the original `RuntimeException`/`Error`
  (or wrapped in `GraphRuntimeException` for checked failures).

The fused-chain exception path was tightened in 1.0.0: an inline
(producer-thread) failure routes through `RuntimeCoordinator.fail(stage,
cause)` so the lifecycle transitions match a worker-thread failure exactly.

## Backpressure

`BackpressureException` is thrown by `Emitter.emit(...)` when the configured
`OverflowPolicy` is `failFast()` (or `blockFor(Duration)` and the deadline
elapses). It is *not* a runtime failure of the graph — only a signal to the
caller. See [Backpressure](backpressure.md).

## Abort

`graph.abort()` is the documented escape hatch:

1. Close every edge (subsequent offers reject).
2. Interrupt all workers.
3. Await termination.
4. Drain & release any remaining items / slab handles.

Steps 1–3 happen before any draining so consumer workers and abort cannot
race the same plain-claim slot. (This race was fixed in 1.0.0; see
[CHANGELOG](https://github.com/ElevatedDev/Lattice/blob/main/CHANGELOG.md).)

## Native Library Unavailable

Without `libstatic_topology_native`, placement requests are recorded as
advisory in `PlacementStatus` but the graph starts. With
`-Dlattice.placement.strict=true`, `start()` instead fails with
`GraphRuntimeException` and the graph never reaches `RUNNING`.

## Slab Leaks

`SlabPool.leakedCount()` returns acquired-minus-released. A persistent
non-zero value after `close()` + `awaitTermination` indicates a custom stage
holding a handle past its lifetime, or a routing topology the compiler should
have rejected (please file an issue with the graph shape).
