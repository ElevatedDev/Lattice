# Graph DSL

`com.lattice.graph.StaticGraph.builder(name)` returns a fluent builder. Calls
declare the *logical* topology; the compiler decides the *physical* runtime
plan.

## Nodes

| Method | Purpose |
| --- | --- |
| `.source(id, payloadType, SourceMode)` | Declares an external entry. `SINGLE_PRODUCER` is a correctness contract: at most one application thread emits at a time. `MULTI_PRODUCER` allows concurrent emitters. |
| `.preallocatedSource(id, payloadType, PreallocationSpec)` | Declares a single-producer source whose payload objects come from a checked reuse pool. |
| `.stampedSource(id, payloadType, SourceMode)` | Declares a source that wraps emissions in `Stamped` values for joins. |
| `.stage(id, in, out, StageLogic, StageSpec)` | A user computation. `StageSpec.singleThreaded()` pins to a single worker. |
| `.batchStage(id, in, out, BatchStageLogic, StageSpec)` | Stage that consumes a `Batch<T>` per invocation. The `StageSpec` must include a batch policy. |
| `.sink(id, payloadType, Consumer, StageSpec)` | Terminal node. |

## Routing

| Method | Purpose |
| --- | --- |
| `.dispatch(id, in, DispatchSpec, StageSpec)` | Selects exactly one downstream by key, weight, or round-robin policy. |
| `.broadcast(id, in, BroadcastSpec, StageSpec)` | Replicates each item to every downstream. |
| `.partition(id, in, PartitionSpec, StageSpec)` | Hash-partitions to N lanes. |
| `.join(id, out, JoinSpec, StageSpec)` | Correlates by stamp across multiple inputs with explicit duplicate, timeout, and missing-branch policy. |

## Edges

`.edge(from, to, EdgeSpec)` connects two nodes. The compiler may upgrade an
MPSC declaration to SPSC when the producer side is provably single-threaded,
or fuse the edge away entirely when [linear fusion](source-specialization-and-fusion.md)
applies.

```java
EdgeSpec.spscRing(1024)
EdgeSpec.mpscRing(1024)
EdgeSpec.spscRing(1024).wait(WaitSpec.blocking())
EdgeSpec.mpscRing(1024).overflow(OverflowPolicy.failFast())
```

See [Edge Semantics](edge-semantics.md) for capacity rules and handle ownership.

## Graph Runtime Specs

Graph-wide runtime controls are optional builder calls. They apply only to the
graph being built:

```java
StaticGraph.builder("orders")
    .fusion(FusionSpec.defaults().inlineSources(true))
    .metrics(MetricsSpec.off().hotCounters(true))
    .placement(GraphPlacementSpec.off().strict(true))
    .diagnostics(DiagnosticsSpec.off().jfr(true));
```

Defaults are conservative for observability overhead and source execution:
normal downstream fusion is enabled; metrics, source inline, source physical
path elision, topology-aware placement, strict placement, first-touch placement,
and JFR are off.

## Configuration Knobs

| Surface | Common knobs | Default | Cost / tradeoff |
| --- | --- | --- | --- |
| `StageSpec` | `.wait(...)`, `.pin(...)`, `.batch(...)` | Single-threaded, phased wait, no pin, no batch. | Wait and pin choices affect CPU residency and scheduling; batch policy can improve throughput while adding fill latency. |
| `EdgeSpec` | `.overflow(...)`, `.wait(...)`, `.memory(...)`, `.batch(...)` | Blocking overflow, phased wait, on-heap slots, no batch. | Overflow policy defines caller behavior under load; residence timing and off-heap handle metadata have explicit costs. |
| `FusionSpec` | `.inlineSources(...)`, `.elideInlineSourcePhysicalPath(...)`, `.validateTypes(...)` | Downstream fusion on; source-inline and elision off. | Source-inline can remove a handoff but moves stage/sink work onto the producer thread and removes ring backpressure. |
| `MetricsSpec` | `.hotCounters(...)`, `.fusedLogicalEdgeCounters(...)`, `.stageHistograms(...)`, `.residenceTiming(...)` | Off. | Enables operational visibility at the cost of counter, timestamp, or histogram work on hot paths. |
| `GraphPlacementSpec` | `.topologyAware(...)`, `.strict(...)`, `.firstTouch(...)` | Off. | Native placement is Linux-strongest and advisory unless strict mode is enabled. |
| `DiagnosticsSpec` | `.jfr(...)` | Off. | Emits Lattice JFR events when a JVM recording enables the event types. |
| `exceptionHandler(...)` | Graph-level `StageExceptionHandler` | Fail-stop default. | Custom policy can continue or replace failed messages, but disables source-inline fusion for the affected graph. |

## Lifecycle

```java
StaticGraph g = builder.build(); // validates and compiles
g.start();                       // workers come up, placement applied
g.emitter("ingress", Order.class).emit(...);
g.close();                       // close all sources, drain accepted items
g.awaitTermination(Duration.ofSeconds(5));
g.abort();                       // fail-fast; no drain promise
```

`build()` may throw `GraphBuildException`; runtime failures surface as
`GraphRuntimeException`. See [Failure Modes](failure-modes.md).
