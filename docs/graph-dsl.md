# Graph DSL

`com.lattice.graph.StaticGraph.builder(name)` returns a fluent builder. Calls
declare the *logical* topology; the compiler decides the *physical* runtime
plan.

## Nodes

| Method | Purpose |
| --- | --- |
| `.source(id, payloadType, SourceMode)` | Declares an external entry. `SINGLE_PRODUCER` is a correctness contract: at most one application thread emits at a time. `MULTI_PRODUCER` allows concurrent emitters. |
| `.stage(id, in, out, StageLogic, StageSpec)` | A user computation. `StageSpec.singleThreaded()` pins to a single worker. |
| `.batchStage(id, in, out, BatchStageLogic, BatchPolicy, StageSpec)` | Stage that consumes a `Batch<T>` per invocation. |
| `.sink(id, payloadType, Consumer, StageSpec)` | Terminal node. |

## Routing

| Method | Purpose |
| --- | --- |
| `.dispatch(id, in, KeyExtractor, ...)` | Selects exactly one downstream by key. |
| `.broadcast(id, in, ...)` | Replicates each item to every downstream. |
| `.partition(id, in, KeyExtractor, lanes, ...)` | Hash-partitions to N lanes. |
| `.join(id, StampExtractor, JoinPolicy, Combiner, ...)` | Correlates by stamp across multiple inputs with explicit duplicate, timeout, and missing-branch policy. |

## Edges

`.edge(from, to, EdgeSpec)` connects two nodes. The compiler may upgrade an
MPSC declaration to SPSC when the producer side is provably single-threaded,
or fuse the edge away entirely when [linear fusion](source-specialization-and-fusion.md)
applies.

```java
EdgeSpec.spscRing(1024)
EdgeSpec.mpscRing(1024)
EdgeSpec.spscRing(1024).withWait(WaitSpec.parking())
EdgeSpec.mpscRing(1024).withOverflow(OverflowPolicy.failFast())
```

See [Edge Semantics](edge-semantics.md) for capacity rules and handle ownership.

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

