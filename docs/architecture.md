# Architecture

This page is the deep-dive companion to the README's "How Lattice Works"
section.

## Build vs Runtime Split

Lattice is split into three phases:

1. **DSL** — the application uses `StaticGraph.builder(...)` to declare
   sources, stages, routing nodes, joins, sinks, and edges.
2. **Compile** — the builder validates the topology and produces a
   `GraphPlan`, which the compiler lowers into a physical runtime plan
   (workers, edges, ownership, fusion decisions).
3. **Run** — workers come up, optional native placement is applied at
   bootstrap, and the steady-state hot path runs without ever returning to
   the compiler.

Build/compile happens once on a single thread, before `start()` returns.
Nothing on the hot path allocates new workers, opens new edges, or rewrites
the plan.

```mermaid
flowchart TB
    subgraph Build["Build time (single thread, before start)"]
        DSL["StaticGraph.builder()<br/>sources · stages · sinks<br/>dispatch · broadcast · partition · join<br/>edges (SPSC | MPSC)"]
        Validate["Validator<br/>type checks · capacity rules<br/>preallocation legality<br/>cycle detection"]
        Plan["GraphPlan<br/>logical topology"]
        Compile["Compiler<br/>SP/SC source specialization<br/>linear fusion eligibility<br/>MPSC vs SPSC physical edge<br/>worker placement plan"]
        Physical["Physical runtime plan<br/>workers · edges · ownership"]
        DSL --> Validate --> Plan --> Compile --> Physical
    end

    subgraph Run["Runtime (steady state, hot path)"]
        Producer["Application thread"]
        Emitter["SourceEmitter<br/>(specialized per source)"]
        FusedChain["Fused linear chain<br/>(eligible source→stage→...→sink<br/>runs on owner thread)"]
        Worker1["Worker A<br/>stage / router"]
        Edge1["Bounded ring edge<br/>(SPSC plain-claim or MPSC CAS)"]
        Worker2["Worker B<br/>stage / join / sink"]
        Sink["Sink callback"]
        Metrics["GraphMetrics ·<br/>StageMetrics ·<br/>EdgeMetrics ·<br/>WaitMetrics"]
        JFR["JFR events<br/>(-Dlattice.jfr=true)"]
        Producer --> Emitter
        Emitter -. "fusion eligible" .-> FusedChain --> Sink
        Emitter -- "physical handoff" --> Worker1 --> Edge1 --> Worker2 --> Sink
        Worker1 --> Metrics
        Edge1 --> Metrics
        Worker2 --> Metrics
        FusedChain --> Metrics
        Metrics -. opt-in .-> JFR
    end

    Physical --> Run
    Native["Optional native backend<br/>(Rust JNI)<br/>CPU affinity · NUMA · first-touch"] -. "during worker bootstrap only" .-> Run
```

## The Compiler Decisions

| Decision | Inputs | Effect |
| --- | --- | --- |
| Source specialization | `SourceMode`, downstream edge shape | Pick SPSC physical ingress when the user contract is single-producer; wire the trusted-fast emit path for non-stamping, non-handle messages. |
| Edge shape selection | Producer count, declared `EdgeSpec` | Upgrade MPSC → SPSC where provably safe; round capacity to a power of two. |
| Linear fusion | Topology shape, payload type, `StageSpec` | When the chain is linear, single-producer, and ownership-balanced, run the chain on the producer thread; remove internal SPSC handoffs. |
| Worker placement | `PinPolicy`, native lib status | Ask the native backend to apply CPU affinity / NUMA preference at bootstrap; degrade to advisory if the lib is missing and `lattice.placement.strict` is unset. |
| Slab handle wiring | Payload type carries handle? | Emit `Retaining` hop variants where needed, `Benign` hop variants otherwise. |

## What Stays Inspectable

Even after fusion, the *logical* graph remains inspectable through
`GraphPlan` and `GraphMetrics`. Per-stage metrics are still recorded (with
`-Dlattice.runtime.fusedLogicalEdgeMetrics=true`); per-edge metrics for
elided physical edges report zero traffic, and the placement report still
names every logical worker. The fusion decision is documented in the plan,
not hidden.

## See Also

- [Graph DSL](graph-dsl.md)
- [Edge Semantics](edge-semantics.md)
- [Source Specialization and Fusion](source-specialization-and-fusion.md)
- [Disruptor Comparison](disruptor-comparison.md)
