# Performance Tuning

This guide is for configuring Lattice graphs and interpreting benchmark
results. The short version: use SPSC whenever ownership proves there is one
producer, leave normal fusion enabled for eligible fixed serial segments, opt
into source inline only when the producer thread may run stage logic, use
preallocated sources for steady-state object reuse, and measure with the same
payload and observability settings you plan to claim.

## Starting Defaults

For a normal production graph, start conservative:

```java
StageSpec.singleThreaded();
EdgeSpec.mpscRing(8192);
EdgeSpec.spscRing(8192);
```

Then tune from the topology:

- External source called by many application threads: `EdgeSpec.mpscRing(...)`.
- External source called by exactly one application thread:
  `source(..., SourceMode.SINGLE_PRODUCER)`.
- Worker-to-worker edge with one upstream worker: `EdgeSpec.spscRing(...)`.
- Serial source -> stage -> stage -> sink segment: default fusion is already
  enabled; use `FusionSpec.disabled()` only when you need the physical baseline.
- Allocation-sensitive source path:
  `preallocatedSource(...)` with a pool larger than the compiled reuse bound.

## SPSC Versus MPSC

Use SPSC when there is exactly one producer and one consumer for the physical
edge. This is the common worker-to-worker case and the fastest path because the
edge does not need multi-producer reservation.

Use MPSC when multiple application threads can emit into the same source or
when producer ownership is not mechanically obvious. MPSC is the safe public
default for external ingress.

Single-producer sources are the bridge between those two worlds:

```java
.source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
.edge("ingress", "validate", EdgeSpec.mpscRing(8192))
```

The edge can remain MPSC in the DSL for readability and compatibility. The
compiler rewrites the physical source ingress edge to SPSC when
`SourceMode.SINGLE_PRODUCER` proves that only one external producer exists.

Do not mark a source single-producer if more than one thread can call its
emitter. That is a correctness contract, not just a performance hint.

## Preallocated Sources

Use `preallocatedSource(...)` when the source payload can be reused and
steady-state allocation is part of the performance budget.

```java
.preallocatedSource(
    "ingress",
    MutableOrder.class,
    PreallocationSpec.pool(ignored -> new MutableOrder()).poolSize(16_384)
)
```

The current v1 contract is intentionally narrow: single-producer, non-stamped,
linear, blocking, on-heap, single-message source domains. Routing, broadcast,
partition, joins, redirects, drop/coalesce overflow, and edge batch policy are
rejected inside the preallocated reuse domain.

That narrowness is the point. Lattice should either prove payload reuse is safe
for the compiled graph or reject the graph at build time; it should not depend
on every stage author remembering an informal ownership rule.

Pool sizing should exceed the maximum in-flight window of the compiled
physical plan. Fusion matters here: elided internal edges do not add to the
reuse bound. When in doubt, start with at least twice the ingress edge capacity
and validate with the compiler and stress tests.

## Fusion

Normal downstream fusion is enabled by default:

```java
StaticGraph.builder("orders")
    .fusion(FusionSpec.defaults())
    // source/stage/sink/edge declarations...
    .build();
```

Fusion is useful for serial static segments such as:

```text
source -> stage -> sink
source -> stageA -> stageB -> stageC -> sink
```

When eligible, the runtime elides internal SPSC handoffs and executes downstream
stages/sinks on the owner worker while preserving logical graph visibility. This
is why the current baseline keeps physical and fused pipeline rows separate.

Fusion is deliberately strict. Expect it only for normal SPSC, blocking,
on-heap, non-batch, non-redirect serial paths without conflicting explicit
pins. Treat capacity, worker placement, and edge-depth visibility as observable
behavior.

Force the physical baseline per graph with:

```java
StaticGraph.builder("orders")
    .fusion(FusionSpec.disabled())
    .build();
```

### Inline Source-Side Fusion

For static topologies whose ingress is owned by a single producer (typically a
benchmark or a hot-path application thread that is already the natural source),
the runtime additionally elides the source -> fused-worker SPSC handoff and
executes the entire fused chain on the producer thread. This is off by default
because it changes which thread executes stage and sink logic. Enable it per
graph:

```java
StaticGraph.builder("orders")
    .fusion(FusionSpec.defaults()
        .inlineSources(true))
    .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
    .build();
```

For the lowest-latency eligible path, the runtime can also remove the
lifecycle-only owner worker and source ingress edge:

```java
StaticGraph.builder("orders")
    .fusion(FusionSpec.defaults()
        .inlineSources(true)
        .elideInlineSourcePhysicalPath(true))
    .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
    .build();
```

Eligibility is strict and conservative:

- `FusionSpec.enabled(true)` must be in effect (the default).
- The source declares `SourceMode.SINGLE_PRODUCER`.
- The source is not preallocated and not stamped.
- The source's payload type is not `Object`, `SlabHandle`, or `Stamped`
  (i.e. cannot transitively carry a slab handle).
- The source has exactly one outgoing edge, and that edge is fusible (SPSC,
  BLOCK, on-heap slots, no batch policy).
- The downstream worker has a fused stage chain or a fused stage->sink chain
  and exactly one input edge.
- No custom `StageExceptionHandler` is installed.
- No explicit effective placement or topology-aware placement applies to the
  inline chain. Pinned/topology-placed graphs keep the source boundary physical
  so stage logic still runs on the placed owner worker.

When all conditions hold, the consumer worker thread parks immediately after
bootstrap; emit calls run the entire chain synchronously on the calling
thread. **Backpressure is not available** in this mode because there is no
ring to fill: each emit either runs the chain to completion or throws.

Because eligibility requires `SourceMode.SINGLE_PRODUCER` (a correctness
contract the application explicitly opted in to), the runtime does not infer
producer-thread ownership. Do not enable source inline if the producer thread
must remain isolated from stage/sink work.

The current checked-in 2026-05-02 stage baseline from
`three-stage-scoped-2026-05-02.json` records publish throughput with 3 forks,
5x5s warmup, and 8x5s measurement:
`latticeThreeStagePipelineFused` at 127,875,286 ops/s,
`latticeManuallyFusedReference` at 209,168,722 ops/s, and
`latticeThreeStagePipelinePhysical` at 31,938,529 ops/s. The completed optimal
path is tracked separately so publish throughput is not confused with
completed-operation throughput.

## Batch Size

Batching is configured through graph APIs:

- `BatchPolicy.maxItems(n)` and `BatchPolicy.linger(...)` for batch stages and
  batch-capable edges.
- The internal single-message drain batch is fixed at `64` in this release.

For batch stages, `BatchPolicy.maxItems(n)` is the low-latency batching option.
`BatchPolicy.linger(...)` can improve fill rate under bursty load but adds
intentional waiting.

## Wait Policies

Wait policies are configured on stages and edges:

```java
StageSpec.singleThreaded()
    .wait(WaitSpec.phased(10_000, 50, Duration.ofNanos(500)));

EdgeSpec.spscRing(8192)
    .wait(WaitSpec.busySpin());
```

Use `WaitSpec.busySpin()` only on reserved cores where burning CPU is expected.
It minimizes wakeup latency but can starve other work and distort benchmarks on
shared machines.

Use `WaitSpec.phasedDefault()` or a custom `WaitSpec.phased(...)` for most
low-latency services. It spins first, yields for a bounded period, then parks.

Use `WaitSpec.blocking()` when CPU efficiency matters more than microsecond
wakeup behavior, or when the graph runs in an environment without reserved
cores.

## Metrics And JFR

Metrics are off by default. They are useful operational telemetry, but they add
work to hot paths and can change benchmark results. Enable them per graph:

```java
StaticGraph.builder("orders")
    .metrics(MetricsSpec.off()
        .hotCounters(true)
        .fusedLogicalEdgeCounters(true)
        .stageHistograms(true)
        .residenceTiming(true))
    .build();
```

JFR event emission is also per graph:

```java
StaticGraph.builder("orders")
    .diagnostics(DiagnosticsSpec.off().jfr(true))
    .build();
```

Use normal JVM recording controls to capture the events:

```bash
-XX:StartFlightRecording=filename=lattice.jfr,settings=profile,dumponexit=true
```

Residence timing adds timestamp reads on offer/poll paths. Stage histograms,
logical fused-edge counters, hot counters, and JFR batch events can also change
measured throughput. Keep observability runs separate from max-throughput runs
unless the claim is explicitly "with observability enabled."

## Native Placement

Placement is optional. Without the native library, requested placement degrades
through startup diagnostics and metrics by default.

Build the Rust JNI backend:

```bash
./gradlew nativeBuildRelease
```

Run with the library visible:

```bash
java -Djava.library.path=native/static-topology-native/target/release ...
```

Use strict mode when placement is part of correctness or benchmark evidence:

```java
StaticGraph.builder("orders")
    .placement(GraphPlacementSpec.off().strict(true))
    .build();
```

Use topology-aware startup placement as an opt-in helper, not a replacement for
explicit production pinning:

```java
StaticGraph.builder("orders")
    .placement(GraphPlacementSpec.off().topologyAware(true))
    .build();
```

Placement-sensitive claims should be made on Linux with the native backend
loaded. Record CPU ids, NUMA nodes, process affinity, kernel version, CPU
governor, JVM version, and `GraphMetrics.placementReport()`.

For comparison-only benchmark runs, document whether first-touch behavior was
left enabled or disabled with:

```java
StaticGraph.builder("orders")
    .placement(GraphPlacementSpec.off().firstTouch(false))
    .build();
```

To compare the same pinned topology with native placement disabled versus
enabled, use `NativePlacementComparisonBenchmark`. The disabled method forces
`-Dlattice.native.enabled=false`; the enabled method runs with strict placement
and requires the library path to be supplied to the forked JVM:

```bash
./gradlew nativeBuildRelease jmhJar
java -jar build/libs/lattice-1.0-SNAPSHOT-jmh.jar \
  "io.github.elevateddev.lattice.benchmark.NativePlacementComparisonBenchmark.*" \
  -p pinPolicy=none,cpu,core,cpuSet,numaNode,inheritCpuset \
  -jvmArgsAppend "-Dlattice.native.library.path=$(pwd)/native/static-topology-native/target/release/libstatic_topology_native.so -Dlattice.bench.cpuA=0 -Dlattice.bench.cpuB=1 -Dlattice.bench.cpuC=2"
```

## Recommended Profiles

Max-throughput, low-observability run:

```java
StaticGraph.builder("orders")
    .fusion(FusionSpec.defaults())
    .metrics(MetricsSpec.off())
    .diagnostics(DiagnosticsSpec.off())
    .build();
```

```bash
-Xms4g -Xmx4g
-XX:+AlwaysPreTouch
-XX:+UseG1GC
-XX:+DisableExplicitGC
```

Placement-sensitive max-throughput run on Linux:

```java
StaticGraph.builder("orders")
    .fusion(FusionSpec.defaults())
    .metrics(MetricsSpec.off())
    .placement(GraphPlacementSpec.off()
        .strict(true)
        .topologyAware(true))
    .build();
```

```bash
-Djava.library.path=native/static-topology-native/target/release
-Xms4g -Xmx4g
-XX:+AlwaysPreTouch
-XX:+UseG1GC
-XX:+DisableExplicitGC
```

The checked-in public baseline uses `-XX:+UseParallelGC`, not the production
profile examples above. Do not compare a G1GC or different-heap run with the
published ParallelGC baseline without rerunning both sides under the same JVM
profile.

Observability and diagnostics run:

```java
StaticGraph.builder("orders")
    .metrics(MetricsSpec.off()
        .hotCounters(true)
        .fusedLogicalEdgeCounters(true)
        .stageHistograms(true)
        .residenceTiming(true))
    .placement(GraphPlacementSpec.off().strict(true))
    .diagnostics(DiagnosticsSpec.off().jfr(true))
    .build();
```

```bash
-XX:StartFlightRecording=filename=lattice.jfr,settings=profile,dumponexit=true
```

## Benchmark Methodology

Use JMH for throughput and allocation evidence. Build the benchmark jar, run
multiple forks, and store JSON results with the exact JVM flags:

```bash
./gradlew clean test jmhJar
java -jar build/libs/lattice-1.0-SNAPSHOT-jmh.jar \
  "io.github.elevateddev.lattice.benchmark.(TopologyBenchmark|ApplesToApplesDisruptorBenchmark|OptimalPathBenchmark|DisruptorBaselineBenchmark).*" \
  -wi 5 -i 8 -f 3 -w 5s -r 5s \
  -jvmArgsAppend "-Xms2g -Xmx2g -XX:+AlwaysPreTouch -XX:+UnlockDiagnosticVMOptions -XX:+UseParallelGC" \
  -rf json -rff results/lattice-jmh.json
```

Run GC-profiler measurements separately:

```bash
-prof gc
```

Do not mix profiled and unprofiled throughput rows in the same claim. GC
profiler rows can still prove allocation behavior, but their throughput is not
directly comparable to non-profiled rows because the profiler itself changes
the run.

For Disruptor comparisons:

- Compare complete topology semantics, not isolated queue operations.
- Match producer count, consumer count, buffer capacity, payload model, and
  allocation model.
- Use completion-gated rows such as `OptimalPathBenchmark` when the claim is
  completed operation throughput rather than enqueue/publish throughput.
- Include rows that exercise favorable Disruptor shapes, especially single
  shared stream and MPSC reference cases.
- Keep pooled mutable payload rows separate from allocating value-transform
  rows.
- Use the standalone Disruptor single-producer baseline when isolating a
  shared-ring result from graph-topology effects.

## Current Data Scope

The current public result set under
[`docs/benchmark-results/2026-05-02-per-graph-refresh/`](docs/benchmark-results/2026-05-02-per-graph-refresh/)
is a JDK 21 data set refreshed after the per-graph runtime API migration. It
shows the architectural value of source specialization, equal-call-site manual
fusion, inline fusion, completion-gated comparison, and the allocation profile
of the optimal path.

Scope rules:

- GC-profiler overhead changes throughput and should be used primarily for
  allocation evidence.
- Apples-to-apples benchmark rows are only fair when the payload model and
  dependency semantics match the claim.
- Publish-throughput rows are not completed-operation rows. Use
  `optimal-path-completed-2026-05-02.json` when completion matters.
- Treat single-producer Disruptor rows as baseline context for shared-ring
  workloads, not as proof about every static graph shape.

The practical reading of the current data is that Lattice is strongest when the
compiler can specialize and inline a static topology. The headline physical,
inline-fused, equal-call-site reference, and completion-gated rows put Lattice
ahead by point estimate. The broader end-to-end matrix is mixed: physical
source/sink and routing-heavy shapes can favor Disruptor, while the eligible
static linear pipeline is the Lattice fast path. Keep the raw artifacts and
topology semantics attached to any comparison.
