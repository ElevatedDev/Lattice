# Performance Tuning

This guide is for configuring Lattice graphs and interpreting benchmark
results. The short version: use SPSC whenever ownership proves there is one
producer, turn on fusion only for eligible fixed serial segments, use
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
- Serial source -> stage -> stage -> sink segment:
  `-Dlattice.fusion.enabled=true`, after validating fusion eligibility.
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
partition, joins, redirects, lossy overflow, and edge batch policy are rejected
inside the preallocated reuse domain.

Pool sizing should exceed the maximum in-flight window of the compiled
physical plan. Fusion matters here: elided internal edges do not add to the
reuse bound. When in doubt, start with at least twice the ingress edge capacity
and validate with the compiler and stress tests.

## Fusion

Enable fusion with:

```bash
-Dlattice.fusion.enabled=true
```

Fusion is useful for serial static segments such as:

```text
source -> stage -> sink
source -> stageA -> stageB -> stageC -> sink
```

When eligible, the runtime elides internal SPSC handoffs and executes the
segment on the owner worker while preserving logical graph visibility. This is
why the current baseline keeps physical and fused pipeline rows separate.

Fusion is deliberately strict. Expect it only for normal SPSC, blocking,
on-heap, non-batch, non-redirect serial paths without conflicting explicit
pins. Treat capacity, worker placement, and edge-depth visibility as observable
behavior: turning fusion on is a topology decision, not a universal default.

### Inline source-side fusion (default-on, eligibility-gated)

For static topologies whose ingress is owned by a single producer (typically a
benchmark or a hot-path application thread that is already the natural source),
the runtime additionally elides the source -> fused-worker SPSC handoff and
executes the entire fused chain on the producer thread. This is enabled by
default and engages automatically when eligibility holds:

```bash
# Default. Disable explicitly only if the producer thread must remain isolated
# from the stage and sink work.
-Dlattice.fusion.inlineSource=true
```

Eligibility is strict and conservative:

- `lattice.fusion.enabled=true` must be in effect (default).
- The source declares `SourceMode.SINGLE_PRODUCER`.
- The source is not preallocated and not stamped.
- The source's payload type is not `Object`, `SlabHandle`, or `Stamped`
  (i.e. cannot transitively carry a slab handle).
- The source has exactly one outgoing edge, and that edge is fusible (SPSC,
  BLOCK, on-heap slots, no batch policy).
- The downstream worker has a fused stage chain or a fused stage->sink chain
  and exactly one input edge.

When all conditions hold, the consumer worker thread parks immediately after
bootstrap; emit calls run the entire chain synchronously on the calling
thread. **Backpressure is not available** in this mode because there is no
ring to fill: each emit either runs the chain to completion or throws.

Because eligibility requires `SourceMode.SINGLE_PRODUCER` (a correctness
contract the application explicitly opted in to), turning this on by default
does not change semantics for graphs that did not declare single-producer
ingress. Disable it explicitly if your producer thread cannot tolerate doing
the stage and sink work synchronously (for example because it must remain
free for placement reasons).

The current checked-in isolated stage baseline records
`latticeThreeStagePipelineFused` at 61,838,846 ops/s,
`latticeThreeStagePipelineFusedReference` at 52,698,325 ops/s, and
`latticeThreeStagePipelinePhysical` at 27,660,948 ops/s. The completed optimal
path is tracked separately so publish throughput is not confused with
completed-operation throughput.

## Batch Size

There are two batching knobs:

- `BatchPolicy.maxItems(n)` and `BatchPolicy.linger(...)` for batch stages.
- `-Dlattice.runtime.singleMessageBatchSize=<n>` for how many single-message
  items a worker drains per turn.

The default single-message drain batch is `64`. Increase it when throughput is
limited by worker loop overhead and the workload tolerates longer per-turn
residence time. Decrease it toward `1` when testing strict handoff behavior or
when tail latency matters more than peak throughput.

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

Hot counters are enabled by default. They are useful operational telemetry, but
they add work to the hottest paths. For throughput-only benchmarks:

```bash
-Dlattice.metrics.hotCounters=false
-Dlattice.metrics.stageHistograms=false
-Dlattice.metrics.residence=false
-Dlattice.jfr=false
```

For observability runs:

```bash
-Dlattice.metrics.hotCounters=true
-Dlattice.metrics.stageHistograms=true
-Dlattice.metrics.residence=true
-Dlattice.jfr=true
-XX:StartFlightRecording=filename=lattice.jfr,settings=profile,dumponexit=true
```

Residence timing adds timestamp reads on offer/poll paths. Stage histograms and
JFR batch events can also change measured throughput. Keep observability runs
separate from max-throughput runs unless the claim is explicitly "with
observability enabled."

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

```bash
-Dlattice.placement.strict=true
```

Use topology-aware startup placement as an opt-in helper, not a replacement for
explicit production pinning:

```bash
-Dlattice.placement.topologyAware.enabled=true
```

Placement-sensitive claims should be made on Linux with the native backend
loaded. Record CPU ids, NUMA nodes, process affinity, kernel version, CPU
governor, JVM version, and `GraphMetrics.placementReport()`.

For comparison-only benchmark runs, document whether first-touch behavior was
left enabled or disabled with:

```bash
-Dlattice.firstTouch.enabled=false
```

## Recommended Property Sets

Max-throughput, low-observability run:

```bash
-Dlattice.fusion.enabled=true
-Dlattice.runtime.singleMessageBatchSize=64
-Dlattice.metrics.hotCounters=false
-Dlattice.metrics.stageHistograms=false
-Dlattice.metrics.residence=false
-Dlattice.jfr=false
-Xms4g -Xmx4g
-XX:+AlwaysPreTouch
-XX:+UseG1GC
-XX:+DisableExplicitGC
```

Placement-sensitive max-throughput run on Linux:

```bash
-Djava.library.path=native/static-topology-native/target/release
-Dlattice.placement.strict=true
-Dlattice.placement.topologyAware.enabled=true
-Dlattice.fusion.enabled=true
-Dlattice.runtime.singleMessageBatchSize=64
-Dlattice.metrics.hotCounters=false
-Dlattice.metrics.stageHistograms=false
-Dlattice.metrics.residence=false
-Dlattice.jfr=false
-Xms4g -Xmx4g
-XX:+AlwaysPreTouch
-XX:+UseG1GC
-XX:+DisableExplicitGC
```

Observability and diagnostics run:

```bash
-Dlattice.metrics.hotCounters=true
-Dlattice.metrics.stageHistograms=true
-Dlattice.metrics.residence=true
-Dlattice.jfr=true
-Dlattice.placement.strict=true
-XX:StartFlightRecording=filename=lattice.jfr,settings=profile,dumponexit=true
```

## Benchmark Methodology

Use JMH for throughput and allocation evidence. Build the benchmark jar, run
multiple forks, and store JSON results with the exact JVM flags:

```bash
./gradlew clean test jmhJar
java -jar build/libs/lattice-1.0-SNAPSHOT-jmh.jar \
  "com.lattice.benchmark.(TopologyBenchmark|ApplesToApplesDisruptorBenchmark|OptimalPathBenchmark|DisruptorBaselineBenchmark).*" \
  -wi 5 -i 8 -f 3 -w 5s -r 5s \
  -jvmArgsAppend "-Xms2g -Xmx2g -XX:+AlwaysPreTouch -XX:+UnlockDiagnosticVMOptions -XX:+UseParallelGC -Dlattice.fusion.enabled=true -Dlattice.fusion.inlineSource=true -Dlattice.metrics.hotCounters=false -Dlattice.metrics.residence=false -Dlattice.metrics.stageHistograms=false -Dlattice.runtime.fusedLogicalEdgeMetrics=false -Dlattice.runtime.inlineDepthTracking=false" \
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
[`docs/benchmark-results/v1.0.0-baseline/`](docs/benchmark-results/v1.0.0-baseline/)
is a JDK 21 data set refreshed on 2026-04-29. It shows the architectural value
of source specialization, equal-call-site manual fusion, and inline fusion.

Scope rules:

- GC-profiler overhead changes throughput and should be used primarily for
  allocation evidence.
- Apples-to-apples benchmark rows are only fair when the payload model and
  dependency semantics match the claim.
- Publish-throughput rows are not completed-operation rows. Use
  `optimal-path-completed.json` when completion matters.
- Treat single-producer Disruptor rows as baseline context for shared-ring
  workloads, not as proof about every static graph shape.

The practical reading of the current data is that Lattice is strongest when the
compiler can specialize and inline a static topology. The isolated physical,
inline-fused, reference-framed, equal-call-site reference, and completion-gated
rows all put Lattice ahead by point estimate; the physical row has overlapping
JMH error bars, while the fused and completion-gated rows are clearer. Disruptor
remains a strong baseline for a single shared sequence domain, so keep the raw
artifacts and topology semantics attached to any comparison.
