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
why the current local pipeline benchmark shows a large gap between fused and
physical rows.

Fusion is deliberately strict. Expect it only for normal SPSC, blocking,
on-heap, non-batch, non-redirect serial paths without conflicting explicit
pins. Treat capacity, worker placement, and edge-depth visibility as observable
behavior: turning fusion on is a topology decision, not a universal default.

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
  "com.lattice.benchmark.(TopologyBenchmark|ApplesToApplesDisruptorBenchmark|DisruptorBaselineBenchmark).*" \
  -wi 10 -i 10 -f 5 -w 10s -r 10s \
  -jvmArgsAppend "-Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:+DisableExplicitGC" \
  -rf json -rff results/lattice-jmh.json
```

Run GC-profiler measurements separately:

```bash
-prof gc
```

Do not mix profiled and unprofiled throughput rows in the same claim. The
current `docs/benchmark-results/apples-2026-04-26/pipeline-fused-current-isolated-gc.json`
row still shows effectively zero allocation, but its throughput is lower than
the non-profiled fused row because the profiler itself changes the run.

For Disruptor comparisons:

- Compare complete topology semantics, not isolated queue operations.
- Match producer count, consumer count, buffer capacity, payload model, and
  allocation model.
- Include rows where Disruptor is expected to win, especially single shared
  stream and MPSC reference cases.
- Keep pooled mutable payload rows separate from allocating value-transform
  rows.
- Use the standalone Disruptor single-producer baseline when the SPSC apples
  row is anomalous.

## Current Data Caveats

The current public result set under
`docs/benchmark-results/apples-2026-04-26/` is a Windows JDK 21 data set. It
shows the architectural value of fusion and preallocation, but it is not a NUMA
release report.

Known caveats:

- Windows scheduling variance affects confidence intervals and tail behavior.
- GC-profiler overhead changes throughput and should be used primarily for
  allocation evidence.
- Apples-to-apples benchmark rows are only fair when the payload model and
  dependency semantics match the claim.
- The SPSC apples Disruptor row had an anomalously poor result. Prefer
  `docs/benchmark-results/apples-2026-04-26/disruptor-baseline-single.json`
  for the single-producer Disruptor baseline.

The practical reading of the current data is mixed: Lattice is strong when the
compiler can specialize a static topology, while Disruptor remains a strong
baseline for a single shared sequence domain and multi-producer reference
paths.
