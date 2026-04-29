# Lattice Publication Benchmark Baseline

This document summarizes the checked-in benchmark baseline under
[`benchmarks/baseline/`](benchmarks/baseline/). It is the current public
results snapshot for regression review, open-source transparency, and research
comparison.

These numbers were captured on 2026-04-29 with the flags listed below. Keep
the raw JMH JSON and stdout logs with any quoted number.

## Validation Profile

| Property | Value |
| --- | --- |
| CPU | Intel Core i7-7700 @ 3.60 GHz, 4 cores / 8 threads |
| NUMA | 1 node, CPUs 0-7 |
| JDK | OpenJDK 21.0.10+7-Ubuntu-124.04 |
| Gradle | 8.8 |
| JMH | 1.36 |
| Disruptor | 4.0.0 on the JMH classpath only |
| Native backend | Not loaded; portable placement rows use `pinning=false` |

Common JVM flags:

```text
-Xms2g -Xmx2g
-XX:+AlwaysPreTouch
-XX:+UnlockDiagnosticVMOptions
-XX:+UseParallelGC
-Dlattice.fusion.enabled=true
-Dlattice.fusion.inlineSource=true
-Dlattice.metrics.hotCounters=false
-Dlattice.metrics.residence=false
-Dlattice.metrics.stageHistograms=false
-Dlattice.runtime.fusedLogicalEdgeMetrics=false
-Dlattice.runtime.inlineDepthTracking=false
```

See [`benchmarks/baseline/env.txt`](benchmarks/baseline/env.txt) for exact
include patterns and artifact profiles.

## Figures

| Figure | Description |
| --- | --- |
| ![Three-stage publish throughput](docs/assets/perf-pipeline.svg) | Deduplicated best logged Lattice and Disruptor publish rows with JMH error bars. |
| ![Lattice vs Disruptor ratios](docs/assets/disruptor-comparison.svg) | Ratios for the deduplicated publish rows plus the completion-gated optimal path. |
| ![End-to-end latency percentiles](docs/assets/latency-percentiles.svg) | Saturating-throughput end-to-end latency percentiles from checked-in logs. |

## Artifacts

| Artifact | Purpose |
| --- | --- |
| [`three-stage-vs-disruptor.json`](benchmarks/baseline/three-stage-vs-disruptor.json) | Broad three-stage Lattice physical/inline-fused vs Disruptor physical/manual-fused matrix retained for audit history. |
| [`three-stage-isolated-physical.json`](benchmarks/baseline/three-stage-isolated-physical.json) | Isolated semi-smoke physical three-stage Lattice vs Disruptor publish throughput. |
| [`three-stage-isolated-fused-copy.json`](benchmarks/baseline/three-stage-isolated-fused-copy.json) | Isolated semi-smoke Lattice inline-fused vs Disruptor manually fused copy-payload publish throughput. |
| [`three-stage-isolated-reference.json`](benchmarks/baseline/three-stage-isolated-reference.json) | Isolated semi-smoke reference-payload and equal-call-site Lattice vs Disruptor publish throughput. |
| [`optimal-path-completed.json`](benchmarks/baseline/optimal-path-completed.json) | Completion-gated optimal path: each operation waits for sink/handler completion. |
| [`lattice-core-basics.json`](benchmarks/baseline/lattice-core-basics.json) | Source/sink paths, batched topology, routing/topology rows, and raw edge regression rows. |
| [`lattice-placement.json`](benchmarks/baseline/lattice-placement.json) | Portable placement subset, first-touch on/off, pinning disabled. |

## Headline Results

### Top checked-in head-to-head rows

These rows deduplicate isolated and full-matrix repeats, then use the best
checked-in Lattice point estimate and the best checked-in Disruptor point
estimate for each published workload.

| Comparison | Lattice (ops/s) | Lattice source | Disruptor (ops/s) | Disruptor source | Ratio |
| --- | ---: | --- | ---: | --- | ---: |
| Physical three-stage publish | 27,660,948 | `three-stage-isolated-physical.json` | 26,377,465 | `three-stage-isolated-physical.json` | 1.05x |
| Inline/manual fused copy publish | 61,838,846 | `three-stage-isolated-fused-copy.json` | 45,888,659 | `three-stage-vs-disruptor.json` | 1.35x |
| Manual fused reference publish, equal call-site | 92,094,463 | `three-stage-vs-disruptor.json` | 44,045,374 | `three-stage-vs-disruptor.json` | 2.09x |

The fused-copy row intentionally compares the best logged Lattice point against
the best logged Disruptor copy-payload point, even though those winners come
from different artifacts. The reference row uses equal call-site footing:
`latticeManuallyFusedReference` is one Lattice stage doing the same three
increments inline as the Disruptor manually fused handler. Lattice is ahead in
all published head-to-head rows.

### Completed optimal path

| Benchmark | Score (ops/s) | Error |
| --- | ---: | ---: |
| Lattice inline-fused completed path | 29,903,291 | +-4,942,063 |
| Disruptor manually fused completed path | 4,742,326 | +-1,028,517 |

This benchmark closes the async publish-rate loophole: every operation waits
until the sink/handler confirms completion for the same sequence. On this host,
the Lattice inline-fused completed path measured 6.31x the Disruptor
busy-spin/manual-fused completed path.

### Broader Lattice topology rows

| Benchmark | Score (ops/s) | Error |
| --- | ---: | ---: |
| One source/sink single-producer | 12,398,644 | +-4,959,990 |
| One source/sink preallocated single-producer | 11,001,289 | +-6,079,214 |
| One source three-stage fused | 8,438,270 | +-3,936,722 |
| Batched validate/sink | 8,915,111 | +-3,600,749 |
| Validate/journal/risk/commit | 8,667,266 | +-2,726,702 |
| Partition four lanes | 9,880,370 | +-1,567,154 |
| Broadcast four branch | 4,873,629 | +-8,298,715 |
| Dispatch fanout | 8,892,558 | +-778,723 |
| Stamped all-of join | 5,244,672 | +-223,596 |

Rows with wide confidence intervals retain their JMH error bars in the tables
and figures. Do not rank close results without checking the raw JSON confidence
intervals and the matching topology semantics.

## Latency

Latency is printed by `LatencyRecorder` in the stdout logs. These are
saturating-throughput histograms, not fixed-rate latency measurements.

| Benchmark | Kind | p50 (ns) | p99 (ns) | p99.9 (ns) | p99.99 (ns) | Max (ns) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| oneSourceOneSinkSingleProducer | end-to-end | 801 | 892,415 | 4,190,207 | 9,936,895 | 45,088,767 |
| oneSourceOneSinkPreallocatedSingleProducer | end-to-end | 12,911 | 948,735 | 7,958,527 | 11,444,223 | 20,938,751 |
| oneSourceThreeStageSinkFused | end-to-end | 342,527 | 1,847,295 | 6,238,207 | 12,369,919 | 22,986,751 |
| batchedValidateSink | end-to-end | 1,001 | 1,740,799 | 7,335,935 | 15,433,727 | 32,243,711 |
| validateJournalRiskCommit | end-to-end | 1,500 | 2,498,559 | 11,657,215 | 19,021,823 | 44,761,087 |

## Reproduction

Build the benchmark jar:

```bash
./gradlew jmhJar
```

Representative command:

```bash
java -jar build/libs/lattice-1.0-SNAPSHOT-jmh.jar \
  "com.lattice.benchmark.OptimalPathBenchmark.*" \
  -f 3 -wi 5 -i 8 -w 5s -r 5s -bm thrpt -tu s \
  -jvmArgsAppend "-Xms2g -Xmx2g -XX:+AlwaysPreTouch -XX:+UnlockDiagnosticVMOptions -XX:+UseParallelGC -Dlattice.fusion.enabled=true -Dlattice.fusion.inlineSource=true -Dlattice.metrics.hotCounters=false -Dlattice.metrics.residence=false -Dlattice.metrics.stageHistograms=false -Dlattice.runtime.fusedLogicalEdgeMetrics=false -Dlattice.runtime.inlineDepthTracking=false" \
  -rf json -rff benchmarks/baseline/optimal-path-completed.json
```

Use [`docs/linux-validation.md`](docs/linux-validation.md) to reproduce the
same methodology on another Linux host before claiming results for that
hardware profile.
