# Lattice Benchmark Baseline

This document summarizes the checked-in local baseline under
[`benchmarks/baseline/`](benchmarks/baseline/). It is intended for regression
review and open-source transparency.

These numbers were captured on 2026-04-29 on a WSL2 development host. They are
not publication-grade Linux/NUMA claims. Keep the raw JMH JSON and stdout logs
with any quoted number.

## Host And Profile

| Property | Value |
| --- | --- |
| Host | `DEVELOP-PC2`, Ubuntu on WSL2 |
| Kernel | `6.6.87.2-microsoft-standard-WSL2` |
| CPU | Intel Core i7-7700 @ 3.60 GHz, 4 cores / 8 threads |
| NUMA | 1 node, CPUs 0-7 |
| JDK | OpenJDK 21.0.10+7-Ubuntu-124.04 |
| Gradle | 8.8 |
| JMH | 1.36 |
| Disruptor | 4.0.0 on the JMH classpath only |
| Native backend | Not loaded; Rust was not installed on this host |

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

## Artifacts

| Artifact | Purpose |
| --- | --- |
| [`three-stage-vs-disruptor.json`](benchmarks/baseline/three-stage-vs-disruptor.json) | Three-stage Lattice physical/inline-fused vs Disruptor physical/manual-fused publish throughput. |
| [`optimal-path-completed.json`](benchmarks/baseline/optimal-path-completed.json) | Completion-gated optimal path: each operation waits for sink/handler completion. |
| [`lattice-core-basics.json`](benchmarks/baseline/lattice-core-basics.json) | Raw edge sanity, SPSC source paths, batched topology, routing/topology rows. |
| [`lattice-edge-pair-mpsc-ingress.json`](benchmarks/baseline/lattice-edge-pair-mpsc-ingress.json) | SPSC/MPSC edge-pair groups and 4-producer MPSC ingress. |
| [`lattice-placement.json`](benchmarks/baseline/lattice-placement.json) | Portable placement subset, first-touch on/off, pinning disabled. |

## Headline Results

### Three-stage publish throughput

| Benchmark | Score (ops/s) | Error |
| --- | ---: | ---: |
| Lattice physical three-stage | 15,614,507 | +-7,474,896 |
| Lattice inline-fused three-stage | 51,037,862 | +-10,928,856 |
| Lattice inline-fused three-stage (reference framing) | 49,783,065 | +-11,514,920 |
| Lattice manually fused, reference payload (1 stage, 3 increments) | 92,094,463 | +-11,989,415 |
| Disruptor physical three-handler pipeline | 23,775,288 | +-5,959,805 |
| Disruptor manually fused, copy payload | 45,888,659 | +-2,140,864 |
| Disruptor manually fused, reference payload | 44,045,374 | +-4,620,171 |

Lattice's inline-fused path already passes the payload by reference (no
per-slot field copy). The 3-stage Lattice rows and the 3-stage Disruptor
manually-fused-reference row are not equal-call-site comparisons: Disruptor's
row collapses three logical stages into one `EventHandler` call, while the
Lattice fused rows keep three logical stages. Forcing equal call-site footing
(`latticeManuallyFusedReference`: one Lattice stage doing three increments
inline) yields 92.1M ops/s vs Disruptor's 44.0M (2.09x) on this WSL2 run.
The physical three-stage rows were especially noisy and should not be used as
ordering evidence without a dedicated Linux rerun.

### Completed optimal path

| Benchmark | Score (ops/s) | Error |
| --- | ---: | ---: |
| Lattice inline-fused completed path | 29,903,291 | +-4,942,063 |
| Disruptor manually fused completed path | 4,742,326 | +-1,028,517 |

This benchmark closes the async publish-rate loophole: every operation waits
until the sink/handler confirms completion for the same sequence. On this host,
the Lattice inline-fused completed path measured 6.31x the Disruptor
busy-spin/manual-fused completed path.

### Lattice basics

| Benchmark | Score (ops/s) | Error |
| --- | ---: | ---: |
| EdgeMicro SPSC same-thread sanity | 161,175,551 | +-5,880,299 |
| EdgeMicro MPSC same-thread sanity | 78,892,598 | +-1,498,697 |
| EdgePair SPSC total | 107,918,505 | +-16,937,568 |
| EdgePair MPSC total | 76,449,353 | +-8,895,651 |
| MPSC ingress, 4 producer threads | 12,223,452 | +-1,756,623 |
| One source/sink single-producer | 11,110,042 | +-223,488 |
| One source/sink preallocated single-producer | 12,053,808 | +-1,035,740 |
| One source three-stage fused | 8,348,960 | +-2,948,823 |
| Batched validate/sink | 8,878,476 | +-479,806 |
| Validate/journal/risk/commit | 8,790,433 | +-1,183,453 |
| Partition four lanes | 8,339,434 | +-1,961,393 |
| Broadcast four branch | 7,141,538 | +-434,392 |
| Dispatch fanout | 7,288,533 | +-5,244,219 |
| Stamped all-of join | 5,035,640 | +-3,666,841 |

Rows with wide confidence intervals show WSL2 scheduler noise and should not be
used as ordering evidence without a dedicated Linux rerun.

## Latency

Latency is printed by `LatencyRecorder` in the stdout logs. These are
saturating-throughput histograms, not fixed-rate latency measurements.

| Benchmark | Kind | p50 (ns) | p99 (ns) | p99.9 (ns) | p99.99 (ns) | Max (ns) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| oneSourceOneSinkSingleProducer | end-to-end | 700 | 417,023 | 1,184,767 | 4,628,479 | 8,945,663 |
| oneSourceOneSinkPreallocatedSingleProducer | end-to-end | 7,803 | 730,111 | 1,683,455 | 4,132,863 | 9,060,351 |
| mpscIngress | end-to-end | 243,711 | 1,650,687 | 3,432,447 | 9,887,743 | 22,331,391 |
| validateJournalRiskCommit | end-to-end | 1,400 | 3,188,735 | 8,118,271 | 13,721,599 | 18,153,471 |
| spscPlacement firstTouch=false | end-to-end | 300 | 291,327 | 946,687 | 3,215,359 | 10,305,535 |
| spscPlacement firstTouch=true | end-to-end | 400 | 340,735 | 953,343 | 3,035,135 | 11,804,671 |
| mpscPlacement firstTouch=false | end-to-end | 281,343 | 1,809,407 | 3,923,967 | 10,289,151 | 25,640,959 |
| mpscPlacement firstTouch=true | end-to-end | 374,783 | 1,766,399 | 3,903,487 | 11,517,951 | 31,916,031 |

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

Use [`docs/linux-validation.md`](docs/linux-validation.md) before making public
Linux/NUMA performance claims.
