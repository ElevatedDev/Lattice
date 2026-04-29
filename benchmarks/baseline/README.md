# Benchmark Baseline

This directory contains the refreshed local benchmark baseline captured on
2026-04-29. It replaces the older development-run artifacts with a smaller set
of reproducible JMH JSON/log pairs.

This is a WSL2 orientation baseline for regression review. It is not a
publication-grade Linux/NUMA performance claim; see `env.txt` for the host,
JVM flags, and exact include patterns.

## Artifact Map

| Artifact | Profile | Purpose |
| --- | --- | --- |
| `three-stage-vs-disruptor.json` / `.log` | 3 forks, 5x5s warmup, 8x5s measure | Three-stage Lattice physical/inline-fused vs Disruptor physical/manual-fused. |
| `optimal-path-completed.json` / `.log` | 3 forks, 5x5s warmup, 8x5s measure | Completion-gated optimal path: each operation waits for the sink/handler to complete the same sequence. |
| `lattice-core-basics.json` / `.log` | 1 fork, 3x5s warmup, 5x5s measure | Raw edge sanity, SPSC source paths, batched topology, and routing/topology rows with latency logs. |
| `lattice-edge-pair-mpsc-ingress.json` / `.log` | 1 fork, 3x5s warmup, 5x5s measure | SPSC/MPSC edge-pair group rows and 4-producer MPSC ingress with latency. |
| `lattice-placement.json` / `.log` | 1 fork, 3x5s warmup, 5x5s measure | Portable placement subset: `containerCpuset`, `pinning=false`, first-touch on/off, on-heap slots. |
| `env.txt` | n/a | Host, toolchain, JVM flags, and include patterns. |

## Three-Stage Head-To-Head

Publish-throughput rows from `three-stage-vs-disruptor.json`.

| Benchmark | Score (ops/s) | Error | Notes |
| --- | ---: | ---: | --- |
| Lattice physical three-stage | 15,614,507 | +-7,474,896 | WSL2 outliers widened CI. |
| Lattice inline-fused three-stage | 51,037,862 | +-10,928,856 | `lattice.fusion.enabled=true`, `lattice.fusion.inlineSource=true`. |
| Lattice inline-fused three-stage (reference framing) | 49,783,065 | +-11,514,920 | Same wiring as the row above; renamed for parity with the Disruptor reference-payload row. The fused path already passes `Signal` by reference. |
| Lattice manually fused, reference payload | 92,094,463 | +-11,989,415 | One Lattice stage performs all three increments inline; mirrors the Disruptor manually-fused-reference shape. |
| Disruptor physical three-handler pipeline | 23,775,288 | +-5,959,805 | `YIELDING` wait profile. |
| Disruptor manually fused, copy payload | 45,888,659 | +-2,140,864 | One handler, event-slot field copy. |
| Disruptor manually fused, reference payload | 44,045,374 | +-4,620,171 | One handler, event slot stores a reference. |

Ratios:

| Comparison | Ratio |
| --- | ---: |
| Lattice physical / Disruptor physical | 0.66x |
| Lattice inline-fused (3 stages) / Disruptor manually fused copy | 1.11x |
| Lattice inline-fused (3 stages) / Disruptor manually fused reference | 1.13x |
| Lattice manually fused reference (1 stage) / Disruptor manually fused reference | 2.09x |

Interpretation: Lattice's inline-fused path already passes the payload by
reference; there is no per-slot field copy on this path. The reference-payload
rows compare two shapes: Lattice's three logical stages and an equal-call-site
row where one Lattice stage performs the three increments inline. On this WSL2
run, the equal-call-site Lattice row is 2.09x the Disruptor manually fused
reference row. The physical three-stage row is noisy and should not be used as
ordering evidence without a dedicated Linux rerun.

## Completed Optimal Path

Rows from `optimal-path-completed.json`. Unlike the publish-throughput rows,
each benchmark operation waits until the sink/handler publishes completion for
the same sequence number.

| Benchmark | Score (ops/s) | Error | Notes |
| --- | ---: | ---: | --- |
| Lattice inline-fused completed path | 29,903,291 | +-4,942,063 | Three logical stages plus sink complete on producer thread. |
| Disruptor manually fused completed path | 4,742,326 | +-1,028,517 | One busy-spin handler; benchmark waits for handler completion. |

Ratio: Lattice completed path / Disruptor completed path = 6.31x on this host.

## Lattice Basics

Selected throughput rows from `lattice-core-basics.json` and
`lattice-edge-pair-mpsc-ingress.json`.

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

Rows with very wide error bars should be treated as noisy WSL2 measurements,
not stable ordering evidence.

## Latency Excerpts

Latency values are printed by `LatencyRecorder` in the `.log` files. These are
saturating-throughput histograms, not fixed-rate service latency.

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

## Caveats

- The host is WSL2 on an Intel i7-7700 with one NUMA node. Native placement and
  cross-socket behavior were not exercised.
- JMH uses compiler blackholes on this JVM; keep the raw JMH notes with any
  quoted number.
- The three-stage head-to-head class measures publish throughput. Use
  `OptimalPathBenchmark` when completed-operation throughput matters.
- The completed-path benchmark was added specifically to avoid comparing
  synchronous inline completion with asynchronous enqueue-only rates.
