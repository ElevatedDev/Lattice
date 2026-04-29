# Benchmark Baseline

This directory contains the refreshed benchmark baseline captured on
2026-04-29. It replaces the older development-run artifacts with a smaller set
of reproducible JMH JSON/log pairs.

This is the checked-in public result set for regression review, open-source
transparency, and research comparison. See `env.txt` for the validation
profile, JVM flags, and exact include patterns.

## Figures

| Figure | File |
| --- | --- |
| Three-stage throughput | `../../docs/assets/perf-pipeline.svg` |
| Lattice vs Disruptor ratios | `../../docs/assets/disruptor-comparison.svg` |
| Core edge and ingress throughput | `../../docs/assets/perf-edge-pair.svg` |
| End-to-end latency percentiles | `../../docs/assets/latency-percentiles.svg` |

## Artifact Map

| Artifact | Profile | Purpose |
| --- | --- | --- |
| `three-stage-vs-disruptor.json` / `.log` | 3 forks, 5x5s warmup, 8x5s measure | Broad three-stage Lattice physical/inline-fused vs Disruptor physical/manual-fused matrix retained for audit history. |
| `three-stage-isolated-physical.json` / `.log` | 2 forks, 3x3s warmup, 5x3s measure | Isolated Lattice physical three-stage vs Disruptor physical three-handler publish throughput. |
| `three-stage-isolated-fused-copy.json` / `.log` | 2 forks, 3x3s warmup, 5x3s measure | Isolated Lattice inline-fused three-stage vs Disruptor manually fused copy-payload publish throughput. |
| `three-stage-isolated-reference.json` / `.log` | 2 forks, 3x3s warmup, 5x3s measure | Isolated reference-payload and equal-call-site Lattice rows vs Disruptor manually fused reference publish throughput. |
| `optimal-path-completed.json` / `.log` | 3 forks, 5x5s warmup, 8x5s measure | Completion-gated optimal path: each operation waits for the sink/handler to complete the same sequence. |
| `lattice-core-basics.json` / `.log` | 1 fork, 3x5s warmup, 5x5s measure | Raw edge sanity, SPSC source paths, batched topology, and routing/topology rows with latency logs. |
| `lattice-edge-pair-mpsc-ingress.json` / `.log` | 1 fork, 3x5s warmup, 5x5s measure | SPSC/MPSC edge-pair group rows and 4-producer MPSC ingress with latency. |
| `lattice-placement.json` / `.log` | 1 fork, 3x5s warmup, 5x5s measure | Portable placement subset: `containerCpuset`, `pinning=false`, first-touch on/off, on-heap slots. |
| `env.txt` | n/a | Host, toolchain, JVM flags, and include patterns. |

## Top Checked-In Three-Stage Head-To-Head

Publish-throughput rows using the best checked-in Lattice point estimate for
each published shape. Each comparison uses the Disruptor row from the same
artifact as the Lattice row it is compared against.

| Comparison | Lattice (ops/s) | Disruptor (ops/s) | Ratio | Artifact |
| --- | ---: | ---: | ---: | --- |
| Physical three-stage | 27,660,948 | 26,377,465 | 1.05x | `three-stage-isolated-physical.json` |
| Inline/manual fused, copy payload | 61,838,846 | 35,200,599 | 1.76x | `three-stage-isolated-fused-copy.json` |
| Inline/manual fused, reference payload | 52,698,325 | 38,713,479 | 1.36x | `three-stage-isolated-reference.json` |
| Manual fused reference, equal call-site | 92,094,463 | 44,045,374 | 2.09x | `three-stage-vs-disruptor.json` |

Interpretation: Lattice's inline-fused path already passes the payload by
reference; there is no per-slot field copy on this path. The reference-payload
rows compare two shapes: Lattice's three logical stages and an equal-call-site
row where one Lattice stage performs the three increments inline. The
equal-call-site Lattice row reaches 92.1M ops/s and is 2.09x the Disruptor
manually fused reference row in the longer checked-in matrix. The physical
three-stage point estimate is 1.05x Disruptor in the isolated physical
artifact; the clearer stage wins are the inline-fused, reference,
equal-call-site, and completed-path rows.

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
| EdgeMicro SPSC same-thread sanity | 41,650,443 | +-55,884,184 |
| EdgeMicro MPSC same-thread sanity | 25,057,290 | +-17,254,709 |
| EdgePair SPSC total | 101,094,180 | +-14,838,658 |
| EdgePair MPSC total | 74,115,581 | +-11,211,687 |
| MPSC ingress, 4 producer threads | 14,289,722 | +-3,851,449 |
| One source/sink single-producer | 12,398,644 | +-4,959,990 |
| One source/sink preallocated single-producer | 11,001,289 | +-6,079,214 |
| One source three-stage fused | 8,438,270 | +-3,936,722 |
| Batched validate/sink | 8,915,111 | +-3,600,749 |
| Validate/journal/risk/commit | 8,667,266 | +-2,726,702 |
| Partition four lanes | 9,880,370 | +-1,567,154 |
| Broadcast four branch | 4,873,629 | +-8,298,715 |
| Dispatch fanout | 8,892,558 | +-778,723 |
| Stamped all-of join | 5,244,672 | +-223,596 |

Rows with very wide error bars retain their JMH error bars in the table. Do
not rank close results without checking the raw JSON confidence intervals and
matching topology semantics.

## Latency Excerpts

Latency values are printed by `LatencyRecorder` in the `.log` files. These are
saturating-throughput histograms, not fixed-rate service latency.

| Benchmark | Kind | p50 (ns) | p99 (ns) | p99.9 (ns) | p99.99 (ns) | Max (ns) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| oneSourceOneSinkSingleProducer | end-to-end | 801 | 892,415 | 4,190,207 | 9,936,895 | 45,088,767 |
| oneSourceOneSinkPreallocatedSingleProducer | end-to-end | 12,911 | 948,735 | 7,958,527 | 11,444,223 | 20,938,751 |
| oneSourceThreeStageSinkFused | end-to-end | 342,527 | 1,847,295 | 6,238,207 | 12,369,919 | 22,986,751 |
| mpscIngress | end-to-end | 16,815 | 1,558,527 | 5,804,031 | 11,976,703 | 43,057,151 |
| batchedValidateSink | end-to-end | 1,001 | 1,740,799 | 7,335,935 | 15,433,727 | 32,243,711 |
| validateJournalRiskCommit | end-to-end | 1,500 | 2,498,559 | 11,657,215 | 19,021,823 | 44,761,087 |

## Interpretation Rules

- JMH uses compiler blackholes on this JVM; keep the raw JMH notes with any
  quoted number.
- The three-stage head-to-head class measures publish throughput. Use
  `OptimalPathBenchmark` when completed-operation throughput matters.
- The completed-path benchmark avoids comparing synchronous inline completion
  with asynchronous enqueue-only rates.
- Native placement and cross-socket behavior are not part of this portable
  baseline because the placement rows use `pinning=false`.
