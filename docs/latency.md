# Latency

This page summarizes the isolated end-to-end latency comparison from
`AllPathsLatencyBenchmark`. Each operation publishes one pooled `Order`, runs
`parse`, `enrich`, `risk`, and `serialize`, then waits for the final completion
sequence before returning.

The numbers are JMH sample-time `ns/op` percentiles from isolated checked-in
2026-05-02 runs. Lower is better. These are checked-in benchmark artifacts, not
portable latency guarantees.

![Isolated end-to-end latency percentiles](assets/latency-percentiles.svg)

![Isolated end-to-end p99 latency](assets/latency-p99.svg)

## Benchmark Profile

| Item | Value |
| --- | --- |
| Benchmark | `com.lattice.benchmark.AllPathsLatencyBenchmark.*` |
| Mode | JMH sample time |
| Warmup | 5 iterations, 3 seconds each |
| Measurement | 5 iterations, 3 seconds each |
| Forks | 3 |
| Heap | `-Xms2g -Xmx2g -XX:+AlwaysPreTouch` |
| GC | `-XX:+UseParallelGC` |
| Wait strategy | Busy spin on Lattice and Disruptor |
| Native placement | Lattice strict topology and Lattice explicit CPU pinning rows only |
| Raw artifacts | [`benchmarks/baseline/latency-isolated-*-2026-05-02.json`](../benchmarks/baseline/) |
| P99 error bars | Min/max of per-iteration p99 samples from the isolated JMH JSON |

## Results

| End-to-end path | mean | p50 | p90 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Lattice source-inline elided | 42.6 | 30 | 39 | 51 | 295 |
| Disruptor manual fused | 296.4 | 231 | 291 | 393 | 9,531 |
| Lattice physical strict topology | 887.9 | 775 | 874 | 1,434 | 21,152 |
| Disruptor physical | 770.1 | 617 | 728 | 3,632 | 30,240 |
| Lattice physical pinned CPU | 914.5 | 774 | 868 | 1,620 | 24,466 |

## Profile Notes

| Path | What it measures |
| --- | --- |
| Lattice source-inline elided | The lowest-latency eligible Lattice path: the producer thread executes the fused chain and the runtime removes the physical source edge. |
| Disruptor manual fused | A favorable Disruptor control where one handler manually performs parse, enrich, risk, and serialize. |
| Lattice physical strict topology | Fusion is disabled; Lattice uses `GraphPlacementSpec.topologyAware(true).strict(true).firstTouch(true)` for native topology placement. |
| Disruptor physical | A comparable Disruptor pipeline with separate dependent handlers for parse, enrich, risk, and commit. |
| Lattice physical pinned CPU | Lattice-only explicit CPU pinning across the physical workers. |

## Reading The Result

Lattice wins the best end-to-end p99 comparison in this run:
`51 ns` for source-inline elided versus `393 ns` for Disruptor manual fused.
That Lattice row is intentionally specialized: there is no physical source
ring and no source-edge backpressure in that mode; the caller runs the eligible
fused chain synchronously.

Lattice also has the lower physical p99 in the strict-topology profile:
`1,434 ns` versus `3,632 ns` for Disruptor physical in isolated runs.
The physical comparison is mixed, not a clean sweep: Disruptor physical is
lower at mean, p50, and p90, while Lattice strict topology is lower at p99 and
p99.9 in this isolated run.

For latency-sensitive static graph services, the p99 result is one of the most
important rows in the set. It shows that Lattice's physical runtime can improve
tail behavior even when central tendency favors the single-ring Disruptor
shape.

The explicit Lattice CPU-pinned physical row is retained as a Lattice placement
reference. It is not the headline row; strict topology is the better Lattice
physical p99 profile in this run.

The p99.9 and maximum samples are more sensitive to host noise, sampling
length, and WSL2 scheduling than p50/p90/p99. Treat them as checked-in
evidence for this run, not a portable latency guarantee.
