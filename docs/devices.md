# Benchmark Devices

This page groups the checked-in benchmark snapshots by device. The numbers are
not normalized across machines; compare Lattice and Disruptor within the same
snapshot, then use cross-device rows only as capacity context.

## Device Snapshots

| Snapshot | Device | OS And Runtime | Native Placement | Artifact Notes |
| --- | --- | --- | --- | --- |
| [2026-05-02 per-graph refresh](benchmark-results/2026-05-02-per-graph-refresh/README.md) | Intel Core i9-14900HX, 16 cores / 32 threads, 1 NUMA node | Ubuntu LTS userspace under WSL2 Linux `6.6.87.2-microsoft-standard-WSL2`, OpenJDK 21.0.10 | Native library loaded for Lattice strict-topology and explicit CPU-pinned latency rows | Current public figures and raw `*-2026-05-02` JSON/log artifacts. |
| [2026-04-29 v1.0.0 baseline](benchmark-results/v1.0.0-baseline/README.md) | Intel i7-7700 @ 3.60 GHz, 4 cores / 8 threads, 1 NUMA node | Ubuntu Linux, OpenJDK 21.0.10 | Not loaded; placement rows use `pinning=false` | Historical publication baseline retained for audit history. |

Both snapshots use JMH 1.36, Java 21, `-Xms2g -Xmx2g`,
`-XX:+AlwaysPreTouch`, and `-XX:+UseParallelGC`. The 2026-05-02 run also uses
`-XX:+UnlockDiagnosticVMOptions`.

## Headline Throughput By Device

| Device Snapshot | Workload | Lattice | Disruptor | Ratio |
| --- | --- | ---: | ---: | ---: |
| i9-14900HX, 2026-05-02 | Physical three-stage publish | 31,938,529 ops/s | 21,698,059 ops/s | 1.47x |
| i9-14900HX, 2026-05-02 | Inline/manual fused copy publish | 127,875,286 ops/s | 35,697,152 ops/s | 3.58x |
| i9-14900HX, 2026-05-02 | Manual fused reference, equal call-site | 209,168,722 ops/s | 31,091,239 ops/s | 6.73x |
| i9-14900HX, 2026-05-02 | Completed optimal path | 77,868,589 ops/s | 3,620,353 ops/s | 21.51x |
| i7-7700, 2026-04-29 | Physical three-stage publish | 27,660,948 ops/s | 26,377,465 ops/s | 1.05x |
| i7-7700, 2026-04-29 | Inline/manual fused copy publish | 61,838,846 ops/s | 45,888,659 ops/s | 1.35x |
| i7-7700, 2026-04-29 | Manual fused reference, equal call-site | 92,094,463 ops/s | 44,045,374 ops/s | 2.09x |
| i7-7700, 2026-04-29 | Completed optimal path | 29,903,291 ops/s | 4,742,326 ops/s | 6.31x |

The i7 rows are derived from the previously published
[v1.0.0 baseline](benchmark-results/v1.0.0-baseline/README.md). That artifact
set predates the per-graph runtime API, so it should not be mixed with the
2026-05-02 raw JSON when making release claims.

## Current Latency Rows

The latency rows below are from the i9-14900HX 2026-05-02 sample-time run.
They use the same parse/enrich/risk/serialize workload and wait for completion
of the emitted sequence.

| Variant | p50 | p90 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: |
| Lattice source-inline elided | 30 ns | 39 ns | 51 ns | 295 ns |
| Disruptor manual fused | 231 ns | 291 ns | 393 ns | 9,531 ns |
| Lattice physical strict topology | 775 ns | 874 ns | 1,434 ns | 21,152 ns |
| Disruptor physical | 617 ns | 728 ns | 3,632 ns | 30,240 ns |
| Lattice physical pinned CPU | 774 ns | 868 ns | 1,620 ns | 24,466 ns |

The native placement rows use explicit CPU pinning or
`GraphPlacementSpec.topologyAware(true).strict(true).firstTouch(true)` with the
Linux native topology library. Each latency row is from an isolated JMH
invocation.

## Reading The Page

- Treat WSL2 and native Ubuntu as different host profiles.
- Do not compare raw ops/s across devices as a product claim.
- Cite the benchmark class, method, JSON artifact, JVM flags, device, and
  native placement status with any number.
- Re-run on the target deployment hardware before making placement or latency
  claims for that hardware.
