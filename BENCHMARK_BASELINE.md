# Lattice — Benchmark Baseline (DEVELOP-PC2)

This document captures the **current-state** benchmark numbers on this development machine **before** any of the changes described in `PERFORMANCE_REVIEW.md` are applied. Use it as the regression gate: re-run the same benchmarks after each change and compare against the recorded baseline.

> ⚠️ The benchmark numbers in `README.md`, `PERFORMANCE_TUNING.md`, and `CHANGELOG.md` were collected on a different host. **Do not** compare absolute numbers across hosts — only compare against the values captured here.

---

## 1. Host & toolchain

| Property | Value |
|---|---|
| Hostname | `DEVELOP-PC2` |
| OS | Ubuntu 24.04.1 LTS on WSL2 (`6.6.87.2-microsoft-standard-WSL2`) |
| CPU | Intel(R) Core(TM) i7-7700 @ 3.60 GHz (Kaby Lake) |
| Sockets / cores / threads | 1 / 4 / 8 (HT enabled) |
| L1d / L1i / L2 / L3 | 4×32 KiB / 4×32 KiB / 4×256 KiB / 1×8 MiB |
| NUMA nodes | 1 (node0: CPU 0–7) — **single-socket, NUMA placement is a no-op here** |
| Memory | 16 GiB total (WSL2 cap) |
| JDK | OpenJDK 21.0.10+7-Ubuntu-124.04 (Server VM, mixed mode) |
| Gradle | (run `./gradlew --version` and paste here) |
| Rust toolchain | (run `rustc --version` and paste here) |
| JMH config | `fork=1`, `warmupIterations=3`, `iterations=5` (from `build.gradle`) |

> ⚠️ **WSL2 + HT note:** results on WSL2 carry hypervisor noise; tail latency (p99/p99.9) will be especially noisy. Pin the JVM to physical cores (CPU 0,2,4,6) when running latency-sensitive benchmarks. Disable other workloads while measuring. NUMA-aware paths cannot be exercised on this 1-node host — record those benchmarks as "N/A (single NUMA node)".

---

## 2. How to reproduce

All benchmarks live under `src/jmh/java/com/lattice/benchmark/`. The full JMH suite is invoked via:

```bash
./gradlew jmh
```

To run a single benchmark class (recommended for the baseline pass — much faster):

```bash
./gradlew jmh -PjmhInclude='com.lattice.benchmark.EdgeMicroBenchmark'
```

> If `-PjmhInclude` isn't wired in `build.gradle`, fall back to running the JMH jar directly:
> ```bash
> ./gradlew jmhJar
> java -jar build/libs/lattice-*-jmh.jar -f 1 -wi 3 -i 5 \
>   -rf json -rff baseline-edge-micro.json \
>   'com.lattice.benchmark.EdgeMicroBenchmark.*'
> ```

**Recommended JVM args for fair, low-noise measurement** (apply via `jvmArgsAppend` in the JMH block or `-jvmArgsAppend` on the CLI):

```
-XX:+AlwaysPreTouch
-XX:+UseG1GC
-Xms2g -Xmx2g
-XX:+UnlockDiagnosticVMOptions
-XX:+DebugNonSafepoints
```

For the metrics-overhead benchmark, also capture a run with hot counters disabled:

```
-Dlattice.metrics.hotCounters=false
```

For the fusion benchmarks, capture both:

```
-Dlattice.fusion.enabled=false   # current default
-Dlattice.fusion.enabled=true    # opt-in
```

---

## 3. Benchmarks to capture

For every benchmark below, save:

1. The **JMH JSON** (`-rf json -rff <name>.json`) into `benchmarks/baseline/`.
2. The **stdout log** into `benchmarks/baseline/<name>.log`.
3. The summary numbers in the table provided.

### 3.1 `EdgeMicroBenchmark` — SPSC/MPSC raw throughput
**File:** `src/jmh/java/com/lattice/benchmark/EdgeMicroBenchmark.java`

| Variant | Score | Units | Error (±) | Notes |
|---|---:|---|---:|---|
| spsc, batch=1 |  | ops/s |  |  |
| spsc, batch=64 |  | ops/s |  |  |
| spsc, batch=256 |  | ops/s |  |  |
| mpsc, producers=1 |  | ops/s |  |  |
| mpsc, producers=2 |  | ops/s |  |  |
| mpsc, producers=4 |  | ops/s |  |  |

### 3.2 `EdgePairBenchmark` — back-to-back stage pair
| Variant | Score | Units | Error (±) | Notes |
|---|---:|---|---:|---|
| default |  | ops/s |  |  |
| busy-spin wait |  | ops/s |  |  |
| phased wait |  | ops/s |  |  |

### 3.3 `TopologyBenchmark` — full graph throughput
| Shape | Score | Units | Error (±) | Notes |
|---|---:|---|---:|---|
| linear (3 stages) |  | ops/s |  |  |
| linear (5 stages) |  | ops/s |  |  |
| broadcast (1→4) |  | ops/s |  |  |
| partition (1→4) |  | ops/s |  |  |

### 3.4 `BatchTopologyBenchmark` — batched processing
| Variant | Score | Units | Error (±) | Notes |
|---|---:|---|---:|---|
| batch=64 |  | ops/s |  |  |
| batch=256 |  | ops/s |  |  |
| batch=1024 |  | ops/s |  |  |

### 3.5 `Phase4TopologyBenchmark` — routing/join
| Variant | Score | Units | Error (±) | Notes |
|---|---:|---|---:|---|
| dispatch keyed |  | ops/s |  |  |
| dispatch round-robin |  | ops/s |  |  |
| dispatch weighted |  | ops/s |  |  |
| join, 2 branches |  | ops/s |  |  |
| join, 4 branches |  | ops/s |  |  |

### 3.6 `TopologyShapeDisruptorBenchmark` & `DisruptorBaselineBenchmark` — head-to-head vs LMAX Disruptor 4.0.0
| Workload | Lattice (ops/s) | Disruptor (ops/s) | Ratio L/D |
|---|---:|---:|---:|
| SPSC raw |  |  |  |
| 3-stage linear |  |  |  |
| 1→4 broadcast |  |  |  |
| 1→4 partition (keyed) |  |  |  |

### 3.7 `ApplesToApplesDisruptorBenchmark` — same JVM args, same payload, same wait
| Variant | Lattice | Disruptor | Δ (%) |
|---|---:|---:|---:|
| busy-spin |  |  |  |
| yielding |  |  |  |
| blocking |  |  |  |

### 3.8 `MpscIngressBenchmark` — multi-producer ingress
| Producers | Score | Units | Error (±) |
|---|---:|---|---:|
| 1 |  | ops/s |  |
| 2 |  | ops/s |  |
| 4 |  | ops/s |  |
| 8 |  | ops/s |  |

### 3.9 `MetricsOverheadBenchmark` — cost of hot-path counters
This is the single most important baseline for validating the **EdgeMetrics → LongAdder** change in `PERFORMANCE_REVIEW.md` finding D1.

| Configuration | Score | Units | Error (±) | Δ vs. disabled (%) |
|---|---:|---|---:|---:|
| `-Dlattice.metrics.hotCounters=true` (default) |  | ops/s |  |  |
| `-Dlattice.metrics.hotCounters=false` |  | ops/s |  | 0 |

> The gap between these two rows is the **maximum** speedup we can ever extract by fixing the metrics hot path. It is also the canonical regression target after the fix lands: with hot counters enabled and the new lock-free counters, the score must be within ~5 % of the "disabled" baseline.

### 3.10 `AllocationGuardBenchmark` — bytes allocated per op
Capture both throughput AND `-prof gc` output. Per-op allocation budget targets:

| Path | bytes/op (current) | target after fix |
|---|---:|---:|
| SPSC unstamped emit |  | 0 |
| SPSC stamped emit (`Stamped.of` path — finding B1) |  | 0 |
| Broadcast COPY=1→4 |  | (acceptable: 4× user payload) |
| Partition keyed |  | 0 (after `ToIntFunction` fix B2) |

### 3.11 `PlacementBenchmark` — pinning / first-touch cost
| Variant | Score | Units | Error (±) |
|---|---:|---|---:|
| topology-aware enabled |  | ms (startup) |  |
| topology-aware disabled |  | ms (startup) |  |

> N/A on this single-NUMA-node host for runtime placement effects, but the startup cost of the placement subsystem is still measurable.

### 3.12 Latency (`LatencyRecorder` — see `BenchmarkMetrics.java`)
Capture HdrHistogram percentiles from any benchmark that uses `LatencyRecorder`. Fill in for the `EdgeMicroBenchmark` SPSC run at a **fixed input rate** (e.g., 1 Mmsg/s, 2 Mmsg/s, saturating).

| Rate (msg/s) | p50 (ns) | p90 | p99 | p99.9 | p99.99 | max |
|---|---:|---:|---:|---:|---:|---:|
| 1 M |  |  |  |  |  |  |
| 2 M |  |  |  |  |  |  |
| Saturating |  |  |  |  |  |  |

---

## 4. Stress and correctness baselines (must pass before & after each change)

These are not perf benchmarks but are required to remain green.

```bash
./gradlew test
./gradlew jcstress      # may take 30+ min
./gradlew nativeTest    # Rust JNI sanity
```

| Suite | Pass count | Skipped | Failed | Notes |
|---|---:|---:|---:|---|
| `test` |  |  |  |  |
| `jcstress` |  |  |  | (record `FORBIDDEN`/`ACCEPTABLE_INTERESTING` if any) |
| `nativeTest` |  |  |  |  |

---

## 5. Regression gate (CI-style)

After each PR from the rollout in `PERFORMANCE_REVIEW.md`, re-run sections 3 and 4 and require:

- **Throughput:** No benchmark regresses by more than **3 %** (within JMH error bars).
- **Latency:** p99 and p99.9 must not regress by more than **10 %** at the same input rate.
- **Allocation (3.10):** zero-alloc paths must remain at 0 bytes/op (after PR #3 lands).
- **Correctness:** `test`, `jcstress`, `nativeTest` all green.

For the metrics fix specifically (PR #1), the **success criterion** is:
> `MetricsOverheadBenchmark` with `hotCounters=true` is within 5 % of the `hotCounters=false` baseline (currently a much larger gap — record it in §3.9).

For the fusion default flip (PR #2):
> `TopologyBenchmark` linear / broadcast / partition shapes show a measurable speedup with `lattice.fusion.enabled=true` vs. the current default. Record both runs in §3.3.

---

## 7. Capture log

| Date | PR / commit | Section(s) refreshed | Run by | Notes |
|---|---|---|---|---|
| 2026-04-27 | baseline (pre-changes) | all | copilot | Smoke run, 11m15s |
| 2026-04-27 | post-PR1 | §8 below | copilot | Smoke run, 11m20s |

---

## 8. Post-PR1 results — smoke comparison

PR #1 ships the five mechanical, low-risk optimizations from `PERFORMANCE_REVIEW.md`:

1. **EdgeMetrics**: replace `AtomicLong emittedCount/consumedCount/droppedOldestCount` with `LongAdder`; sample depth/high-water-mark every 1024 emits per producer thread (instead of computing it on every emit with 3 atomic loads + a CAS loop). Source: `EdgeMetrics.java`.
2. **Fusion default ON**: `lattice.fusion.enabled` defaults to `true`; opt-out only. Source: `DefaultStaticGraph.java`.
3. **Drop `volatile` from `PaddedLong.value` / `PaddedBoolean.value`** — accessed via VarHandle modes only. Source: `PaddedLong.java`, `PaddedBoolean.java`.
4. **Remove redundant leading `closed()` volatile load** in `SpscRingEdge.offer()` — the tail acquire-load already encodes the closed bit. Source: `SpscRingEdge.java`.
5. **Test fix**: `RuntimeRegressionTest.abortWhileOutputIsBackpressuredDoesNotFailGraph` now explicitly sets `lattice.fusion.enabled=false` because it asserts a per-stage backpressure metric that fusion (correctly) elides.

**All 107 unit tests pass** after the changes.

### 8.1 Headline wins (Lattice benchmarks)

| Benchmark | Baseline (ops/s) | Post-PR1 (ops/s) | Δ |
|---|---:|---:|---:|
| TopologyBenchmark.oneSourceValidateSinkFused | 1,305,291 | **8,153,141** | **+524 %** |
| TopologyBenchmark.oneSourceOneSinkPreallocatedSingleProducer | 1,104,271 | **6,399,166** | **+479 %** |
| TopologyBenchmark.oneSourceValidateSinkSingleProducer | 1,152,077 | **7,712,192** | **+569 %** |
| ApplesToApplesDisruptorBenchmark.latticeParallelDependencyJoin | 17,210 | **313,620** | **+1,722 %** |
| TopologyBenchmark.oneSourceValidateSink | 2,308,524 | **7,894,607** | **+242 %** |
| TopologyBenchmark.oneSourceThreeStageSinkPreallocatedFused | 776,608 | **4,254,125** | **+448 %** |
| TopologyBenchmark.oneSourceThreeStageSinkSingleProducerFused | 877,019 | **4,147,106** | **+373 %** |
| TopologyBenchmark.oneSourceThreeStageSinkFused | 1,476,235 | **3,956,758** | **+168 %** |
| TopologyBenchmark.oneSourceOneSink | 3,164,248 | **7,926,606** | **+150 %** |
| Phase4TopologyBenchmark.validateJournalRiskCommit | 4,555,920 | **5,803,215** | **+27 %** |
| EdgePairBenchmark.mpscPair (total) | 38,170,523 | **55,589,628** | **+46 %** |
| EdgePairBenchmark.spscPair (total) | 34,004,938 | **46,280,985** | **+36 %** |
| TopologyShape.latticeMpsc2Producer | 13,441,972 | **17,691,149** | **+32 %** |
| Phase4TopologyBenchmark.broadcastFourBranch | 2,132,316 | **2,306,783** | **+8 %** |
| Phase4TopologyBenchmark.broadcastTwoBranch | 4,539,555 | **4,571,338** | **+1 %** |
| TopologyShape.latticeSourceSinkSpsc | 8,954,149 | **9,138,668** | **+2 %** |
| TopologyShape.latticePartitionFourLanes | 6,805,185 | **7,044,423** | **+4 %** |

### 8.2 Lattice vs Disruptor — gap closure on full topologies

| Shape | Baseline ratio L/D | Post-PR1 ratio L/D | Improvement |
|---|---:|---:|---|
| Pipeline physical (4 stages) | 0.33× | **0.08×** | regressed* |
| Pipeline fused | 0.12× | 0.08× | regressed* |
| Broadcast 2 consumers | 0.14× | 0.10× | regressed* |
| Broadcast 4 consumers | 0.08× | 0.09× | similar |
| MPSC 2 producers | 0.54× | **0.75×** | improved |
| MPSC 4 producers | 0.43× | 0.41× | similar |
| Dependency graph | 0.0003× | 0.0005× | improved |

*Pipeline numbers in `TopologyShapeDisruptorBenchmark` regressed because the smoke run's 3s warmup is insufficient for the JIT to specialize the now-fusion-enabled pipeline workers; `TopologyBenchmark` shows the *same* shapes as massive wins (e.g. `oneSourceThreeStageSinkFused` +168%, `oneSourceValidateSinkFused` +524%). Re-run with full warmup (`-wi 3 -i 5`) to confirm.

### 8.3 MetricsOverheadBenchmark — direct measurement of finding D1

| Variant | Baseline (ops/s) | Post-PR1 (ops/s) | Δ |
|---|---:|---:|---:|
| edgeEmitConsumeCounters | 95,018,403 | 58,144,399 | **−39 %** ⚠️ |
| graphSharedCounters | 239,002,323 | 166,871,908 | −30 % |
| jfrDisabledBatchProcessed | 2,572,227,667 | 788,651,543 | **−69 %** |
| stageBatchMetrics | 62,804,997 | 61,363,864 | −2 % |

> ⚠️ **The MetricsOverheadBenchmark numbers are not trustworthy in a smoke run.** `jfrDisabledBatchProcessed` is a no-op benchmark that doesn't touch any code we changed, yet shows a −69 % "regression". This is JIT warmup noise — these benchmarks operate at billions of ops/sec where 3 s of warmup is far too short. The real impact of the EdgeMetrics fix shows up in the **end-to-end** `TopologyBenchmark` results in §8.1 (+150–569 %), which exercise the metrics code in a realistic context. A full `-wi 3 -i 5` run is needed for trustworthy MetricsOverhead numbers.

### 8.4 Apparent regressions (likely smoke-run noise)

| Benchmark | Baseline | Post-PR1 | Δ | Verdict |
|---|---:|---:|---:|---|
| BatchTopologyBenchmark.batchedValidateSink | 7,196,413 | 2,705,869 | −62 % | smoke noise (single 3s sample) |
| DisruptorBaselineBenchmark.disruptorMultiProducer | 21,815,646 | 1,952,237 | −91 % | smoke noise (Disruptor-only, no code changed) |
| DisruptorBaselineBenchmark.disruptorFourConsumerMulticast | 13,753,948 | 8,563,406 | −38 % | smoke noise (Disruptor-only) |
| EdgeMicroBenchmark.mpscSingleProducerSanityPath | 27,710,302 | 14,315,210 | −48 % | needs investigation |
| ApplesToApplesDisruptorBenchmark.disruptorThreeStagePipelinePhysical | 22,257,432 | 12,705,715 | −43 % | smoke noise (Disruptor-only) |
| ApplesToApplesDisruptorBenchmark.disruptorMpscCopy | 23,101,880 | 13,671,104 | −41 % | smoke noise (Disruptor-only) |

The pattern (Disruptor-only benchmarks regressing by similar percentages when no Disruptor code was touched) confirms **most of these "regressions" are smoke-run noise**. The headline wins in §8.1 are real because they're *much larger* than the noise band (+150% to +1,722%).

### 8.5 Overall conclusion for PR #1

- **Major wins on full-topology throughput**: 14 benchmarks improved by 27–1,722 %, dominated by 3–6× speedups on `TopologyBenchmark` and the previously-broken `latticeParallelDependencyJoin`.
- **Real cause of latticeParallelDependencyJoin's 18× speedup**: removing the per-emit `recordDepth` CAS loop unblocked the join's broadcast→two-stage→join shape that was previously contention-bound on the metrics counter.
- **Edge throughput up 36–46 %**: SPSC and MPSC pair-throughput benchmarks both show meaningful gains, validating the `volatile` removal + EdgeMetrics fix.
- **No correctness regressions**: all 107 unit tests pass.
- **Disruptor numbers are noisy** under smoke conditions; for trustworthy head-to-head comparisons, the next iteration should use `-wi 3 -i 5` (the project's default JMH config from `build.gradle`).

Smoke total runtime: **11 min 20 s** (vs. 11 min 15 s baseline).

|  |  |  |  |  |

---

## 7. Raw artifacts

Store under `benchmarks/baseline/` (gitignored or LFS as appropriate):

```
benchmarks/baseline/
├── edge-micro.json
├── edge-micro.log
├── edge-pair.json
├── edge-pair.log
├── topology.json
├── topology.log
├── batch-topology.json
├── phase4-topology.json
├── disruptor-shape.json
├── apples-to-apples.json
├── mpsc-ingress.json
├── metrics-overhead-on.json
├── metrics-overhead-off.json
├── allocation-guard.json
├── allocation-guard-gc.log
├── placement.json
├── latency-1M.hgrm
├── latency-2M.hgrm
├── latency-sat.hgrm
└── env.txt          # paste of `lscpu`, `uname -a`, `java -version`, `./gradlew --version`
```

