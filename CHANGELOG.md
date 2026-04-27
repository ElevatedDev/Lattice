# Changelog

All notable user-facing changes should be documented here.

This project has not published a stable release yet. The current repository
state is pre-1.0 and follows the five-phase development plan in
`docs/static-topology-numa-runtime-5-phase-plan.md`.

## Unreleased

### Added

- `-Dlattice.fusion.validateTypes` (default `false`): re-enables defensive
  per-event runtime type assertions between fused stages and the terminal
  sink. The fused chain is built at graph build time from already type-checked
  stage definitions and the public ingress emit boundary still validates
  user-supplied items, so the intra-fused checks add cost without adding
  safety on the hot path. Flip this on while developing custom `StageLogic`.

### Changed

- **Inline source-side fusion is now on by default** (`lattice.fusion.inlineSource=true`).
  The change is correctness-preserving for graphs that did not opt in to
  `SourceMode.SINGLE_PRODUCER` (those topologies are not eligible). Eligible
  single-producer graphs now skip the source->fused-worker SPSC handoff and
  execute the fused chain synchronously on the producer thread by default.
  Set the property to `false` explicitly to keep the producer thread isolated
  from the stage and sink work.
- The fused stage/sink chain executor (`LinearStageSinkLoop`) was rebuilt for
  the JIT:
  - Each `LinearStageOutput` now holds a `final` reference to its successor
    rather than indexing into a shared `Output[]` array. This removes the
    per-event array load that defeated field-folding on the hot path and lets
    the JIT inline through every fused hop monomorphically.
  - Chain hops are now specialized into `Benign` (non-handle payload) and
    `Retaining` (handle-bearing payload) variants chosen at wire time. The
    `Benign` variants drop the per-hop `try/catch/finally` frame and the
    handle-ownership scope branch entirely; only the chain-entry hop wraps
    the chain in a single translator try/catch so user-visible
    `FusedStageException` semantics are preserved without each hop paying
    for its own exception frame. The common case (plain POJO payloads) sees
    a measurably tighter inner loop.
  - All metric/JFR gates on the fused hot path now read static-final getters
    (`StageMetrics.hotCountersEnabled()`, `StageMetrics.histogramsEnabled()`,
    `JfrEvents.enabled()`) instead of per-instance booleans. With the
    recommended max-throughput profile (`-Dlattice.metrics.hotCounters=false`
    `-Dlattice.metrics.stageHistograms=false` `-Dlattice.jfr=false`
    `-Dlattice.runtime.fusedLogicalEdgeMetrics=false`) the entire metrics and
    `System.nanoTime()` blocks fold away at JIT time. The `recordLogicalTransfer`
    gate is now a single static-final boolean (`LOGICAL_METRICS_ON =
    StageMetrics.hotCountersEnabled() && FUSED_LOGICAL_EDGE_METRICS`) so the
    JIT removes the entire call shell on the fused hot path when off.
  - Intra-fused `validate()` calls (null check + class identity check) are
    now gated behind `-Dlattice.fusion.validateTypes` (default `false`).
- `SpscRingEdge` producer-side `producerHeadCache` and consumer-side
  `consumerTailCache` are now stored in dedicated cache-line-padded boxes
  (`PaddedLongCache`) so they no longer false-share with the published
    cursors or each other.

### Changed (earlier in Unreleased)

- `SourceEmitter.emit` now routes through the trusted fast-path
  (`EdgeSender.emitTrustedFromSource`) when the source is non-stamping and
  the message type cannot carry a slab handle. This collapses the steady-state
  emit to "abort-check + edge.offer + record" and inlines cleanly with JIT.
- `MpscRingEdge` consumer-side reads of the local `head` cursor were lowered
  from full-volatile (`get`) to `getOpaque`. The producer-published-sequence
  acquire load already provides the only ordering the consumer needs.

### Added

- Phase 5 public documentation set:
  - getting started guide;
  - graph DSL guide;
  - edge semantics guide;
  - ordering guarantees guide;
  - backpressure guide;
  - observability guide;
  - operations runbook;
  - failure modes guide;
  - compatibility matrix;
  - contribution and security policy drafts.

### Current Runtime Surface

- Static graph API with sources, stages, sinks, dispatch, broadcast,
  partition, joins, and stamped sources.
- Bounded SPSC and MPSC ring edges.
- Blocking, timed blocking, fail-fast, lossy, coalescing, and redirect overflow
  policies.
- Lifecycle states, graceful stop, quiesce/resume, abort, fail-stop behavior,
  and stage exception handling.
- Batch stage and edge-local batching policy support.
- Optional Rust JNI backend for Linux affinity and NUMA diagnostics.
- Graph, stage, edge, placement, wait, routing, join, slab, and JFR
  observability surfaces.
- JUnit, JCStress, and JMH verification harnesses.

### Known Release Gaps

- No published Maven artifacts or JPMS module descriptor yet.
- Java 21 is the current build baseline; JDK 25 and JDK 26 validation remain
  Phase 5 work.
- No FFM/jextract native backend yet.
- Checked-in Windows benchmark artifacts exist; no Linux/NUMA
  publication-grade benchmark report yet.
- No Micrometer or JMX integration yet.
- Maven Central signing and remote publishing are not configured yet.
