# Changelog

All notable user-facing changes should be documented here.

This project has not published a stable release yet. The current repository
state is pre-1.0.

## Unreleased

### Fixed

- **Inline source fusion now executes the owner stage's logic.** The previous
  wiring attached the source emitter directly to the worker's fused-chain
  entry, which was the output the owner stage uses to send into the chain --
  not an input that runs the owner. As a result, for a topology
  `source -> A -> B -> sink` where `A -> B` is fused, inline source emit
  silently skipped `A`'s logic and ran only `B` and the sink. The new
  `inlineEntryOutput()` runs `worker.logic.onMessage(item, output, context)`
  on the producer thread, so the owner stage is invoked exactly as on the
  ring path. Inline fusion is now also restricted to chains that terminate
  in a fused sink (no downstream-ring tail), so a producer-thread emit
  cannot block past user sink code, keeping `tryEmit` and timed `emit`
  semantics within the user-controlled portion of the chain.
- **Trusted source emit now validates.** `EdgeSender.emitTrustedFromSource`
  used to bypass `validateItem`, accepting `null` and the wrong runtime type.
  Because rings use `null` as the empty-slot sentinel and as the "not yet
  published" plain-claim signal, a null published by a producer was
  indistinguishable from "slot not ready" and could wedge the consumer.
  The trusted path now performs the same null + monomorphic class-equality
  checks the public emit boundary does. Cost: one always-not-null branch
  and one cached-class comparison on the source-thread hot path.
- **`StaticGraph#emitter(name, type)` type assignability is no longer
  reversed.** The old check accepted requesting a wider supertype of the
  declared source payload (e.g. `Number` for an `Integer` source), letting
  the caller construct an `Emitter<Number>` and emit `Double`s into the
  Integer-typed source -- the runtime would still throw at the producer's
  validation, but the API contract was wrong. The check now requires
  `exposedType.isAssignableFrom(type)`: callers may request the same type or
  a narrower subtype, never a wider supertype. Same fix applied to
  `preallocatedEmitter`.
- **SPSC source-ingress close now waits for an in-flight source offer.**
  Source-owned SPSC edges carry a close guard so `close()` cannot return while
  the producer thread is between the open check and the tail publish. Direct
  SPSC edges and worker-to-worker SPSC edges keep the existing fast path; they
  are made safe by worker quiescence before `releaseRemainingAfterQuiescence()`.

### Reviewed but not changed (with rationale)

- **`JoinSpec.ANY_OF` state lifecycle after first emission.** The reviewer
  flagged that distinct branches arriving for an already-emitted `ANY_OF`
  stamp do not advance `receivedCount`, so the join state remains in the
  table until timeout or capacity eviction. Changing this would have flipped
  the documented `DuplicatePolicy` semantics: existing tests
  (`anyOfDuplicateCountAndIgnoreAreObservableWithoutFailingGraph`,
  `anyOfDuplicateFailRejectsLaterSourceForAlreadyEmittedStamp`) encode
  Option A semantics where any arrival post-emit is a duplicate. The
  state-pinning effect is bounded by capacity / timeout and is not a
  correctness bug under Option A. A semantics revision (Option B: per-source
  seen tracking after emission) would be a deliberate API change and is
  out of scope for this round.

### Added

- Maven Central release wiring through Vanniktech's Central Portal publishing
  plugin, including signed publication tasks for
  `io.github.elevateddev:lattice`.
- Bundled native-library loading support. Release jars can carry platform
  libraries under `META-INF/native/lattice/<os>-<arch>/`, while
  `lattice.native.enabled=false` and `lattice.native.library.path=...` keep
  their explicit override behavior.
- JaCoCo coverage reporting and verification, CycloneDX SBOM generation,
  CodeQL, OpenSSF Scorecard, Dependency Review, release checksums, and GitHub
  artifact attestation workflow coverage.
- Native Rust tests now run in CI and in the per-platform release native
  artifact matrix before release binaries are packaged.
- Public trust wording that Lattice is currently used in production for drone
  telemetry workloads.
- `StaticGraph#compilationReport()` and public
  `GraphCompilationReport` rows for build-time worker, edge, sender, merge,
  and fallback decisions. Reports include stable reason codes such as
  `source.specialized_to_spsc`, `fusion.non_fusible_edge.overflow`, and
  `source_inline.multi_producer_source`, plus a human-readable
  `toSummaryString()` for support output.
- Per-graph runtime configuration on `StaticGraph.Builder`:
  `fusion(FusionSpec)`, `metrics(MetricsSpec)`,
  `placement(GraphPlacementSpec)`, and `diagnostics(DiagnosticsSpec)`.
  These replace process-global fusion, metrics, placement, first-touch, and
  JFR runtime flags for graph behavior.
- Phase 5 documentation set under `docs/`, including getting started, graph
  DSL, edge semantics, ordering guarantees, backpressure, observability,
  operations, failure modes, compatibility, API, release, examples, and
  benchmark result pages.
- GitHub Pages entry points: `docs/index.md`, `docs/README.md`, and
  `docs/_config.yml`.
- GitHub-recognized project metadata files: `SECURITY.md`,
  `CODE_OF_CONDUCT.md`, `NOTICE`, and `.editorconfig`.
- Open-source repository scaffolding: GitHub Actions CI/release artifact
  workflows, issue template, pull request template, Dependabot config, and
  `.gitattributes`.
- `FusionSpec.validateTypes(true)` to re-enable defensive per-event runtime
  type assertions inside fused chains while developing custom stage logic.
- `OptimalPathBenchmark`, a completion-gated Lattice-vs-Disruptor comparison
  that waits for sink/handler completion of the same sequence instead of
  measuring enqueue rate only.
- Publication benchmark figures for throughput, latency percentiles,
  Lattice-vs-Disruptor ratios, and runtime guarantees under `docs/assets/`,
  including isolated stage artifacts for the Lattice-vs-Disruptor rows.
- Generated GitHub Pages Javadocs under `docs/api/latest/`.

### Changed

- Root `README.md` rewritten around the public contract, quick start,
  verification commands, native placement, benchmark caveats, and the new docs
  index.
- Maven coordinates are aligned to `io.github.elevateddev:lattice`, and
  `HdrHistogram` is exposed as an API dependency because histogram types appear
  in public metrics accessors.
- Optional native access classes moved from the old `com.staticgraph.*`
  namespace to `io.github.elevateddev.lattice.nativeaccess` before the first public release.
- `CONTRIBUTING.md` expanded with scope, hot-path, test, documentation, and
  security expectations for public contributions.
- `SECURITY.md` rewritten for pre-1.0 support status and private vulnerability
  reporting.
- Inline source-side fusion is now a per-graph opt-in through
  `FusionSpec.inlineSources(true)`. Normal downstream fusion remains enabled by
  default through `FusionSpec.defaults()`, while source physical-path elision is
  separately gated by `elideInlineSourcePhysicalPath(true)`.
- The fused stage/sink chain executor was tightened for the JIT by using final
  successor references, specialized benign/retaining hop variants, a default
  no-observability fast path, and optional intra-fused type validation.
- `SpscRingEdge` producer and consumer cursor caches now use dedicated
  cache-line-padded boxes.
- `SourceEmitter.emit` uses the trusted fast path when the source is
  non-stamping and the message type cannot carry a slab handle.
- `MpscRingEdge` consumer-side local head reads were lowered from volatile to
  opaque where acquire publication already supplies the required ordering.
- Native backend support now includes partial Windows x86_64 and macOS 64-bit
  implementations instead of a blanket non-Linux ENOSYS stub. Linux remains
  the only backend with full affinity, NUMA query, and local memory policy.

### Fixed

- Documentation now avoids claims about public stability annotation classes
  that do not exist in the current module.
- Benchmark documentation now points at the refreshed checked-in
  `benchmarks/baseline/` artifact set and
  `docs/benchmark-results/v1.0.0-baseline/` snapshot instead of older local
  result paths.
- Benchmark documentation now presents the 2026-04-29 result set as the
  current public baseline, with raw artifacts, fair-comparison scope, and JMH
  error bars attached to the figures.

### Known Gaps

- No published Maven Central artifacts yet.
- JDK 25 and JDK 26 validation remain future release work.
- No FFM/jextract native backend yet.
- No Micrometer or JMX integration yet.
