# Changelog

All notable user-facing changes should be documented here.

This project has not published a stable release yet. The current repository
state is pre-1.0 and follows the five-phase development plan in
`docs/static-topology-numa-runtime-5-phase-plan.md`.

## Unreleased

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
