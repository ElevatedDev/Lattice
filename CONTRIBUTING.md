# Contributing

Lattice is still moving toward Phase 5 release hardening. Contributions should
keep the runtime aligned with the five-phase plan and preserve the hot-path
discipline documented there.

## Development Setup

Required for Java development:

- JDK 21, matching the current Gradle toolchain.
- Gradle through the checked-in wrapper.

Optional for Linux placement work:

- Rust and Cargo.
- A Linux host for the JNI backend.
- A production-like multi-NUMA host for placement benchmark claims.

Run the core checks before opening a change:

```bash
./gradlew test
./gradlew jmhClasses
./gradlew jcstress
```

On Windows, use `.\gradlew.bat` for the same tasks.

## Pull Requests

Before opening a pull request:

- Keep the change scoped to one behavioral or documentation concern.
- Include a short description of the user-visible effect and the verification
  commands you ran.
- Link related issues or design notes when they exist.
- Update `CHANGELOG.md` for user-facing runtime, API, release, or docs changes.
- Avoid checking in local benchmark logs unless the docs explicitly cite them as
  release evidence.

## Scope Discipline

The runtime is intentionally scoped to compiled static graphs. Avoid adding
features that imply dynamic topology mutation, distributed execution, hidden
persistence, automatic cloud orchestration, or transparent work stealing unless
the product plan is explicitly changed.

Public API changes require extra care. The phase plan treats these types as
stable product contracts:

- `StaticGraph`
- `StaticGraph.Builder`
- `Emitter`
- `StageLogic`
- `BatchStageLogic`
- `Output`
- `StageContext`
- `StageHandle`
- `GraphPlan`
- `GraphPlan.Node`
- `GraphPlan.Edge`
- `GraphPlan.Placement`
- `GraphState`
- `SourceMode`
- `PreallocationSpec`
- `PreallocatedEmitter`
- `GraphMetrics`
- `GraphMetrics.StagePlacement`
- `StageMetrics`
- `EdgeMetrics`
- `WaitMetrics`
- `PlacementStatus`
- `WorkerState`
- `EdgeSpec`
- `StageSpec`
- `PinPolicy`
- `WaitSpec`
- `OverflowPolicy`
- `BatchPolicy`
- `MemoryMode`
- `DispatchSpec`
- `BroadcastSpec`
- `PartitionSpec`
- `JoinSpec`
- `JoinGroup`
- `Stamped`
- `SlabPool`
- `SlabHandle`

Unsupported options should fail with clear diagnostics instead of silently
degrading, except where fallback behavior is explicitly part of the contract
such as non-strict native placement.

## Hot-Path Rules

Per-message paths should avoid allocation, logging, reflection, lock
acquisition, exception construction, and native calls. Native placement work
belongs at worker bootstrap, not in send/poll/process loops.

Allowed hot-path operations include:

- array or memory access;
- VarHandle acquire/release/opaque/CAS/FAA where justified;
- monotonic counter updates;
- direct stage invocation;
- wait-policy checks;
- metrics that have been measured and isolated from the critical path.

## Test Expectations

Match test depth to the behavioral risk:

- Graph DSL and validation changes need JUnit coverage.
- Edge memory-ordering changes need JCStress coverage.
- Wait, backpressure, lifecycle, and shutdown changes need race-oriented tests.
- Performance-sensitive changes need JMH coverage or a reason why an existing
  benchmark already covers the path.
- Placement changes need fallback tests and strict-mode tests where possible.

Benchmark numbers in documentation should identify the host, JVM, native
library path, CPU topology, command line, warmup/measurement configuration, and
the exact raw artifact that produced the quoted number.

## Documentation

User-visible behavior changes should update the relevant guide under `docs/`.
Keep documentation explicit about what is implemented, what is planned, and
what remains outside the current public contract.

The `/docs` directory is the GitHub Pages source. Keep `docs/index.md` usable
as the Pages entry point and `docs/README.md` usable as the GitHub directory
landing page.

## Security

Do not publish suspected vulnerabilities in public issues before maintainers
have had a chance to respond privately. See `SECURITY.md`.
