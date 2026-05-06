# API Reference

Generated Javadocs are checked into `docs/api/latest/` so a static docs site
can serve the current public surface:

- **Latest checked-in API files**: [api/latest/](api/latest/index.html)
- **Rendered Javadocs**: available only after GitHub Pages or another static
  host serves the `docs/` directory.
- **Current release path after Pages is enabled**: `api/latest/`.

Maven Central release metadata is configured for
`io.github.elevateddev:lattice`. Use `./gradlew docsJavadoc` to refresh the
checked-in copy before cutting a release.

This page summarizes the public packages and the intended stability contract
for each type. See the repository [`CONTRIBUTING.md`](../CONTRIBUTING.md) for
the full stable-surface list and the
[Compatibility Matrix](compatibility-matrix.md) for the versioning policy.

## Maven Central

Lattice exports the JPMS module `io.github.elevateddev.lattice`. `HdrHistogram` is an API
dependency because public metrics accessors return defensive histogram copies,
so consumers using explicit module paths must keep `HdrHistogram` visible.

Depend on:

```kotlin
implementation("io.github.elevateddev:lattice:1.0.0")
```

## Public Packages

| Package | Purpose | Stability |
| --- | --- | --- |
| `io.github.elevateddev.lattice.graph` | `StaticGraph`, `StaticGraph.Builder`, `GraphPlan`, `GraphPlan.Node`, `GraphPlan.Edge`, `GraphPlan.Placement`, `GraphCompilationReport`, `GraphState`, `SourceMode`, `PreallocationSpec`, `FusionSpec`, `MetricsSpec`, `GraphPlacementSpec`, `DiagnosticsSpec`, `GraphBuildException`, `GraphRuntimeException`. | **Stable** |
| `io.github.elevateddev.lattice.stage` | `StageLogic`, `BatchStageLogic`, `Output`, `StageContext`, `StageHandle`, `StageSpec`, `Emitter`, `PreallocatedEmitter`, `Batch`, `BatchPolicy`, `StageExceptionHandler`, `StageExceptionAction`. | **Stable** |
| `io.github.elevateddev.lattice.edge` | `EdgeSpec`, `OverflowPolicy`, `BackpressureException`. | **Stable** |
| `io.github.elevateddev.lattice.metrics` | `GraphMetrics`, `GraphMetrics.StagePlacement`, `StageMetrics`, `EdgeMetrics`, `WaitMetrics`, `PlacementStatus`, `WorkerState`. | **Stable** |
| `io.github.elevateddev.lattice.placement` | `PinPolicy`, `MemoryMode`, related strategy types. | **Stable** |
| `io.github.elevateddev.lattice.routing` | `dispatch` / `broadcast` / `partition` / `join` specifications. | **Stable** |
| `io.github.elevateddev.lattice.slab` | `SlabPool`, `SlabHandle` and supporting types. | **Stable** |
| `io.github.elevateddev.lattice.wait` | `WaitSpec`, wait-strategy primitives. | **Stable** |
| `io.github.elevateddev.lattice.nativeaccess` | Optional native topology capability checks and JNI access types. | **Experimental** |

## Stability Contract

The stability labels above are the intended public contract for 1.0. The
project does not yet ship public `@Stable`, `@Experimental`, or `@Internal`
annotations.

- **Stable** packages are covered by SemVer for the 1.x release line.
- **Experimental** packages may change in minor releases.
- Non-exported `io.github.elevateddev.lattice.internal.*` packages are not public API.

`GraphCompilationReport.Reason#code()` values are intended for diagnostics and
support tooling. They should be treated as stable public vocabulary for the
1.x release line.

## Maven Coordinate

```xml
<dependency>
  <groupId>io.github.elevateddev</groupId>
  <artifactId>lattice</artifactId>
  <version>1.0.0</version>
</dependency>
```

```kotlin
implementation("io.github.elevateddev:lattice:1.0.0")
```

The release jar declares `module io.github.elevateddev.lattice` and is prepared to carry bundled
native libraries under `META-INF/native/lattice/<os>-<arch>/`.
