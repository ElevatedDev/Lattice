# API Reference

Generated Javadocs are checked into `docs/api/latest/` so a static docs site
can serve the current public surface:

- **Latest checked-in API files**: [api/latest/](api/latest/index.html)
- **Rendered Javadocs**: available only after GitHub Pages or another static
  host serves the `docs/` directory.
- **By version, after Pages is enabled**: `api/<version>/`
  (for example `api/1.0.0/`).

No Maven Central artifact has been published yet. Until the first release,
build from source and use `./gradlew docsJavadoc` to refresh the checked-in
copy.

This page summarizes the public packages and the intended stability contract
for each type. See the repository [`CONTRIBUTING.md`](../CONTRIBUTING.md) for
the full stable-surface list and the
[Compatibility Matrix](compatibility-matrix.md) for the versioning policy.

## Using Before Maven Central

For local adoption before the first published artifact, install the jar into
your local Maven cache:

```bash
./gradlew publishToMavenLocal
```

Then depend on the current snapshot coordinate:

```kotlin
implementation("io.github.elevateddev:lattice:1.0-SNAPSHOT")
```

Lattice exports the JPMS module `com.lattice`. `HdrHistogram` is an API
dependency because public metrics accessors return defensive histogram copies,
so consumers using explicit module paths must keep `HdrHistogram` visible.

## Public Packages

| Package | Purpose | Stability |
| --- | --- | --- |
| `com.lattice.graph` | `StaticGraph`, `StaticGraph.Builder`, `GraphPlan`, `GraphPlan.Node`, `GraphPlan.Edge`, `GraphPlan.Placement`, `GraphState`, `SourceMode`, `PreallocationSpec`, `FusionSpec`, `MetricsSpec`, `GraphPlacementSpec`, `DiagnosticsSpec`, `GraphBuildException`, `GraphRuntimeException`. | **Stable** |
| `com.lattice.stage` | `StageLogic`, `BatchStageLogic`, `Output`, `StageContext`, `StageHandle`, `StageSpec`, `Emitter`, `PreallocatedEmitter`, `Batch`, `BatchPolicy`, `StageExceptionHandler`, `StageExceptionAction`. | **Stable** |
| `com.lattice.edge` | `EdgeSpec`, `OverflowPolicy`, `BackpressureException`. | **Stable** |
| `com.lattice.metrics` | `GraphMetrics`, `GraphMetrics.StagePlacement`, `StageMetrics`, `EdgeMetrics`, `WaitMetrics`, `PlacementStatus`, `WorkerState`. | **Stable** |
| `com.lattice.placement` | `PinPolicy`, `MemoryMode`, related strategy types. | **Stable** |
| `com.lattice.routing` | `dispatch` / `broadcast` / `partition` / `join` specifications. | **Stable** |
| `com.lattice.slab` | `SlabPool`, `SlabHandle` and supporting types. | **Stable** |
| `com.lattice.wait` | `WaitSpec`, wait-strategy primitives. | **Stable** |
| `com.lattice.nativeaccess` | Optional native topology capability checks and JNI access types. | **Experimental** |

## Stability Contract

The stability labels above are the intended public contract for 1.0. The
project does not yet ship public `@Stable`, `@Experimental`, or `@Internal`
annotations.

- **Stable** packages are covered by SemVer once 1.0.0 is published.
- **Experimental** packages may change in minor releases.
- Non-exported `com.lattice.internal.*` packages are not public API.

## Planned Maven Coordinate

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

The release jar declares `module com.lattice`.
