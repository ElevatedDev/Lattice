# API Reference

Release Javadoc is intended to be published to GitHub Pages for each stable
release:

- **Latest release**: <https://elevateddev.github.io/Lattice/api/latest/>
- **By version**: `https://elevateddev.github.io/Lattice/api/<version>/`
  (e.g. `api/1.0.0/`).

No stable Maven artifact has been published yet. Until the first release, build
from source and use `./gradlew javadoc` for local API docs.

This page summarizes the public packages and the stability contract for each
type. See the repository
[`CONTRIBUTING.md`](https://github.com/ElevatedDev/Lattice/blob/main/CONTRIBUTING.md)
for the full stable-surface list and the
[Compatibility Matrix](compatibility-matrix.md) for the versioning policy.

## Public Packages

| Package | Purpose | Stability |
| --- | --- | --- |
| `com.lattice.graph` | `StaticGraph`, `StaticGraph.Builder`, `GraphPlan`, `GraphState`, `SourceMode`, `PreallocationSpec`, `GraphBuildException`, `GraphRuntimeException`. | **Stable** |
| `com.lattice.stage` | `StageLogic`, `BatchStageLogic`, `Output`, `StageContext`, `StageHandle`, `StageSpec`, `Emitter`, `PreallocatedEmitter`, `Batch`, `BatchPolicy`, `StageExceptionHandler`, `StageExceptionAction`. | **Stable** |
| `com.lattice.edge` | `EdgeSpec`, `OverflowPolicy`, `BackpressureException`. | **Stable** |
| `com.lattice.metrics` | `GraphMetrics`, `StageMetrics`, `EdgeMetrics`, `WaitMetrics`, `PlacementStatus`, `WorkerState`. | **Stable** |
| `com.lattice.placement` | `PinPolicy`, `MemoryMode`, related strategy types. | **Stable** |
| `com.lattice.routing` | `dispatch` / `broadcast` / `partition` / `join` specifications. | **Stable** |
| `com.lattice.slab` | `SlabPool`, `SlabHandle` and supporting types. | **Stable** |
| `com.lattice.wait` | `WaitSpec`, wait-strategy primitives. | **Stable** |
| `com.staticgraph.runtime.nativeaccess` | Optional native topology capability checks and JNI access types. | **Experimental** |

## Stability Contract

The stability labels above are the current documentation contract. The project
does not yet ship public `@Stable`, `@Experimental`, or `@Internal`
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

The published jar declares `module com.lattice`.
