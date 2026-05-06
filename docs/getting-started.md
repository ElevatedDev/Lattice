# Getting Started

This guide walks you through running your first Lattice graph in about ten
minutes.

## Requirements

- JDK 21 (matching the project's Gradle toolchain).
- The checked-in Gradle wrapper (`./gradlew` / `gradlew.bat`).
- Optional: Rust 1.85+ + Cargo, only if you want the [native placement
  backend](../README.md#native-placement-backend)
  for Linux affinity and NUMA diagnostics.

## Build And Smoke-Test

```bash
./gradlew test
./gradlew jmhClasses examplesClasses
```

The portable release gate is:

```bash
./gradlew releaseCheck
```

This compiles main, examples, tests, JMH, JCStress; builds the runtime jar,
sources jar, and javadoc jar; generates the Maven POM; and verifies the
documentation links you are reading right now. It also generates a CycloneDX
SBOM and enforces the current JaCoCo coverage floor: at least 80% line and 65%
branch coverage on the release scope.

The Maven Central coordinate is:

```kotlin
implementation("io.github.elevateddev:lattice:1.0.0")
```

## Your First Graph

```java
import io.github.elevateddev.lattice.edge.EdgeSpec;
import io.github.elevateddev.lattice.graph.SourceMode;
import io.github.elevateddev.lattice.graph.StaticGraph;
import io.github.elevateddev.lattice.stage.Emitter;
import io.github.elevateddev.lattice.stage.StageSpec;
import java.time.Duration;

record Order(int id, boolean valid) {}
record ValidOrder(int id) {}

StaticGraph graph = StaticGraph.builder("orders")
    .source("ingress", Order.class, SourceMode.SINGLE_PRODUCER)
    .stage("validate", Order.class, ValidOrder.class,
        (order, out, ctx) -> { if (order.valid()) out.push(new ValidOrder(order.id())); },
        StageSpec.singleThreaded())
    .sink("egress", ValidOrder.class, order -> { /* persist */ }, StageSpec.singleThreaded())
    .edge("ingress",  "validate", EdgeSpec.spscRing(1024))
    .edge("validate", "egress",   EdgeSpec.spscRing(1024))
    .build();

graph.start();

Emitter<Order> ingress = graph.emitter("ingress", Order.class);
ingress.emit(new Order(1, true));
ingress.close();

graph.awaitTermination(Duration.ofSeconds(5));
```

`SourceMode.SINGLE_PRODUCER` is a correctness contract: at most one
application thread may call the emitter at a time. Use the default source mode
and an MPSC ingress edge when producers are concurrent.

## What To Read Next

- [Graph DSL](graph-dsl.md) - every builder method.
- [Edge Semantics](edge-semantics.md) - SPSC vs MPSC, capacity rules, slab
  handles.
- [Ordering Guarantees](ordering-guarantees.md) - exactly what Lattice promises.
- [Backpressure](backpressure.md) - overflow policies, sizing, redirect.
- [Observability](observability.md) - metrics, JFR events, placement reports.
- [Architecture](architecture.md) - how Lattice compiles your graph.
- [Examples Overview](examples/README.md) - runnable, JMH-grade examples.
