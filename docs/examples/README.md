# Examples Overview

Runnable examples live under
[`src/examples/java/com/lattice/examples/`](https://github.com/ElevatedDev/Lattice/tree/main/src/examples/java/com/lattice/examples).
Each is `public static void main` and uses only the public API.

| Example | What it shows |
| --- | --- |
| [PreallocatedSourceSinkExample](https://github.com/ElevatedDev/Lattice/blob/main/src/examples/java/com/lattice/examples/PreallocatedSourceSinkExample.java) | Reusing payloads through a preallocated source and slab pool. |
| [FusedLinearPipelineExample](https://github.com/ElevatedDev/Lattice/blob/main/src/examples/java/com/lattice/examples/FusedLinearPipelineExample.java) | Linear chain that the compiler fuses into a single owner thread. |
| [RoutingJoinExample](https://github.com/ElevatedDev/Lattice/blob/main/src/examples/java/com/lattice/examples/RoutingJoinExample.java) | Partition + dispatch + stamped join with explicit policy. |
| [MetricsDiagnosticsExample](https://github.com/ElevatedDev/Lattice/blob/main/src/examples/java/com/lattice/examples/MetricsDiagnosticsExample.java) | Reading graph / stage / edge / placement metrics at runtime. |
| [BenchmarkStyleFastPathExample](https://github.com/ElevatedDev/Lattice/blob/main/src/examples/java/com/lattice/examples/BenchmarkStyleFastPathExample.java) | The hot-path discipline used by the JMH suite. |
| [OrdersPipelineExample](https://github.com/ElevatedDev/Lattice/blob/main/src/examples/java/com/lattice/examples/OrdersPipelineExample.java) | Order validate → enrich → risk → commit pipeline end-to-end. |

## Running

```bash
./gradlew examplesClasses

java -p build/classes/java/main:build/classes/java/examples \
     -m com.lattice/com.lattice.examples.OrdersPipelineExample
```

On Windows, swap `:` for `;` and use `gradlew.bat`.

The examples deliberately use the same hot-path discipline as the JMH
benchmarks (no logging, no allocation in stage bodies, hot-counter flags
disabled) so they are useful as starting templates, not just demos.
