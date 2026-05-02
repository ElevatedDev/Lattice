# Examples Overview

Runnable examples live under
[`src/examples/java/com/lattice/examples/`](../../src/examples/java/com/lattice/examples/).
Each is `public static void main` and uses only the public API.

| Example | What it shows |
| --- | --- |
| [PreallocatedSourceSinkExample](../../src/examples/java/com/lattice/examples/PreallocatedSourceSinkExample.java) | Reusing payloads through a preallocated source and slab pool. |
| [FusedLinearPipelineExample](../../src/examples/java/com/lattice/examples/FusedLinearPipelineExample.java) | Linear chain that the compiler fuses into a single owner thread. |
| [RoutingJoinExample](../../src/examples/java/com/lattice/examples/RoutingJoinExample.java) | Broadcast fan-out plus stamped join with explicit policy. |
| [MetricsDiagnosticsExample](../../src/examples/java/com/lattice/examples/MetricsDiagnosticsExample.java) | Reading graph / stage / edge / placement metrics at runtime. |
| [BenchmarkStyleFastPathExample](../../src/examples/java/com/lattice/examples/BenchmarkStyleFastPathExample.java) | The hot-path discipline used by the JMH suite. |
| [OrdersPipelineExample](../../src/examples/java/com/lattice/examples/OrdersPipelineExample.java) | Minimal validate -> sink order pipeline with wait, overflow, and memory knobs. |

## Running

```bash
./gradlew runOrdersPipelineExample
```

Other example tasks follow the same name pattern, for example
`runFusedLinearPipelineExample`, `runRoutingJoinExample`, and
`runMetricsDiagnosticsExample`.

The examples deliberately use the same hot-path discipline as the JMH
benchmarks (no logging, no allocation in stage bodies, hot-counter flags
disabled) so they are useful as starting templates, not just demos.
