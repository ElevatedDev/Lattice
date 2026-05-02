# Ordering Guarantees

Lattice's ordering contract is intentionally local and explicit.

![Runtime guarantee map](assets/guarantees-map.svg)

## Per-Edge

- **SPSC**: producer order is preserved exactly for accepted items. Items
  rejected or redirected by an overflow policy are not delivered.
- **MPSC**: the order of items accepted into the ring matches the order of
  successful CAS reservations on the tail cursor. This is *not* the same as
  wall-clock order across producer threads.

## Across The Graph

- A linear chain `source → stage₁ → ... → sink` preserves per-source order
  end-to-end whether the chain runs physically (one edge per hop), through
  normal downstream fusion (eligible stage/sink chain runs on the owner
  worker), or through source-inline fusion (eligible chain runs on the producer
  thread after `FusionSpec.inlineSources(true)`).
- A `broadcast` node replicates each item to every downstream branch in the
  same logical order; the *physical* arrival order at sinks across branches
  is independent.
- A `partition` node preserves per-key order: every item routed to the same
  lane preserves its source-side order, but order between lanes is not
  defined.
- A `dispatch` node preserves per-destination order for the same reason.
- A `join` correlates inputs by stamp with an explicit
  duplicate/timeout/missing policy. Joined output order follows stamp arrival
  on the *primary* branch unless the join policy says otherwise.

## What Lattice Does Not Promise

- No global sequence domain across edges (use Disruptor's sequence barriers
  for that semantic).
- No exactly-once external effects.
- No replay, persistence, or transactional rewind.
- MPSC is not wall-clock fair across producers.

See [Edge Semantics](edge-semantics.md) for the memory-ordering primitives that
back these claims, and [Failure Modes](failure-modes.md) for what happens when
ordering interacts with abort and fail-stop.
