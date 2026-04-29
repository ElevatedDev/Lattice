# Edge Semantics

Every edge in a Lattice graph is a bounded ring buffer with a fixed
single-producer/single-consumer (SPSC) or multi-producer/single-consumer
(MPSC) shape. There is no unbounded queue option by design.

## Capacity

- Capacity is power-of-two; the builder rounds up if needed.
- The compiler may shrink an edge's physical buffer to zero slots when
  [linear fusion](source-specialization-and-fusion.md) runs the chain on the
  producer thread.
- Capacity is per-edge. There is no global queue domain.

## SPSC

- Single producer thread, single consumer thread, statically known.
- Plain-claim publication, release/acquire ordering on `tail` / `head`.
- Producer cursor and consumer cursor live in cache-line-padded boxes
  (`PaddedLongCache`) so they do not false-share.
- The producer owns `tail`; the consumer owns `head`. Control-plane close does
  not mutate producer-owned SPSC cursors.
- Closing an SPSC edge is a one-way transition via a separate close flag. The
  fastest `SourceMode.SINGLE_PRODUCER` contract requires close/stop/abort not
  to race active application `emit(...)` calls.

## MPSC

- Many producer threads, single consumer thread.
- CAS on `tail` to reserve, plain claim into the slot, release publication.
- The consumer's read of its local `head` cursor uses opaque ordering; the
  acquire load is on the producer-published sequence.
- Reservation order, not wall-clock order, defines downstream order.

## Payload Ownership

- POJO payloads are user-owned references. Lattice does not copy.
- Slab handles (`SlabHandle<T>`) are reference-counted, single-owner payloads
  issued by a
  [`SlabPool`](../src/main/java/com/lattice/slab/SlabPool.java).
  Acquire on the producer side, release at the terminal sink (or each branch
  of a broadcast). The compiler validates retain/release pairing for the
  topologies it accepts; it rejects unsupported reuse shapes at build time.

## Overflow

The producer side observes capacity. When the ring is full, the configured
[`OverflowPolicy`](backpressure.md) decides what happens: block, time-bounded
block, fail-fast, lossy, coalesce, or redirect.

## Closing & Draining

- Closing a source drains accepted items already in flight.
- `abort()` closes all edges first, interrupts workers, awaits termination,
  then drains any remaining items. If workers do not terminate, cleanup is
  left to the final worker-stop path rather than racing a live consumer.
- A closed edge releases any outstanding slab handles when it is drained.
