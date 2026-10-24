# ADR 0002: The provisioner plans from a full snapshot, not from events

Status: accepted

## Context

A provisioner can react to events ("a pod became unschedulable, buy a node for it") or compute
the whole desired state each time. Event-driven logic is cheap per event but must handle every
interleaving of lost, duplicated and reordered events, crashes between steps, and two replicas.

## Decision

Events only wake the provisioner. Each run waits until pods stop arriving (`batchIdle`, at most
`batchMax`), reads pools, claims, nodes and pods from the informer cache without deep copies, and
calls `plan.Compute`, a pure function from that snapshot and the cloud's offerings to a plan:
claims to create, gangs to place, gangs to release, and why the rest wait.

## Consequences

- A missed or duplicated event cannot change an outcome; the next run sees the same state.
- After a leader change, the new leader computes the same plan from the same objects. The crash
  drill kills the leader three times during a burst: no duplicate servers, no leaks.
- The cache can lag the provisioner's own writes. Before planning, the provisioner waits until
  it sees every claim it created (`awaitCache`), or it would buy the same servers twice.
- `plan.Compute` is deterministic and testable without an API server. At 20,000 nodes and 10,000
  pending pods it takes about 70 ms (`BenchmarkCompute`); the snapshot is the larger cost at scale.
