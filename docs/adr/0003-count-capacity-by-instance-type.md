# ADR 0003: Capacity is counted from the instance type, not from what a node advertises

Status: accepted

## Context

A GPU server registers its Node before the GPU driver and device plugin are up. For a while it
is Ready with 0 allocatable `nvidia.com/gpu`. An autoscaler that counts allocatable resources
sees a node that cannot run the pending pod and buys another; Nebius documents this for its
node groups with the NVIDIA operators installed, and Red Hat for OpenShift.

## Decision

Every NodeClaim counts as the full capacity of its instance type from the moment it exists:
Pending (not yet accepted by the cloud), Launched (booting), Registered (Node exists, GPUs not
advertised) and Initialized. Pending claims also reduce the fabric's available hosts, because
the cloud's offerings do not include them yet. A claim stops counting when it is deleted.

A claim becomes Initialized only when its Node is Ready, has no startup or not-ready taint, and
advertises every GPU of the instance type. Gangs are released only onto initialized nodes. A node
that does not get there within `initializationTTL` is replaced.

## Consequences

- In the burst drill this provisioner bought 56 servers for 54 servers of work; the naive
  baseline, which counts allocatable GPUs, bought 76.
- Offerings are a snapshot. A claim launched after the snapshot was read is still subtracted from
  it (condition timestamps have one-second resolution, so with a second's margin). Before this,
  the burst drill sent 20 creates into fabrics that were already full; after it, none.
- A server that never advertises its GPUs is not planned around forever: it is replaced after
  `initializationTTL`, and the gang it was meant for keeps waiting gated, not half started.
