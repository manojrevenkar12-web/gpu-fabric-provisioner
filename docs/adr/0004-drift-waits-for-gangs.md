# ADR 0004: Image rollouts replace nodes with surge and wait for running gangs

Status: accepted

## Context

When a pool's image (or instance type, labels, taints) changes, every node made from the old
template must be replaced. Evicting one member of a training gang kills the whole job, which
may have run for days; evicting a service replica is routine if its PodDisruptionBudget allows.

## Decision

- Drift is a template hash (`GPUNodePool.TemplateHash`) stored on each NodeClaim at creation.
- Empty drifted nodes are deleted at once, within the pool's `maxUnavailable` budget.
- A drifted node running only pods that a controller recreates gets a replacement claim in the
  same fabric first (`maxSurge` at a time). When the replacement is initialized, the old node is
  cordoned and drained through the Eviction API, so PDBs hold. When it is empty it is deleted.
- A drifted node running any gang member is left alone (`gangPolicy: WaitForCompletion`) and
  becomes an empty drifted node when the gang finishes. `Never` leaves it for an operator.
- Pods marked `gpufabric.dev/do-not-disrupt`, and pods no controller would recreate, block
  voluntary disruption of their node.

## Consequences

- A rollout on a cluster of long training jobs takes as long as the longest job. That is the
  intended trade: the alternative is restarting jobs, which costs more than the old image.
- In the upgrade drill, both gangs completed with zero restarts, the service's PDB
  (minAvailable 5 of 6) was never violated, and no node was left on the old image.
- Not built: a maintenance window or a deadline after which gangs are restarted together.
