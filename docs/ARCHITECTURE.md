# Architecture

```
                     webhook: gate gang pods                 kube-scheduler (unchanged)
  Job / JobSet ──▶ pods ──────────────────────▶ gated pods ──(gate removed)──▶ bound pods
                                                    │ ▲
                         trigger (batched 1–10 s)   │ │ narrow affinity to fabric + remove gate
                                                    ▼ │
  ┌──────────────────────────── leader ─────────────────────────────────────────────────┐
  │ Provisioner: cache snapshot ─▶ plan.Compute (pure) ─▶ create NodeClaims, place, release │
  │ Disruptor:   empty → delete · drifted → surge, drain (Eviction API), delete           │
  │ GC:          cloud instances without a claim · claims without an instance · orphan Nodes│
  └──────────────────────────────────────────────────────────────────────────────────────┘
                 │ NodeClaim                               ▲ offerings, quota (cached)
                 ▼                                         │
  NodeClaim controller: Pending ─create─▶ Launched ─Node─▶ Registered ─Ready+GPUs─▶ Initialized
                 │ idempotent on claim name                │ labels, taints, owner synced in one update
                 ▼                                         │
            cloud.Provider  ◀── httpcloud adapter / sim ───┘
```

## Objects

- **GPUNodePool** (cluster-scoped): instance type, allowed fabrics, image, labels, taints, min
  and max nodes, disruption settings (consolidateAfter, maxUnavailable, maxSurge, gangPolicy).
  CEL rejects `minNodes > maxNodes`. Status: nodes, initialized, in flight, drifted, GPUs, and the
  current template hash.
- **NodeClaim** (cluster-scoped): one server. Its spec is immutable (CEL `self == oldSelf`) and
  carries the template hash it was made under. Its finalizer (set at creation) guarantees the
  instance and Node are deleted before the claim disappears.

## Planning

`internal/plan` is a pure function. Inputs: pools, every claim as a node with its instance type's
capacity and the requests of pods bound to it, pending GPU pods (gated or not), offerings per
fabric, recently failed fabrics, and free quota.

1. Pods are grouped into gangs (`gpufabric.dev/gang` or Kueue's `pod-group-name`). Gangs with
   members already released or bound are fixed to their fabric and planned first.
2. Each other gang, by priority then age, is tried in every (pool, fabric) its pods may use:
   members go to initialized nodes first (best fit), then to in-flight nodes, then to new nodes,
   which must fit the fabric's free hosts, the pool's `maxNodes` and the quota. A trial that does
   not fit is rolled back: a gang never gets part of what it needs.
3. Of the placements that fit, the planner takes the one with the fewest new nodes; then one
   where the gang can start now; then the fabric with the fewest hosts left (keeping large
   fabrics free for large gangs); then the fabric chosen last time.
4. A gang whose members all landed on initialized nodes is released.
5. Single GPU pods fill existing nodes, then new nodes in the fabric with the fewest free hosts.
6. `minNodes` is topped up in the fabric with the most free hosts.

Free slots are bucketed by free GPUs, so a member is placed in O(GPUs per node). See
`internal/plan/bench_test.go` for 1,000 to 20,000 nodes.

## Claim lifecycle

| Phase | Leaves when | Or after |
|---|---|---|
| Pending | the cloud accepts the create (idempotent on the claim name) | InsufficientCapacity: fabric skipped for `unavailableTTL`, claim deleted, gang replanned. Quota: claim deleted. Transient: retried `launchRetries` times with backoff |
| Launched | a Node with the instance's provider ID exists; its labels, taints and owner reference are set and the startup taint removed, in one update | `registrationTTL`: replaced |
| Registered | Ready, no startup or not-ready taint, every GPU advertised | `initializationTTL`: replaced |
| Initialized | – | NotReady for `repairAfter`: replaced |
| Terminating | cordon, drain (evict, or delete if forced), delete instance, delete Node, remove finalizer | – |

## Disruption

Per pool, each pass, within `maxUnavailable` (drains in progress and deletions count):

1. Drains in progress continue; drained nodes are deleted.
2. Drift: empty drifted nodes are deleted; busy ones get a replacement in the same fabric
   (`maxSurge` at a time), then are cordoned and drained. Nodes with gang members wait
   (ADR 0004).
3. Empty initialized nodes are deleted after `consolidateAfter`, down to `minNodes`, unless the
   planner has pending work for them.
4. One underutilized node (at most half its GPUs used, only controller-owned single pods that fit
   on the pool's other nodes) is drained per pass, after `consolidateAfter`.

## Leader election and crash safety

All three loops and both controllers run on the leader only (a Lease in the provisioner's
namespace). The design keeps every step safe to repeat after a crash:

- Cloud creates are idempotent on the claim name.
- A claim deleted before its status recorded the instance leaves an instance GC deletes after
  `gcGrace`, confirming against the API server (not the cache) first.
- The provisioner waits for the cache to show the claims it created before planning again.

## Control-plane cost

At 20,000 nodes the controller's cost is API writes and cache memory, not planning.

- Managed fields are stripped from cached objects (`cache.TransformStripManagedFields`).
- Lists from the cache skip deep copies; nothing modifies cached objects (copies are made first).
- A node costs about five writes from the controller: claim create (finalizer included), three
  status writes (Launched, Registered, Initialized; the last two folded into one when GPUs are
  already there), and one guarded JSON patch to the Node for labels, taints and owner. Pool status is written at most every
  `poolStatusInterval`. `reports/SCALE.md` has the measured numbers.
- The client is limited to `kubeQPS`/`kubeBurst` (200/400): at that rate, writes, not CPU, bound
  how fast nodes come up.
