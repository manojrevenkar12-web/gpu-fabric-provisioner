# Runbook

One section per alert in `deploy/base/monitoring.yaml`. Commands assume `kubectl` access to the
cluster; `gnp` and `nc` are the short names of GPUNodePool and NodeClaim.

First look, for any alert:

```bash
kubectl get gnp                                   # nodes, initialized, in flight, drifted per pool
kubectl get nc -o wide                            # every server: pool, fabric, phase, node, reason
kubectl get events -A --field-selector reason=WaitingForCapacity --sort-by=.lastTimestamp | tail
kubectl -n gpu-fabric-provisioner logs deploy/gpu-fabric-provisioner | grep -v '"level":1'
```

## GangsWaitingLong

Gangs have been waiting for capacity for 30 minutes. A waiting gang buys nothing: it waits until
one fabric can hold all of it.

1. Read why: the `WaitingForCapacity` event on the gang's first pod says, for example, "no single
   fabric can hold all 16 pending members; closest is fab-ams-2 with room for 12 (fab-ams-2 has no
   free gpu-h100-sxm.8gpu hosts)". The reason names the limiting factor: free hosts, quota, or the
   pool's `maxNodes`.
2. Free hosts: the gang is larger than any fabric's free capacity. Either wait, add a fabric to
   the pool (`spec.fabrics`), or ask the job owner to run smaller.
3. Quota: `kubectl get events` shows QuotaExceeded on deleted claims; raise the project quota.
4. `maxNodes`: raise it on the pool, or stop other work in the pool.
5. "waiting for members: 12 of 16 exist": the job controller has not created all pods (check the
   Job / JobSet); nothing to do on the provisioner side.

## LaunchFailuresHigh

Server creations fail: `insufficient-capacity`, `quota-exceeded` or `launch-failed` (transient
errors past `launchRetries`).

1. `kubectl get events --field-selector involvedObject.kind=NodeClaim` shows the cloud's message.
2. InsufficientCapacity: the fabric is skipped for `unavailableTTL` and gangs are planned
   elsewhere; nothing breaks, but gangs may wait. If one fabric keeps failing while its offerings
   show free hosts, the offerings are stale: report to the cloud's capacity team.
3. Persistent `launch-failed`: check `gpufabric_cloud_calls_total{op="create"}` by code and the
   cloud adapter's logs.

## NodesNotInitializing

Servers boot but are replaced by `registration-timeout` (no Node appeared) or
`initialization-timeout` (Node never advertised all GPUs). Every replacement costs a boot.

1. `kubectl get nc` for claims in `Registered` long: `kubectl describe node <node>` and look at
   `Allocatable` for `nvidia.com/gpu` and at the GPU device plugin / driver pods on that node.
2. A driver or image problem affects every node of an image. Roll the pool back
   (`kubectl edit gnp <pool>`, `spec.image`); the rollout follows ADR 0004.
3. Registration timeouts with nodes never appearing: bootstrap (cloud-init, kubelet join token).

## CloudAPIErrors

More than 20% of cloud calls fail with Unavailable. Creates are retried with backoff and claims
are given up after `launchRetries`; nothing is lost, but provisioning slows.

1. `gpufabric_cloud_call_duration_seconds` and the adapter's logs.
2. If the cloud API is down, nothing to do here; the provisioner retries. Do not restart the
   provisioner: a restart replans from the same state and adds load.

## LeakedInstancesCollected

GC deleted a cloud server that no NodeClaim owned. This happens after a crash between the cloud
accepting a create and the claim recording it, or when a claim was deleted by hand.

1. One or two after a crash or failover: expected (the crash drill exercises it).
2. A steady rate means claims are being deleted outside the provisioner, or two installations
   share a cloud project. Check `kubectl get events` for NodeClaim deletions and the audit log.

## PlanSlow

A plan (snapshot plus `plan.Compute`) takes more than 5 s at p99. At 20,000 nodes it should take
well under a second (`reports/SCALE.md`).

1. `kubectl top pod -n gpu-fabric-provisioner`: CPU throttling makes plans slow. Raise the
   CPU request.
2. Many pending pods (tens of thousands) with distinct constraints make matching slower; check
   `gpufabric_pending_pods`.

## DisruptionBlocked

Consolidation or an image rollout has been blocked for hours by `budget` or `do-not-disrupt`.

1. `kubectl get nc -l gpufabric.dev/pool=<pool>` and look at the `Drifted` and `Disrupting`
   conditions.
2. `budget`: `maxUnavailable` is fully used, usually by drains that PDBs block. Find the PDB:
   `kubectl get pdb -A` with `ALLOWED DISRUPTIONS 0`.
3. Gangs (`gang-running`) are expected to block a rollout until they finish; that is not this
   alert.

## NoLeader

No replica is planning. Pending GPU pods wait and nothing is bought or removed.

1. `kubectl -n gpu-fabric-provisioner get lease gpu-fabric-provisioner -o yaml`: holder and renew time.
2. `kubectl -n gpu-fabric-provisioner get pods`: both replicas crash-looping points at
   configuration (`--config`, `--cloud-url`) or RBAC.
3. Running a single replica without leader election is safe for a short time
   (`--leader-elect=false`); never run two without it.
