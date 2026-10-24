# gpu-fabric-provisioner

**A Kubernetes node provisioner for GPU clusters whose servers sit on InfiniBand fabrics.** It
buys servers for training gangs all or nothing, inside one fabric. It counts servers that are
still booting, or still loading their GPU drivers, as capacity. It starts a gang only when every
member can start. It replaces nodes for image upgrades without killing running training jobs or
breaking PodDisruptionBudgets.

Go 1.24, controller-runtime 0.21, Kubernetes 1.33 APIs. Two CRDs, `GPUNodePool` and `NodeClaim`,
with CEL validation. A mutating webhook and scheduling gates. Leader election, Prometheus metrics
and alerts with a runbook. Tested against a real kube-apiserver and etcd (envtest), never a fake
client.

## Why

Two problems show up as soon as GPU node groups autoscale:

- **Late GPUs make autoscalers overbuy.** A GPU server registers its Node before its driver and
  device plugin are up, so for a while it has 0 allocatable `nvidia.com/gpu`. An autoscaler that
  counts allocatable resources sees a node that cannot run the pending pod and buys another.
  [Nebius documents this](https://docs.nebius.com/kubernetes/node-groups/autoscaling) for its
  node groups with the NVIDIA operators installed ("the cluster autoscaler can create more nodes
  in the group than the workloads require"), and so does
  [Red Hat](https://access.redhat.com/solutions/6055181) for OpenShift.
- **Gangs need one fabric and all their members.** A distributed training job is useless until
  all N pods run, and its collectives need all N on one InfiniBand fabric (in Nebius terms,
  [one GPU cluster](https://docs.nebius.com/kubernetes/gpu/clusters)). A per-pod autoscaler with
  the default scheduler starts members as servers appear, in whatever fabric they appear in.
  Half-started gangs hold GPUs while they wait, and gangs split across fabrics run over the
  datacenter network.

## Results

### Fault drills

`go run ./cmd/drill`, gated by `go run ./cmd/gate` (`config/gate.yaml`, 81 checks, all passing;
CI runs both). Every run gets a fresh kube-apiserver and etcd, a simulated cloud, kubelets and
scheduler (`internal/sim`), and the same workload for this provisioner and for a **naive
baseline**: a Cluster-Autoscaler-style loop that scales up for unschedulable pods, counts upcoming
nodes by template and registered nodes by allocatable resources, and picks a fabric (node group)
at random. Both share the same NodeClaim lifecycle and consolidation, so only the scale-up
decision differs. Full report: [reports/DRILLS.md](reports/DRILLS.md).

Times are compressed: servers boot in 1.5–3 s and advertise their GPUs 2 s after registering;
jobs run 6–40 s. A gang split across fabrics is assumed to run 3× slower, and its GPU time is not
counted as useful. That is an assumption, stated in the report.

| Drill | What breaks | This provisioner | Naive baseline |
|---|---|---|---|
| `burst` | 51 servers of gangs (2–8 servers) and single pods within 2 s | 14/14 jobs in 22 s; 56 servers; 8,634 GPU-s paid, 52% useful; **0** split gangs; 11 GPU-s in half-started gangs | 14/14 jobs in 43 s; 76 servers; 17,113 GPU-s paid, 5% useful; **9** split gangs; 183 GPU-s in half-started gangs |
| `contention` | 50 servers of gangs for 32 hosts in fabrics of 12, 12 and 8 | 9/9 jobs in 26 s; 32 servers; 6,188 GPU-s paid, 56% useful; **0** split gangs; 31 GPU-s in half-started gangs | 9/9 jobs in 62 s; 32 servers; 13,265 GPU-s paid, 1% useful; **8** split gangs; 965 GPU-s in half-started gangs |
| `ice` | another tenant takes 30 of the largest fabric's free hosts; offerings stale for 10 s | 14/14 jobs in 20 s; 54 servers; 7,958 GPU-s paid, 56% useful; **0** split gangs; 11 GPU-s in half-started gangs | 14/14 jobs in 43 s; 60 servers; 14,078 GPU-s paid, 12% useful; **5** split gangs; 186 GPU-s in half-started gangs |
| `quota` | quota of 20 servers; queued gangs need 32 | 7/7 jobs in 27 s; 20 servers; 3,918 GPU-s paid, 57% useful; **0** split gangs; 8 GPU-s in half-started gangs | 7/7 jobs in 34 s; 20 servers; 5,134 GPU-s paid, 44% useful; **0** split gangs; 1,298 GPU-s in half-started gangs |
| `crash` | leader killed 3 times mid-burst (2 replicas, lease not released) | 14/14 jobs in 28 s; 55 servers; 10,216 GPU-s paid, 44% useful; **0** split gangs; 20 GPU-s in half-started gangs; 0 duplicate servers, 0 leaked | – |
| `flaky` | 15% of servers never boot, 10% never advertise GPUs, 5% of cloud calls fail | 14/14 jobs in 26 s; 70 servers; 9,750 GPU-s paid, 46% useful; **0** split gangs; 15 GPU-s in half-started gangs; 70 servers launched, at most 55 alive at once: stuck and GPU-less servers were replaced | – |
| `upgrade` | image change under 2 training gangs and a 6-replica service with PDB minAvailable 5 | 3/3 jobs; rollout done 26 s after the image change; **0** gang restarts; **0** PDB violations; 6 evictions; 12 servers at most for 11 in use; 0 nodes left on the old image | – |

In `quota`, the naive baseline finished here but deadlocked in one of four runs: partly started
gangs held the whole quota and no job completed in 180 s. Its 626 create calls were rejected by
the quota; this provisioner made none it could not use. In `crash`, `flaky` and `upgrade` the
baseline is not run: they test this provisioner's own recovery.

What the naive column is not: real Cluster Autoscaler has a workaround for late GPUs when the
cloud provider labels GPU nodes (it treats them as not ready until GPUs appear). The baseline is
the loop without it. That is the behaviour the Nebius page describes.

### Control plane at size

`go run ./cmd/scale -nodes 1000,5000,10000,20000`: one manager brings N nodes from nothing to
initialized through a real API server, with the production client limit (200 QPS, burst 400).
The simulated kubelets and cloud run in the same process on a 2-vCPU machine. Full numbers:
[reports/SCALE.md](reports/SCALE.md).

| Nodes | All initialized | Nodes per minute | Manager writes per node | Plan with all nodes cached (p50) | Gang released (members) | Process heap |
|---|---|---|---|---|---|---|
| 1,000 | 28 s | 2,124 | 5.13 | 5 ms | 0.7 s (50 pods) | 23 MiB |
| 5,000 | 169 s | 1,775 | 5.21 | 13 ms | 1.1 s (64 pods) | 78 MiB |
| 10,000 | 384 s | 1,562 | 5.15 | 61 ms | 1.6 s (64 pods) | 152 MiB |
| 20,000 | 881 s | 1,361 | 5.28 | 97 ms | 2.5 s (64 pods) | 323 MiB |

Writes per node stay at about 5.2 at every size, and duplicate servers at 0. The planner is not
the limit: `plan.Compute` takes about 70 ms for 20,000 nodes and 10,000 pending pods
(`go test -bench . ./internal/plan`). On this machine the 20,000-node run is bound by the
simulated kubelets sharing two vCPUs with the API server (the manager averaged 120 writes/s of its
200). On a real control plane, the client limit and the API server would be the bound, which is
why the work went into writes per node
([postmortem](docs/postmortems/2026-10-24-slow-node-bring-up-at-1000.md)).

### What testing found

- [Gangs were released onto nodes the scheduler would not use yet](docs/postmortems/2026-10-24-gang-released-onto-tainted-nodes.md).
  "Ready" is not "schedulable": the API server taints new Nodes `not-ready` at creation. Found by
  the first test against a real API server.
- [Bringing up 1,000 nodes took five minutes and 6.7 writes per node](docs/postmortems/2026-10-24-slow-node-bring-up-at-1000.md).
  Write conflicts went through controller-runtime's shared failure backoff. Now it takes 28 s
  and 5.1 writes per node.

## How it works

```
 Job / JobSet ─▶ webhook gates gang pods ─▶ gated pods ──gate removed──▶ kube-scheduler (unchanged)
                                               │ ▲
                              batched trigger  │ │ one update: narrow node affinity to the fabric + remove gate
                                               ▼ │
 leader ─ Provisioner: cache snapshot ─▶ plan.Compute (pure) ─▶ NodeClaims · placements · releases
        ─ Disruptor:   empty ─▶ delete · drifted ─▶ surge, drain via Eviction API, delete
        ─ GC:          instances without a claim · claims without an instance · orphan Nodes
 NodeClaim: Pending ─create (idempotent)─▶ Launched ─Node─▶ Registered ─Ready + all GPUs─▶ Initialized
```

- **Gate, then narrow** ([ADR 0001](docs/adr/0001-gate-then-narrow-affinity.md)). A webhook adds
  the `gpufabric.dev/placement` scheduling gate to gang pods. While a pod is gated, the API server
  lets a controller add node-affinity terms that narrow it
  ([KEP-3838](https://www.kubernetes.dev/resources/keps/3838/), stable since 1.30). When every
  member fits on initialized nodes of one fabric, each pod gets `gpufabric.dev/fabric In [f]`
  and loses the gate in one update. No custom scheduler. Gangs are read from `gpufabric.dev/gang`
  or Kueue's [plain pod group](https://kueue.sigs.k8s.io/docs/tasks/run_plain_pods) labels, and
  Kueue's own gate is respected.
- **Plan from a snapshot** ([ADR 0002](docs/adr/0002-plan-from-a-snapshot.md)). Events only wake
  the provisioner. Each batch plans from the whole cache with a pure, deterministic function. A
  gang is tried in every allowed fabric and rolled back if it does not fit, so it never gets part
  of its capacity. Gangs go where they need the fewest new servers, then to the fabric with the
  fewest hosts left, keeping large fabrics for large gangs. Single pods fill fragmented fabrics.
- **Capacity from the instance type** ([ADR 0003](docs/adr/0003-count-capacity-by-instance-type.md)).
  A claim counts as a full server from the moment it exists, booting or not. Claims the cloud
  has not seen yet also reduce the fabric's offerings. A node is initialized only when it is
  Ready, without startup or not-ready taints, and advertises every GPU.
- **Upgrades wait for gangs** ([ADR 0004](docs/adr/0004-drift-waits-for-gangs.md)). A template
  hash marks drift. Busy nodes get a replacement in the same fabric first (`maxSurge`), then are
  drained through the Eviction API. Nodes running gang members wait until the gang is done.
- **Failures:** InsufficientCapacity skips the fabric for `unavailableTTL` and replans the gang
  elsewhere while it is still gated. Quota errors invalidate the cached quota. Servers that never
  register or never advertise their GPUs are replaced after their TTLs, and NotReady nodes after
  `repairAfter`. Creates are idempotent on the claim name. GC removes what a crash leaves behind,
  after checking the API server, not the cache.

More: [ARCHITECTURE](docs/ARCHITECTURE.md) · [RUNBOOK](docs/RUNBOOK.md) (one section per alert in
[deploy/base/monitoring.yaml](deploy/base/monitoring.yaml)) · [kustomize base](deploy/base) ·
[example pool](deploy/examples/pool.yaml) and [gang job](deploy/examples/gang-job.yaml).

## API

```yaml
apiVersion: gpufabric.dev/v1alpha1
kind: GPUNodePool
metadata: {name: h100-training}
spec:
  instanceType: gpu-h100-sxm.8gpu
  fabrics: [fab-ams-1, fab-ams-2]        # empty: every fabric that offers the type
  image: ubuntu24-cuda12-2026.10         # changing it rolls every node
  taints: [{key: nvidia.com/gpu, effect: NoSchedule}]
  limits: {minNodes: 2, maxNodes: 512}   # CEL: minNodes <= maxNodes
  disruption: {consolidateAfter: 10m, maxUnavailable: 5%, maxSurge: 4, gangPolicy: WaitForCompletion}
```

```
$ kubectl get nc
NAME                POOL            FABRIC      PHASE         NODE
h100-training-x7k2  h100-training   fab-ams-1   Initialized   gpu-i-000412
h100-training-9qpl  h100-training   fab-ams-1   Registered    gpu-i-000413   # GPUs not advertised yet
```

Metrics include `gpufabric_gangs{state}`, `gpufabric_gang_release_latency_seconds`,
`gpufabric_nodeclaims{pool,phase}`, `gpufabric_pool_gpus{state}`, `gpufabric_cloud_calls_total{op,code}`,
`gpufabric_plan_duration_seconds` and `gpufabric_disruption_blocked_total{reason}`.

## Run it

```bash
go test -short ./...                                           # unit tests, seconds
export KUBEBUILDER_ASSETS=$(setup-envtest use 1.33.0 -p path)  # kube-apiserver + etcd
go test ./...                                                  # + integration tests on a real API server
go run ./cmd/drill && go run ./cmd/gate                        # fault drills vs the naive baseline, ~11 min
go run ./cmd/scale -nodes 1000                                 # control-plane scale run
```

The manager talks to a cloud through `cloud.Provider`. `internal/cloud/httpcloud` speaks a small
JSON API, and `cmd/simcloud` serves the simulated cloud over it:

```bash
go run ./cmd/simcloud -config config/simcloud.yaml &
go run ./cmd/manager --cloud-url http://localhost:8090 --config config/controller.yaml --enable-webhook=false
```

## Limits

- **The cloud is simulated.** Fabrics, capacity, quota, boot times and failures are scenario
  settings. A real cloud needs a `cloud.Provider` implementation or an adapter behind the HTTP
  API. None for a real cloud is in this repository.
- **The scheduler in the drills is simulated** (`internal/sim/scheduler.go`). It filters like
  kube-scheduler and scores MostAllocated. The real one with default scoring spreads pods
  (LeastAllocated), which leaves more partly used nodes for single pods.
- **No reservations.** Between a gang's release and its binding, or while its last servers boot,
  single pods may take capacity the planner counted for the gang. In the burst drill this cost
  two extra servers (56 bought for 54 servers of work).
- **Gangs start a little later when nothing goes wrong.** Waiting until every member's server is
  initialized costs time: in the burst drill the median gang started after 7.1 s, against 6.2 s
  for the baseline, whose gangs start member by member (and, in 9 cases, across fabrics).
- **Times are compressed** in drills (seconds, not minutes). The scale runs use the production
  client limits and share two vCPUs with the simulated kubelets.
- **Not built:** a maintenance window or deadline for restarting gangs during upgrades,
  topology inside a fabric (spine/leaf blocks), local-disk scheduling, and cost-aware choice
  between instance types.
- **Who wrote the tests:** the scenarios, the baseline and the provisioner were written by the
  same author. The baseline is described above so it can be judged.
