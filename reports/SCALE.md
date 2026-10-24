# Scale results

Generated from `reports/scale.json` (`go run ./cmd/scale -nodes 1000,5000,10000,20000`). One manager,
production client limits (200 QPS, burst 400), against kube-apiserver and etcd from envtest 1.33. A pool with
`minNodes: N` is created and the run measures how long N servers take from nothing to initialized:
created in the simulated cloud (0.5–1.5 s boot), registered by the simulated kubelet, labelled and untainted
by the provisioner, and advertising 8 GPUs (0.5 s after registration). Then it times a plan with every
node in the cache, and a gang that fits on existing nodes from creation to release. Everything (API server,
etcd, simulated kubelets and cloud, manager) shares one 2-vCPU machine.

| Nodes | 10% initialized | 50% | All | Nodes per minute | Writes per node | Plan p50 / max | Gang released | Process heap |
|---|---|---|---|---|---|---|---|---|
| 1,000 | 11 s | 23 s | 28 s | 2,124 | 5.13 | 5 / 6 ms | 0.7 s (50 pods) | 23 MiB |
| 5,000 | 34 s | 136 s | 169 s | 1,775 | 5.21 | 13 / 33 ms | 1.1 s (64 pods) | 78 MiB |
| 10,000 | 79 s | 311 s | 384 s | 1,562 | 5.15 | 61 / 303 ms | 1.6 s (64 pods) | 152 MiB |
| 20,000 | 149 s | 707 s | 881 s | 1,361 | 5.28 | 97 / 294 ms | 2.5 s (64 pods) | 323 MiB |

Manager writes by kind:

| Nodes | Claim creates | Claim status updates | Node patches | Pool status updates | Cloud creates | Duplicate servers |
|---|---|---|---|---|---|---|
| 1,000 | 1,000 | 3,123 | 1,000 | 6 | 1,000 | 0 |
| 5,000 | 5,000 | 16,039 | 5,001 | 34 | 5,000 | 0 |
| 10,000 | 10,000 | 31,433 | 10,005 | 76 | 10,000 | 0 |
| 20,000 | 20,000 | 65,449 | 20,027 | 169 | 20,000 | 0 |

## Reading it

- **Writes per node stay flat** (5.1–5.3): one claim create (finalizer included), three claim status writes (Launched, Registered, Initialized), one node patch, and pool status at most every 5 s. Nothing grows with cluster size.
- **At 20,000 nodes the run is bound by the machine, not by the client limit.** The manager averaged 120 writes/s against a 200/s limit; the simulated kubelets registered nodes as fast as two vCPUs allowed (the first 4,600 Nodes took four minutes). On a real control plane, with kubelets elsewhere, the 200 QPS client limit would bound it at roughly 20,000 × 5.3 / 200 ≈ 9 minutes; raising `kubeQPS` is the lever, and the API server's capacity is the real limit.
- **Planning is not the bottleneck:** a full plan with 20,000 nodes in the cache takes about 100 ms, most of it reading the cache; `plan.Compute` alone is about 70 ms with 10,000 pending pods (`go test -bench . ./internal/plan`).
- **Heap** is the whole process, including the simulator's own cache of every Node, Pod and NodeClaim, so the manager's share is below it. Cached objects carry no managed fields (`cache.TransformStripManagedFields`).
- **No duplicate servers:** one cloud create per claim at every size; the provisioner waits for its own claims to appear in the cache before planning again.

History: the first 1,000-node run took 324 s and 6.7 writes per node; see
[the postmortem](../docs/postmortems/2026-10-24-slow-node-bring-up-at-1000.md).
