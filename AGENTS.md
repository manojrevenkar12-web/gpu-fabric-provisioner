# Working on this repository

Fast feedback:
- `go test -short ./...`: unit tests (planner, pod helpers, config, webhook, cloud adapter), seconds.
- `go test ./...` with `KUBEBUILDER_ASSETS` set (`setup-envtest use 1.33.0 -p path`): adds the
  integration tests against kube-apiserver and etcd, about three minutes.
- `go run ./cmd/drill && go run ./cmd/gate`: fault drills and the release gate, about fifteen minutes.
- `go run ./cmd/scale -nodes 1000`: control-plane scale run.
- `make generate` after any change in `api/` (deepcopy, CRDs, RBAC); CI fails if it is stale.

Invariants (each has a test or a drill check; keep it that way):
- A gang is released only when every member fits on initialized nodes of one fabric. Release is
  one pod update that narrows node affinity and removes the gate (`podutil.Release`).
- A gang never gets part of the capacity it needs: placement trials roll back (`plan.fitAll`).
- Capacity counts from the instance type for every claim, including ones still booting or not yet
  advertising GPUs. Never plan from `allocatable` (ADR 0003).
- `plan.Compute` is pure: no clock, no randomness, no API calls. Same snapshot, same plan.
- Cached objects are read without deep copies; copy before modifying anything from the cache.
- Every cloud call that creates is idempotent on the claim name; GC confirms against the API
  server (not the cache) before deleting an instance.
- Nothing voluntarily disrupts a node that runs a gang member, a do-not-disrupt pod or an
  uncontrolled pod. Evictions go through the Eviction API.
- Timings and client limits live in `config/controller.yaml`; pool limits on GPUNodePool objects;
  drill thresholds in `config/gate.yaml`. Not in code.
- The simulator (`internal/sim`, `internal/simenv`, `internal/drill`) is never imported by
  `cmd/manager` or `internal/controllers`.
