# Gangs were released onto nodes the scheduler would not use yet

Found by: `TestGangGetsOneFabricAndStartsTogether`, the first integration test against a real
kube-apiserver. Severity in production: low (seconds of delay per gang), but it broke the
invariant the design rests on: "a gang is released only when every member can start".

## What happened

The test created a 4-member gang. The provisioner bought 4 servers in one fabric, saw all 4
claims reach `Initialized`, and removed the gate. All 4 pods stayed `Pending` with
`PodScheduled=False, Unschedulable` for the rest of the test.

Every Node had `node.kubernetes.io/not-ready:NoSchedule`. The API server's
`TaintNodesByCondition` admission plugin adds that taint to every Node at creation; the node
lifecycle controller in kube-controller-manager removes it once the Node is Ready. envtest runs
no controller manager, so the taint stayed. In a real cluster it goes within seconds, but the
gate came off before it did.

## Why

`NodeInitialized` checked the Ready condition, the provisioner's own startup taint and the GPU
count. It did not check the taints the scheduler itself would reject the pods for. "Ready" and
"schedulable" are different facts, and the provisioner released gangs on the first.

## Fix

- `NodeInitialized` now also requires that `node.kubernetes.io/not-ready` and
  `node.kubernetes.io/unreachable` are gone.
- The simulated kubelet does what the node lifecycle controller does (removes the not-ready
  taint once the Node is Ready), so the tests run against the same sequence a cluster produces.

## What it says about testing

The planner's unit tests could not have caught this: they take "initialized" as an input. It
needed the real API server's admission chain. That is why every lifecycle test in
`internal/controllers` runs against kube-apiserver and etcd rather than a fake client.
