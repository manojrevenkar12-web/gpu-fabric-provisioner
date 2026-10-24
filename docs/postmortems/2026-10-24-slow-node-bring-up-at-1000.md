# Bringing up 1,000 nodes took five minutes and 6.7 API writes per node

Found by: the first run of `cmd/scale` at 1,000 nodes. Severity in production: high for the
20,000-node target. Write volume and retry behaviour decide how fast a cluster that size can
grow, and how much load the provisioner puts on the API server while it grows.

## What happened

| Run | Time until 1,000 nodes were initialized | Writes per node (manager) | Node writes per node |
|---|---|---|---|
| First run | 324 s; 10% initialized only at 316 s | 6.67 | 1.76 |
| Simulated kubelet fixed, conflicts requeued quickly | 30 s | 6.23 | 1.53 |
| Node registration as a guarded JSON patch, emptiness tracked in memory | 27 s | 5.16 | 1.00 |

Almost nothing was initialized for five minutes, then everything was initialized at once.

## Why

Two causes. One was in the benchmark, one was in the controller.

1. **The simulated kubelet.** After creating a Node, it read the Node back from its cache, which
   did not have it yet. It treated the miss as an error and abandoned the rest of its loop,
   including removing the `node.kubernetes.io/not-ready` taint. It got through about two nodes
   a second. That is a harness bug, but it hid the second cause.
2. **Conflicts went through the failure backoff.** Registering a node (labels, owner reference,
   taint removal) was an `Update` with a resourceVersion. The kubelet writes the Node's status at
   the same time, so about half of those updates conflicted. A conflict was returned as an
   error. controller-runtime then requeued the claim through its default rate limiter: an
   exponential backoff per item, plus a 10-per-second bucket shared by every claim. Under a burst
   of conflicts, retries queued behind the bucket.

Separately, the disruptor wrote an `Empty` condition on every initialized node and wrote it again
when the first pod arrived: two status writes per node for a timer.

## Fix

- The kubelet uses the object returned by `Create` and keeps going when a single node fails.
- `ClaimReconciler` turns a conflict into a short, jittered `RequeueAfter` (100–300 ms) instead of
  an error, so it never enters the shared backoff.
- Node registration is one JSON patch. It adds labels and the owner reference, and removes the
  startup taint behind a `test` op on the taint's key. A concurrent status write cannot make it
  conflict. A concurrent taint change makes the `test` fail instead of corrupting the list.
- Emptiness for `consolidateAfter` is tracked in the disruptor's memory. After a leader change
  the timers restart, which only delays consolidation by `consolidateAfter`.

## What is left

About 3 claim status writes per node: Launched, Registered and Initialized. Registered and
Initialized are folded into one write when the GPUs are already advertised at registration.
Folding them always would hide a node stuck without GPUs from `kubectl get nc`. That is worth
one write.
