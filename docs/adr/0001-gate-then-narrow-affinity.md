# ADR 0001: Gangs are held by a scheduling gate and pinned to a fabric by narrowing node affinity

Status: accepted

## Context

A distributed training job is a gang: N pods that are useless unless all N run, and that must
run on servers attached to one InfiniBand fabric (in Nebius terms, one GPU cluster). The default
scheduler places pods one at a time. Without help, it starts the first members of a gang on the
first servers that appear, in whatever fabric they are in, and those members hold their GPUs
while they wait for the rest.

Options:

1. A custom scheduler or a coscheduling scheduler plugin. It solves the gang problem but makes
   the provisioner depend on how a cluster's scheduler is configured, and a managed Kubernetes
   service does not want to replace kube-scheduler for every customer.
2. Reservations by taint: taint new servers for one gang and add a matching toleration to its
   pods (tolerations can be added to existing pods). It holds capacity for the gang, but the
   gang's pods can still start one by one as servers arrive, the taints must be cleaned up on
   every node when the gang finishes or shrinks, and tainted capacity is useless to anyone else.
3. Scheduling gates (KEP-3521) plus mutable scheduling directives for gated pods (KEP-3838,
   stable since Kubernetes 1.30): while a pod has a gate, the API server lets a controller add
   required node-affinity terms, but only ones that narrow the existing constraints.

## Decision

Option 3. A mutating webhook adds the `gpufabric.dev/placement` gate to gang pods that request
GPUs. The provisioner chooses a fabric for the whole gang and records it as an annotation (a
decision it may still change). When every member fits on initialized nodes of that fabric, it
updates each pod once: it adds `gpufabric.dev/fabric In [<fabric>]` to every node-affinity term
and removes the gate in the same update. The default scheduler then places the members as
usual, and they all fit.

## Consequences

- No scheduler changes. Kueue-managed pods keep working: Kueue's own gate is left alone, and a
  pod is only planned once foreign gates are gone (`podutil.ForeignGated`).
- The narrowing is one-way. Once released, a gang cannot move to another fabric; the planner
  treats released or bound members as fixing the gang's fabric.
- If the webhook is down (`failurePolicy: Ignore`), pods are created ungated and get today's
  behaviour, not an outage.
- `TestReleaseIsAcceptedOnlyForGatedPods` checks against a real API server that the update is
  accepted for gated pods and rejected for others.
- Between release and binding, the scheduler may place other pods on the capacity the gang was
  counted against. The gang then waits for one more plan; the provisioner does not reserve.
