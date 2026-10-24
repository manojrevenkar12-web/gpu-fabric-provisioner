package sim

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PDBController computes PodDisruptionBudget status the way kube-controller-manager's
// disruption controller does (healthy pods minus the minimum), so the API server's eviction
// checks work in an environment without a controller manager.
type PDBController struct {
	Client client.Client
	Tick   time.Duration
}

// Start runs until ctx ends.
func (c *PDBController) Start(ctx context.Context) error {
	t := time.NewTicker(c.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			c.step(ctx)
		}
	}
}

func (c *PDBController) step(ctx context.Context) {
	var pdbs policyv1.PodDisruptionBudgetList
	if err := c.Client.List(ctx, &pdbs); err != nil {
		return
	}
	for i := range pdbs.Items {
		pdb := &pdbs.Items[i]
		sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			continue
		}
		healthy, expected := Healthy(ctx, c.Client, pdb.Namespace, sel)
		// Like the real disruption controller, pods the API server has just evicted (listed in
		// DisruptedPods) count as unhealthy until their deletion shows up, so a stale cache
		// cannot hand out the same disruption twice.
		disrupted := map[string]metav1.Time{}
		for name, at := range pdb.Status.DisruptedPods {
			pod := &corev1.Pod{}
			err := c.Client.Get(ctx, client.ObjectKey{Namespace: pdb.Namespace, Name: name}, pod)
			if err == nil && pod.DeletionTimestamp == nil && time.Since(at.Time) < 2*time.Minute {
				disrupted[name] = at
				healthy--
			}
		}
		desired := int32(0)
		if pdb.Spec.MinAvailable != nil {
			v, _ := intstr.GetScaledValueFromIntOrPercent(pdb.Spec.MinAvailable, int(expected), true)
			desired = int32(v)
		}
		allowed := healthy - desired
		if allowed < 0 {
			allowed = 0
		}
		st := policyv1.PodDisruptionBudgetStatus{
			ObservedGeneration: pdb.Generation, DisruptionsAllowed: allowed, CurrentHealthy: healthy,
			DesiredHealthy: desired, ExpectedPods: expected, DisruptedPods: disrupted,
		}
		if pdb.Status.ObservedGeneration == st.ObservedGeneration && pdb.Status.DisruptionsAllowed == st.DisruptionsAllowed &&
			pdb.Status.CurrentHealthy == st.CurrentHealthy && pdb.Status.ExpectedPods == st.ExpectedPods &&
			len(pdb.Status.DisruptedPods) == len(disrupted) {
			continue
		}
		p := pdb.DeepCopy()
		p.Status = st
		_ = c.Client.Status().Update(ctx, p)
	}
}

// Healthy counts running, ready, not-terminating pods matching a selector, and all matching pods.
func Healthy(ctx context.Context, c client.Client, ns string, sel labels.Selector) (healthy, expected int32) {
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(ns), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return 0, 0
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		expected++
		if p.DeletionTimestamp == nil && p.Status.Phase == corev1.PodRunning {
			healthy++
		}
	}
	return healthy, expected
}
