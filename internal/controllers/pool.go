package controllers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/metrics"
)

// PoolReconciler keeps GPUNodePool status and per-pool metrics current. Every claim event of a
// pool triggers it, so at thousands of nodes it would rewrite the pool's status many times a
// second; it writes at most once per MinInterval instead and catches up afterwards.
type PoolReconciler struct {
	Client      client.Client
	Cloud       *CloudState
	MinInterval time.Duration

	mu        sync.Mutex
	lastWrite map[string]time.Time
}

// Reconcile recomputes one pool's status from its claims.
func (r *PoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pool := &v1alpha1.GPUNodePool{}
	if err := r.Client.Get(ctx, req.NamespacedName, pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	var claims v1alpha1.NodeClaimList
	if err := r.Client.List(ctx, &claims, client.MatchingLabels{v1alpha1.LabelPool: pool.Name}, client.UnsafeDisableDeepCopy); err != nil {
		return ctrl.Result{}, err
	}
	hash := pool.TemplateHash()
	st := pool.Status.DeepCopy()
	st.ObservedGeneration = pool.Generation
	st.TemplateHash = hash
	st.Nodes, st.Initialized, st.InFlight, st.Drifted, st.GPUs = 0, 0, 0, 0, 0
	phases := map[v1alpha1.NodeClaimPhase]int{}
	var inflightGPUs, readyGPUs int64
	gpus := int64(0)
	if it, ok := r.Cloud.Type(ctx, pool.Spec.InstanceType); ok {
		gpus = int64(it.GPUs)
	}
	for i := range claims.Items {
		c := &claims.Items[i]
		st.Nodes++
		st.GPUs += gpus
		phase := c.Status.Phase
		if phase == "" {
			phase = v1alpha1.PhasePending
		}
		phases[phase]++
		switch {
		case phase == v1alpha1.PhaseInitialized:
			st.Initialized++
			readyGPUs += gpus
		case phase != v1alpha1.PhaseTerminating:
			st.InFlight++
			inflightGPUs += gpus
		}
		if c.Spec.TemplateHash != hash {
			st.Drifted++
		}
	}
	for _, ph := range []v1alpha1.NodeClaimPhase{v1alpha1.PhasePending, v1alpha1.PhaseLaunched, v1alpha1.PhaseRegistered, v1alpha1.PhaseInitialized, v1alpha1.PhaseTerminating} {
		metrics.Claims.WithLabelValues(pool.Name, string(ph)).Set(float64(phases[ph]))
	}
	metrics.PoolGPUs.WithLabelValues(pool.Name, "initialized").Set(float64(readyGPUs))
	metrics.PoolGPUs.WithLabelValues(pool.Name, "inflight").Set(float64(inflightGPUs))
	_, known := r.Cloud.Type(ctx, pool.Spec.InstanceType)
	cond := metav1.Condition{Type: v1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: "InstanceTypeOffered",
		Message: fmt.Sprintf("the cloud offers %s", pool.Spec.InstanceType), ObservedGeneration: pool.Generation}
	if !known {
		cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, "UnknownInstanceType", fmt.Sprintf("the cloud does not offer %s", pool.Spec.InstanceType)
	}
	meta.SetStatusCondition(&st.Conditions, cond)
	if poolStatusEqual(&pool.Status, st) {
		return ctrl.Result{}, nil
	}
	r.mu.Lock()
	if r.lastWrite == nil {
		r.lastWrite = map[string]time.Time{}
	}
	if wait := r.MinInterval - time.Since(r.lastWrite[pool.Name]); wait > 0 {
		r.mu.Unlock()
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	r.lastWrite[pool.Name] = time.Now()
	r.mu.Unlock()
	p := pool.DeepCopy()
	p.Status = *st
	return ctrl.Result{}, client.IgnoreNotFound(r.Client.Status().Update(ctx, p))
}

func poolStatusEqual(a, b *v1alpha1.GPUNodePoolStatus) bool {
	if a.ObservedGeneration != b.ObservedGeneration || a.Nodes != b.Nodes || a.Initialized != b.Initialized ||
		a.InFlight != b.InFlight || a.Drifted != b.Drifted || a.GPUs != b.GPUs || a.TemplateHash != b.TemplateHash ||
		len(a.Conditions) != len(b.Conditions) {
		return false
	}
	for i := range a.Conditions {
		if a.Conditions[i].Type != b.Conditions[i].Type || a.Conditions[i].Status != b.Conditions[i].Status ||
			a.Conditions[i].Reason != b.Conditions[i].Reason || a.Conditions[i].Message != b.Conditions[i].Message {
			return false
		}
	}
	return true
}

// poolForClaim maps a claim event to its pool.
func poolForClaim(_ context.Context, o client.Object) []ctrl.Request {
	c, ok := o.(*v1alpha1.NodeClaim)
	if !ok {
		return nil
	}
	return []ctrl.Request{{NamespacedName: types.NamespacedName{Name: c.Spec.Pool}}}
}
