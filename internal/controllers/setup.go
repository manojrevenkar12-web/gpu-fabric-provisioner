package controllers

import (
	"context"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/config"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/podutil"
)

// Options configure Setup.
type Options struct {
	Settings config.Settings
	Cloud    cloud.Provider
	// Webhook registers the pod gating webhook on the manager's webhook server.
	Webhook bool
	// NoProvisioner leaves out the provisioner (the drills run the naive baseline instead).
	NoProvisioner bool
}

// Components are the parts Setup wired, for tests and drills.
type Components struct {
	Provisioner *Provisioner
	Disruptor   *Disruptor
	GC          *GC
	Claims      *ClaimReconciler
	Cloud       *CloudState
}

// Setup registers indexes, controllers and runnables with a manager.
func Setup(ctx context.Context, mgr ctrl.Manager, o Options) (*Components, error) {
	s := o.Settings
	cs := NewCloudState(Instrument(o.Cloud), s.OfferingsRefresh.Duration, s.UnavailableTTL.Duration)
	idx := mgr.GetFieldIndexer()
	if err := idx.IndexField(ctx, &corev1.Pod{}, IndexPodNode, func(o client.Object) []string {
		if n := o.(*corev1.Pod).Spec.NodeName; n != "" {
			return []string{n}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := idx.IndexField(ctx, &corev1.Node{}, IndexNodeProviderID, func(o client.Object) []string {
		if p := o.(*corev1.Node).Spec.ProviderID; p != "" {
			return []string{p}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := idx.IndexField(ctx, &v1alpha1.NodeClaim{}, IndexClaimProvider, func(o client.Object) []string {
		if p := o.(*v1alpha1.NodeClaim).Status.ProviderID; p != "" {
			return []string{p}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	c := mgr.GetClient()
	prov := &Provisioner{Client: c, Reader: mgr.GetCache(), Cloud: cs, Settings: s, Recorder: mgr.GetEventRecorderFor("gpu-fabric-provisioner")}
	claims := &ClaimReconciler{Client: c, Cloud: cs, Settings: s, Recorder: mgr.GetEventRecorderFor("gpu-fabric-provisioner"), OnChange: prov.Trigger}
	if err := ctrl.NewControllerManagedBy(mgr).Named("nodeclaim").
		For(&v1alpha1.NodeClaim{}).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(claimForNode(c))).
		WithOptions(controller.Options{MaxConcurrentReconciles: s.ClaimWorkers}).
		Complete(claims); err != nil {
		return nil, err
	}
	if err := ctrl.NewControllerManagedBy(mgr).Named("gpunodepool").
		For(&v1alpha1.GPUNodePool{}).
		Watches(&v1alpha1.NodeClaim{}, handler.EnqueueRequestsFromMapFunc(poolForClaim)).
		Complete(&PoolReconciler{Client: c, Cloud: cs, MinInterval: s.PoolStatusInterval.Duration}); err != nil {
		return nil, err
	}
	if err := triggers(ctx, mgr, prov); err != nil {
		return nil, err
	}
	dis := &Disruptor{Client: c, Reader: mgr.GetCache(), Cloud: cs, Settings: s, Recorder: mgr.GetEventRecorderFor("gpu-fabric-provisioner")}
	gc := &GC{Client: c, APIReader: mgr.GetAPIReader(), Cloud: cs, Settings: s}
	runnables := []manager.Runnable{dis, gc}
	if !o.NoProvisioner {
		runnables = append(runnables, prov)
	}
	for _, r := range runnables {
		if err := mgr.Add(r); err != nil {
			return nil, err
		}
	}
	if o.Webhook {
		mgr.GetWebhookServer().Register("/mutate-pods", &webhook.Admission{Handler: &PodGater{Decoder: admission.NewDecoder(mgr.GetScheme())}})
	}
	return &Components{Provisioner: prov, Disruptor: dis, GC: gc, Claims: claims, Cloud: cs}, nil
}

func claimForNode(c client.Client) handler.MapFunc {
	return func(ctx context.Context, o client.Object) []reconcile.Request {
		node := o.(*corev1.Node)
		if name := node.Labels[v1alpha1.LabelNodeClaim]; name != "" {
			return []reconcile.Request{{NamespacedName: client.ObjectKey{Name: name}}}
		}
		if node.Spec.ProviderID == "" {
			return nil
		}
		var claims v1alpha1.NodeClaimList
		if err := c.List(ctx, &claims, client.MatchingFields{IndexClaimProvider: node.Spec.ProviderID}); err != nil {
			return nil
		}
		out := make([]reconcile.Request, 0, len(claims.Items))
		for i := range claims.Items {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKey{Name: claims.Items[i].Name}})
		}
		return out
	}
}

// triggers wakes the provisioner on events that can change a plan. The provisioner batches them.
func triggers(ctx context.Context, mgr ctrl.Manager, p *Provisioner) error {
	podInf, err := mgr.GetCache().GetInformer(ctx, &corev1.Pod{})
	if err != nil {
		return err
	}
	gpuPod := func(o any) bool {
		pod, ok := o.(*corev1.Pod)
		if !ok {
			if t, ok := o.(toolscache.DeletedFinalStateUnknown); ok {
				pod, _ = t.Obj.(*corev1.Pod)
			}
		}
		return pod != nil && podutil.WantsGPU(pod)
	}
	if _, err := podInf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(o any) {
			if gpuPod(o) {
				p.Trigger()
			}
		},
		UpdateFunc: func(oldO, newO any) {
			a, b := oldO.(*corev1.Pod), newO.(*corev1.Pod)
			if podutil.WantsGPU(b) && (a.Spec.NodeName != b.Spec.NodeName || a.Status.Phase != b.Status.Phase ||
				len(a.Spec.SchedulingGates) != len(b.Spec.SchedulingGates) || (a.DeletionTimestamp == nil) != (b.DeletionTimestamp == nil)) {
				p.Trigger()
			}
		},
		DeleteFunc: func(o any) {
			if gpuPod(o) {
				p.Trigger()
			}
		},
	}); err != nil {
		return err
	}
	nodeInf, err := mgr.GetCache().GetInformer(ctx, &corev1.Node{})
	if err != nil {
		return err
	}
	if _, err := nodeInf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(any) { p.Trigger() },
		UpdateFunc: func(oldO, newO any) {
			a, b := oldO.(*corev1.Node), newO.(*corev1.Node)
			ra, rb := nodeCondition(a, corev1.NodeReady), nodeCondition(b, corev1.NodeReady)
			readyChanged := (ra == nil) != (rb == nil) || (ra != nil && rb != nil && ra.Status != rb.Status)
			ga, gb := a.Status.Allocatable[v1alpha1.ResourceGPU], b.Status.Allocatable[v1alpha1.ResourceGPU]
			if readyChanged || ga.Cmp(gb) != 0 || a.Spec.Unschedulable != b.Spec.Unschedulable || !reflect.DeepEqual(a.Spec.Taints, b.Spec.Taints) {
				p.Trigger()
			}
		},
		DeleteFunc: func(any) { p.Trigger() },
	}); err != nil {
		return err
	}
	claimInf, err := mgr.GetCache().GetInformer(ctx, &v1alpha1.NodeClaim{})
	if err != nil {
		return err
	}
	name := func(o any) string {
		if t, ok := o.(toolscache.DeletedFinalStateUnknown); ok {
			o = t.Obj
		}
		if c, ok := o.(*v1alpha1.NodeClaim); ok {
			return c.Name
		}
		return ""
	}
	_, err = claimInf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(o any) { p.Observed(name(o)); p.Trigger() },
		UpdateFunc: func(oldO, newO any) {
			a, b := oldO.(*v1alpha1.NodeClaim), newO.(*v1alpha1.NodeClaim)
			if a.Status.Phase != b.Status.Phase || (a.DeletionTimestamp == nil) != (b.DeletionTimestamp == nil) {
				p.Trigger()
			}
		},
		DeleteFunc: func(o any) { p.Observed(name(o)); p.Trigger() },
	})
	if err != nil {
		return err
	}
	poolInf, err := mgr.GetCache().GetInformer(ctx, &v1alpha1.GPUNodePool{})
	if err != nil {
		return err
	}
	_, err = poolInf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { p.Trigger() },
		UpdateFunc: func(any, any) { p.Trigger() },
		DeleteFunc: func(any) { p.Trigger() },
	})
	return err
}
