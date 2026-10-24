package sim

import (
	"context"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/plan"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/podutil"
)

// Scheduler binds pods to nodes the way kube-scheduler does for these workloads: filters on
// readiness, cordons, taints, node affinity and resources, and scores MostAllocated (the bin-packing
// setting GPU clusters use). It schedules one pod at a time; it knows nothing about gangs.
type Scheduler struct {
	Client client.Client
	Tick   time.Duration
	// Unschedulable records pods it could not place (with PodScheduled=False), like kube-scheduler.
	marked map[string]bool
}

type schedNode struct {
	node *corev1.Node
	free plan.Resources
}

// Start runs the scheduling loop until ctx ends.
func (s *Scheduler) Start(ctx context.Context) error {
	s.marked = map[string]bool{}
	t := time.NewTicker(s.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			_ = s.step(ctx)
		}
	}
}

func (s *Scheduler) step(ctx context.Context) error {
	var nodes corev1.NodeList
	if err := s.Client.List(ctx, &nodes); err != nil {
		return err
	}
	var pods corev1.PodList
	if err := s.Client.List(ctx, &pods); err != nil {
		return err
	}
	byName := map[string]*schedNode{}
	var cands []*schedNode
	for i := range nodes.Items {
		n := &nodes.Items[i]
		a := n.Status.Allocatable
		gpu := a["nvidia.com/gpu"]
		sn := &schedNode{node: n, free: plan.Resources{GPU: int32(gpu.Value()), MilliCPU: a.Cpu().MilliValue(), Memory: a.Memory().Value()}}
		byName[n.Name] = sn
		if n.Spec.Unschedulable || !ready(n) || n.DeletionTimestamp != nil {
			continue
		}
		cands = append(cands, sn)
	}
	var pending []*corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if p.Spec.NodeName != "" {
			if sn := byName[p.Spec.NodeName]; sn != nil {
				r := podutil.Requests(p)
				sn.free = plan.Resources{GPU: sn.free.GPU - r.GPU, MilliCPU: sn.free.MilliCPU - r.MilliCPU, Memory: sn.free.Memory - r.Memory}
			}
			continue
		}
		if p.DeletionTimestamp == nil && len(p.Spec.SchedulingGates) == 0 {
			pending = append(pending, p)
		}
	}
	sort.SliceStable(pending, func(i, j int) bool {
		pi, pj := prio(pending[i]), prio(pending[j])
		if pi != pj {
			return pi > pj
		}
		return pending[i].CreationTimestamp.Before(&pending[j].CreationTimestamp)
	})
	for _, p := range pending {
		r := podutil.Requests(p)
		var best *schedNode
		for _, sn := range cands {
			if sn.free.GPU < r.GPU || sn.free.MilliCPU < r.MilliCPU || sn.free.Memory < r.Memory {
				continue
			}
			if _, bad := corev1helpers.FindMatchingUntoleratedTaint(sn.node.Spec.Taints, p.Spec.Tolerations, func(t *corev1.Taint) bool {
				return t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute
			}); bad {
				continue
			}
			if ok, err := nodeaffinity.GetRequiredNodeAffinity(p).Match(sn.node); err != nil || !ok {
				continue
			}
			if best == nil || sn.free.GPU < best.free.GPU || (sn.free.GPU == best.free.GPU && sn.node.Name < best.node.Name) {
				best = sn
			}
		}
		key := p.Namespace + "/" + p.Name
		if best == nil {
			if !s.marked[key] {
				s.markUnschedulable(ctx, p)
				s.marked[key] = true
			}
			continue
		}
		b := &corev1.Binding{ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name},
			Target: corev1.ObjectReference{Kind: "Node", Name: best.node.Name}}
		if err := s.Client.SubResource("binding").Create(ctx, p, b); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				continue
			}
			continue
		}
		delete(s.marked, key)
		best.free = plan.Resources{GPU: best.free.GPU - r.GPU, MilliCPU: best.free.MilliCPU - r.MilliCPU, Memory: best.free.Memory - r.Memory}
	}
	return nil
}

func (s *Scheduler) markUnschedulable(ctx context.Context, p *corev1.Pod) {
	pod := p.DeepCopy()
	cond := corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
		Message: "0 nodes are available", LastTransitionTime: metav1.Now()}
	found := false
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodScheduled {
			pod.Status.Conditions[i] = cond
			found = true
		}
	}
	if !found {
		pod.Status.Conditions = append(pod.Status.Conditions, cond)
	}
	_ = s.Client.Status().Update(ctx, pod)
}

func prio(p *corev1.Pod) int32 {
	if p.Spec.Priority != nil {
		return *p.Spec.Priority
	}
	return 0
}

func ready(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
