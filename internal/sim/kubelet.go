package sim

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
)

// KubeletConfig sets how simulated nodes behave.
type KubeletConfig struct {
	// JoinDelay: from the instance running to its Node registering.
	JoinDelay time.Duration
	// GPUDelay: from registration to the device plugin advertising GPUs. Until then the Node is
	// Ready but has 0 allocatable GPUs, which is what real GPU nodes do while drivers load.
	GPUDelay time.Duration
	// PodStartDelay: from binding to Running.
	PodStartDelay time.Duration
	Tick          time.Duration
}

type simNode struct {
	name         string
	instanceID   string
	registeredAt time.Time
	gpus         int32
	noGPU        bool
	advertised   bool
	broken       bool
}

// Kubelet registers a Node for every running instance, advertises GPUs late, runs bound pods and
// completes pod deletions, like the kubelets of a real cluster would.
type Kubelet struct {
	Client client.Client
	Cloud  *Cloud
	Cfg    KubeletConfig

	mu      sync.Mutex
	nodes   map[string]*simNode // instance ID -> node
	seen    map[string]time.Time
	started map[string]time.Time // pod key -> bound at
}

// Start runs the kubelet loop until ctx ends.
func (k *Kubelet) Start(ctx context.Context) error {
	k.mu.Lock()
	k.nodes, k.seen, k.started = map[string]*simNode{}, map[string]time.Time{}, map[string]time.Time{}
	k.mu.Unlock()
	t := time.NewTicker(k.Cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := k.step(ctx); err != nil && ctx.Err() == nil {
				log.FromContext(ctx).V(1).Info("kubelet step", "err", err.Error())
			}
		}
	}
}

// Break makes a node NotReady (a failed GPU host).
func (k *Kubelet) Break(ctx context.Context, nodeName string) error {
	k.mu.Lock()
	for _, n := range k.nodes {
		if n.name == nodeName {
			n.broken = true
		}
	}
	k.mu.Unlock()
	node := &corev1.Node{}
	if err := k.Client.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return err
	}
	setReady(node, false)
	return k.Client.Status().Update(ctx, node)
}

func (k *Kubelet) step(ctx context.Context) error {
	now := time.Now()
	running := k.Cloud.runningInstances()
	live := map[string]bool{}
	for _, in := range running {
		live[in.ID] = true
		k.mu.Lock()
		n := k.nodes[in.ID]
		first, ok := k.seen[in.ID]
		if !ok {
			k.seen[in.ID] = now
			first = now
		}
		k.mu.Unlock()
		if n == nil {
			if now.Sub(first) < k.Cfg.JoinDelay {
				continue
			}
			node := newNode(in)
			if err := k.Client.Create(ctx, node); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
			if err := k.Client.Get(ctx, client.ObjectKeyFromObject(node), node); err != nil {
				return err
			}
			node.Status = nodeStatus(in, 0, true)
			if err := k.Client.Status().Update(ctx, node); err != nil {
				return err
			}
			k.mu.Lock()
			k.nodes[in.ID] = &simNode{name: node.Name, instanceID: in.ID, registeredAt: now, gpus: in.gpus, noGPU: in.noGPU}
			k.mu.Unlock()
			continue
		}
		if !n.advertised && !n.noGPU && !n.broken && now.Sub(n.registeredAt) >= k.Cfg.GPUDelay {
			node := &corev1.Node{}
			if err := k.Client.Get(ctx, client.ObjectKey{Name: n.name}, node); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return err
			}
			node.Status = nodeStatus(in, n.gpus, true)
			if err := k.Client.Status().Update(ctx, node); err != nil {
				if apierrors.IsConflict(err) {
					continue
				}
				return err
			}
			n.advertised = true
		}
	}
	if err := k.lifecycle(ctx); err != nil {
		return err
	}
	// Instances that are gone take their Node's heartbeat with them.
	k.mu.Lock()
	var gone []*simNode
	for id, n := range k.nodes {
		if !live[id] {
			gone = append(gone, n)
			delete(k.nodes, id)
		}
	}
	k.mu.Unlock()
	for _, n := range gone {
		node := &corev1.Node{}
		if err := k.Client.Get(ctx, client.ObjectKey{Name: n.name}, node); err == nil {
			setReady(node, false)
			_ = k.Client.Status().Update(ctx, node)
		}
	}
	return k.pods(ctx, now)
}

// lifecycle does what kube-controller-manager's node lifecycle controller does: the API server
// taints new nodes node.kubernetes.io/not-ready at creation, and the taint goes once the node is Ready.
func (k *Kubelet) lifecycle(ctx context.Context) error {
	k.mu.Lock()
	names := make([]string, 0, len(k.nodes))
	for _, n := range k.nodes {
		if !n.broken {
			names = append(names, n.name)
		}
	}
	k.mu.Unlock()
	for _, name := range names {
		node := &corev1.Node{}
		if err := k.Client.Get(ctx, client.ObjectKey{Name: name}, node); err != nil {
			continue
		}
		if !ready(node) {
			continue
		}
		taints := node.Spec.Taints[:0]
		changed := false
		for _, t := range node.Spec.Taints {
			if t.Key == corev1.TaintNodeNotReady || t.Key == corev1.TaintNodeUnreachable {
				changed = true
				continue
			}
			taints = append(taints, t)
		}
		if !changed {
			continue
		}
		n := node.DeepCopy()
		n.Spec.Taints = taints
		if err := k.Client.Update(ctx, n); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (k *Kubelet) pods(ctx context.Context, now time.Time) error {
	k.mu.Lock()
	mine := map[string]bool{}
	for _, n := range k.nodes {
		if !n.broken {
			mine[n.name] = true
		}
	}
	k.mu.Unlock()
	var pods corev1.PodList
	if err := k.Client.List(ctx, &pods); err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.NodeName == "" {
			continue
		}
		key := pod.Namespace + "/" + pod.Name
		if pod.DeletionTimestamp != nil {
			// A real kubelet stops the containers and confirms the deletion.
			_ = k.Client.Delete(ctx, pod, client.GracePeriodSeconds(0))
			k.mu.Lock()
			delete(k.started, key)
			k.mu.Unlock()
			continue
		}
		if !mine[pod.Spec.NodeName] || pod.Status.Phase != corev1.PodPending {
			continue
		}
		k.mu.Lock()
		at, ok := k.started[key]
		if !ok {
			k.started[key] = now
			at = now
		}
		k.mu.Unlock()
		if now.Sub(at) < k.Cfg.PodStartDelay {
			continue
		}
		pod.Status.Phase = corev1.PodRunning
		pod.Status.StartTime = &metav1.Time{Time: now}
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
		if err := k.Client.Status().Update(ctx, pod); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func newNode(in runningInstance) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-" + in.ID,
			Labels: map[string]string{
				"kubernetes.io/hostname":   "gpu-" + in.ID,
				v1alpha1.LabelInstanceType: in.InstanceType,
				"nvidia.com/gpu.product":   in.typ.GPUProduct,
			},
		},
		Spec: corev1.NodeSpec{
			ProviderID: in.ProviderID,
			Taints:     []corev1.Taint{{Key: v1alpha1.TaintUnregistered, Effect: corev1.TaintEffectNoExecute}},
		},
	}
}

func nodeStatus(in runningInstance, gpus int32, ready bool) corev1.NodeStatus {
	res := corev1.ResourceList{
		corev1.ResourceCPU:                        *resource.NewMilliQuantity(in.typ.MilliCPU, resource.DecimalSI),
		corev1.ResourceMemory:                     *resource.NewQuantity(in.typ.MemoryBytes, resource.BinarySI),
		corev1.ResourcePods:                       resource.MustParse("110"),
		corev1.ResourceName(v1alpha1.ResourceGPU): *resource.NewQuantity(int64(gpus), resource.DecimalSI),
	}
	st := corev1.NodeStatus{Capacity: res, Allocatable: res.DeepCopy()}
	n := &corev1.Node{Status: st}
	setReady(n, ready)
	return n.Status
}

func setReady(n *corev1.Node, ready bool) {
	status := corev1.ConditionTrue
	reason := "KubeletReady"
	if !ready {
		status, reason = corev1.ConditionUnknown, "NodeStatusUnknown"
	}
	now := metav1.Now()
	for i := range n.Status.Conditions {
		if n.Status.Conditions[i].Type == corev1.NodeReady {
			if n.Status.Conditions[i].Status != status {
				n.Status.Conditions[i].LastTransitionTime = now
			}
			n.Status.Conditions[i].Status, n.Status.Conditions[i].Reason, n.Status.Conditions[i].LastHeartbeatTime = status, reason, now
			return
		}
	}
	n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{
		Type: corev1.NodeReady, Status: status, Reason: reason, LastHeartbeatTime: now, LastTransitionTime: now,
		Message: fmt.Sprintf("simulated kubelet (ready=%v)", ready),
	})
}
