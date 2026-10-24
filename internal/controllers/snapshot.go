package controllers

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/plan"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/podutil"
)

// Cluster is a read-only view of the objects the provisioner works with. Objects come straight
// from the informer cache without deep copies, so they must never be modified; copy first.
type Cluster struct {
	Pools      map[string]*v1alpha1.GPUNodePool
	Claims     []*v1alpha1.NodeClaim
	Nodes      map[string]*corev1.Node
	PodsByNode map[string][]*corev1.Pod
	Pending    []*corev1.Pod
	// AllPods by namespace/name, for gang lookups.
	GangPods map[string][]*corev1.Pod
}

// LoadCluster lists pools, claims, nodes and pods from the cache.
func LoadCluster(ctx context.Context, c client.Reader) (*Cluster, error) {
	noCopy := client.UnsafeDisableDeepCopy
	var pools v1alpha1.GPUNodePoolList
	if err := c.List(ctx, &pools, noCopy); err != nil {
		return nil, err
	}
	var claims v1alpha1.NodeClaimList
	if err := c.List(ctx, &claims, noCopy); err != nil {
		return nil, err
	}
	var nodes corev1.NodeList
	if err := c.List(ctx, &nodes, client.HasLabels{v1alpha1.LabelPool}, noCopy); err != nil {
		return nil, err
	}
	var pods corev1.PodList
	if err := c.List(ctx, &pods, noCopy); err != nil {
		return nil, err
	}
	cl := &Cluster{
		Pools: map[string]*v1alpha1.GPUNodePool{}, Nodes: map[string]*corev1.Node{},
		PodsByNode: map[string][]*corev1.Pod{}, GangPods: map[string][]*corev1.Pod{},
	}
	for i := range pools.Items {
		cl.Pools[pools.Items[i].Name] = &pools.Items[i]
	}
	for i := range claims.Items {
		cl.Claims = append(cl.Claims, &claims.Items[i])
	}
	for i := range nodes.Items {
		cl.Nodes[nodes.Items[i].Name] = &nodes.Items[i]
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if podutil.Terminal(pod) {
			continue
		}
		if g, _ := podutil.Gang(pod); g != "" {
			cl.GangPods[g] = append(cl.GangPods[g], pod)
		}
		if pod.Spec.NodeName != "" {
			cl.PodsByNode[pod.Spec.NodeName] = append(cl.PodsByNode[pod.Spec.NodeName], pod)
			continue
		}
		if podutil.WantsGPU(pod) && !podutil.ForeignGated(pod) {
			cl.Pending = append(cl.Pending, pod)
		}
	}
	return cl, nil
}

// Drifted reports whether a claim was made under a different template than its pool's current one.
func (cl *Cluster) Drifted(claim *v1alpha1.NodeClaim) bool {
	pool := cl.Pools[claim.Spec.Pool]
	return pool != nil && claim.Spec.TemplateHash != pool.TemplateHash()
}

// Node returns the Node of a claim, if registered.
func (cl *Cluster) Node(claim *v1alpha1.NodeClaim) *corev1.Node {
	if claim.Status.NodeName == "" {
		return nil
	}
	return cl.Nodes[claim.Status.NodeName]
}

// Snapshot converts the cluster and the cloud view into planner input.
func (cl *Cluster) Snapshot(v View) (*plan.Snapshot, map[string]plan.GangInfo) {
	s := &plan.Snapshot{Available: v.Available, Unavailable: v.Unavailable}
	for _, pool := range cl.Pools {
		if pool.DeletionTimestamp != nil {
			continue
		}
		it, ok := v.Types[pool.Spec.InstanceType]
		if !ok {
			continue
		}
		s.Pools = append(s.Pools, plan.Pool{
			Name: pool.Name, InstanceType: it, Fabrics: pool.Spec.Fabrics, MinNodes: pool.Spec.Limits.MinNodes,
			MaxNodes: pool.Spec.Limits.MaxNodes, Weight: pool.Spec.Weight, Labels: pool.Spec.Labels,
			Taints: pool.Spec.Taints, TemplateHash: pool.TemplateHash(),
		})
	}
	var claimGPUs int64
	for _, claim := range cl.Claims {
		it, ok := v.Types[claim.Spec.InstanceType]
		if !ok {
			continue
		}
		claimGPUs += int64(it.GPUs)
		n := plan.Node{
			Claim: claim.Name, NodeName: claim.Status.NodeName, Pool: claim.Spec.Pool, Fabric: claim.Spec.Fabric,
			Capacity:    plan.Resources{GPU: it.GPUs, MilliCPU: it.MilliCPU, Memory: it.MemoryBytes},
			Initialized: claim.Status.Phase == v1alpha1.PhaseInitialized,
			// The offerings include this server's host unless it launched before they were read.
			Launched:    claim.Status.InstanceID != "" && launchedBefore(claim, v.FetchedAt),
			Schedulable: claim.DeletionTimestamp == nil && !cl.Drifted(claim),
		}
		if node := cl.Node(claim); node != nil {
			if node.Spec.Unschedulable {
				n.Schedulable = false
			}
			for _, pod := range cl.PodsByNode[node.Name] {
				r := podutil.Requests(pod)
				n.Used = plan.Resources{GPU: n.Used.GPU + r.GPU, MilliCPU: n.Used.MilliCPU + r.MilliCPU, Memory: n.Used.Memory + r.Memory}
			}
		}
		s.Nodes = append(s.Nodes, n)
	}
	used := v.Quota.GPUUsed
	if claimGPUs > used {
		used = claimGPUs
	}
	s.QuotaFreeGPUs = v.Quota.GPULimit - used
	gangs := map[string]plan.GangInfo{}
	for _, pod := range cl.Pending {
		g, size := podutil.Gang(pod)
		p := plan.Pod{
			Key: pod.Namespace + "/" + pod.Name, Gang: g, GangSize: size, Requests: podutil.Requests(pod),
			Gated: podutil.Gated(pod), Fabric: podutil.Fabric(pod), PlannedFabric: pod.Annotations[v1alpha1.AnnotationPlacement],
			Created: pod.CreationTimestamp.Time, Spec: &pod.Spec,
		}
		if pod.Spec.Priority != nil {
			p.Priority = *pod.Spec.Priority
		}
		if p.Requests.GPU <= 0 {
			continue
		}
		s.Pods = append(s.Pods, p)
		if g != "" {
			if _, seen := gangs[g]; !seen {
				gangs[g] = cl.gangInfo(g)
			}
		}
	}
	return s, gangs
}

func (cl *Cluster) gangInfo(g string) plan.GangInfo {
	var gi plan.GangInfo
	for _, pod := range cl.GangPods[g] {
		if pod.Spec.NodeName == "" {
			continue
		}
		gi.Bound++
		if gi.Fabric == "" {
			if n := cl.Nodes[pod.Spec.NodeName]; n != nil {
				gi.Fabric = n.Labels[v1alpha1.LabelFabric]
			}
		}
	}
	return gi
}

func launchedBefore(claim *v1alpha1.NodeClaim, t time.Time) bool {
	c := meta.FindStatusCondition(claim.Status.Conditions, v1alpha1.ConditionLaunched)
	return c != nil && c.LastTransitionTime.Time.Before(t)
}
