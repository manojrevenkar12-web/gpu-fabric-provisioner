// Package naive is the baseline the drills compare against: a provisioner that works the way a
// Cluster-Autoscaler-style loop does when nothing in it knows about GPUs, gangs or fabrics. It
// is deliberately ordinary, not broken:
//
//   - it scales up for pods the scheduler marked Unschedulable;
//   - it counts nodes that have not registered yet as upcoming capacity, and registered nodes by
//     their allocatable resources, so a GPU node whose device plugin has not started counts as 0 GPUs;
//   - each fabric is a node group; one scale-up goes to one group chosen at random (the groups
//     are identical, so a least-waste expander ties);
//   - a group that failed with InsufficientCapacity is backed off for a while;
//   - it does not know the cloud's free capacity or quota until a create fails;
//   - gang pods are scheduled one by one as nodes appear; nothing holds them back.
//
// It shares the NodeClaim lifecycle and consolidation with the real provisioner, so the only
// difference the drills measure is the scale-up decision.
package naive

import (
	"context"
	"math/rand"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/controllers"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/podutil"
)

// Provisioner is the naive scale-up loop.
type Provisioner struct {
	Client   client.Client
	Reader   client.Reader
	Cloud    *controllers.CloudState
	Interval time.Duration
	Seed     int64
	rng      *rand.Rand
}

// NeedLeaderElection: only the leader provisions.
func (p *Provisioner) NeedLeaderElection() bool { return true }

// Start runs the loop until ctx ends.
func (p *Provisioner) Start(ctx context.Context) error {
	p.rng = rand.New(rand.NewSource(p.Seed))
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := p.Once(ctx); err != nil {
				log.FromContext(ctx).Error(err, "naive scale-up failed")
			}
		}
	}
}

type slot struct {
	pool, fabric string
	free         int32
}

func unschedulable(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
			return true
		}
	}
	return false
}

// Once runs one scale-up iteration.
func (p *Provisioner) Once(ctx context.Context) error {
	if p.rng == nil {
		p.rng = rand.New(rand.NewSource(p.Seed))
	}
	view, err := p.Cloud.Get(ctx)
	if view.Types == nil {
		return err
	}
	cl, err := controllers.LoadCluster(ctx, p.Reader)
	if err != nil {
		return err
	}
	var pending []*corev1.Pod
	for _, pod := range cl.Pending {
		if !podutil.Gated(pod) && unschedulable(pod) {
			pending = append(pending, pod)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	var slots []*slot
	count := map[string]int32{}
	for _, c := range cl.Claims {
		count[c.Spec.Pool]++
		if c.DeletionTimestamp != nil {
			continue
		}
		it := view.Types[c.Spec.InstanceType]
		node := cl.Node(c)
		if node == nil {
			if c.Status.Phase == v1alpha1.PhasePending || c.Status.Phase == v1alpha1.PhaseLaunched || c.Status.Phase == "" {
				slots = append(slots, &slot{c.Spec.Pool, c.Spec.Fabric, it.GPUs}) // upcoming node
			}
			continue
		}
		if node.Spec.Unschedulable {
			continue
		}
		alloc := node.Status.Allocatable[v1alpha1.ResourceGPU]
		free := int32(alloc.Value())
		for _, pod := range cl.PodsByNode[node.Name] {
			free -= podutil.Requests(pod).GPU
		}
		if free > 0 {
			slots = append(slots, &slot{c.Spec.Pool, c.Spec.Fabric, free})
		}
	}
	sort.Slice(pending, func(i, j int) bool { return podutil.Requests(pending[i]).GPU > podutil.Requests(pending[j]).GPU })
	var need []*corev1.Pod
	for _, pod := range pending {
		r := podutil.Requests(pod).GPU
		f := podutil.Fabric(pod)
		placed := false
		for _, s := range slots {
			if s.free >= r && (f == "" || f == s.fabric) {
				s.free -= r
				placed = true
				break
			}
		}
		if !placed {
			need = append(need, pod)
		}
	}
	if len(need) == 0 {
		return nil
	}
	// Node groups: every (pool, fabric) pair, minus groups in backoff or at their pool's max.
	type group struct {
		pool   *v1alpha1.GPUNodePool
		fabric string
	}
	var groups []group
	names := make([]string, 0, len(cl.Pools))
	for n := range cl.Pools {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		pool := cl.Pools[n]
		if pool.Spec.Limits.MaxNodes != nil && count[n] >= *pool.Spec.Limits.MaxNodes {
			continue
		}
		fabrics := pool.Spec.Fabrics
		if len(fabrics) == 0 {
			for f, types := range view.Available {
				if _, ok := types[pool.Spec.InstanceType]; ok {
					fabrics = append(fabrics, f)
				}
			}
		}
		sort.Strings(fabrics)
		for _, f := range fabrics {
			if view.Unavailable[f+"/"+pool.Spec.InstanceType] {
				continue
			}
			groups = append(groups, group{pool, f})
		}
	}
	if len(groups) == 0 {
		return nil
	}
	g := groups[p.rng.Intn(len(groups))]
	it := view.Types[g.pool.Spec.InstanceType]
	// Pods pinned to another fabric cannot use this group.
	var gpus []int32
	for _, pod := range need {
		if f := podutil.Fabric(pod); f == "" || f == g.fabric {
			gpus = append(gpus, podutil.Requests(pod).GPU)
		}
	}
	nodes := 0
	var open []int32
	for _, r := range gpus {
		placed := false
		for i := range open {
			if open[i] >= r {
				open[i] -= r
				placed = true
				break
			}
		}
		if !placed && r <= it.GPUs {
			open = append(open, it.GPUs-r)
			nodes++
		}
	}
	if max := g.pool.Spec.Limits.MaxNodes; max != nil && count[g.pool.Name]+int32(nodes) > *max {
		nodes = int(*max - count[g.pool.Name])
	}
	for i := 0; i < nodes; i++ {
		if err := p.Client.Create(ctx, controllers.NewClaim(g.pool, g.fabric, "naive")); err != nil {
			return err
		}
	}
	return nil
}
