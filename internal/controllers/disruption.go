package controllers

import (
	"context"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/config"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/metrics"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/plan"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/podutil"
)

// Disruptor removes nodes nobody needs (consolidation) and replaces nodes made from an old
// template (drift), within each pool's disruption budget. It never voluntarily disrupts a node
// that runs gang members, a pod marked do-not-disrupt, or a pod no controller would recreate,
// and it evicts through the Eviction API so PodDisruptionBudgets hold.
type Disruptor struct {
	Client   client.Client
	Reader   client.Reader
	Cloud    *CloudState
	Settings config.Settings
	Recorder record.EventRecorder
	Now      func() time.Time

	mu            sync.Mutex
	underutilized map[string]time.Time
	emptySince    map[string]time.Time
}

// NeedLeaderElection: only the leader disrupts.
func (d *Disruptor) NeedLeaderElection() bool { return true }

// Start runs disruption passes until ctx ends.
func (d *Disruptor) Start(ctx context.Context) error {
	t := time.NewTicker(d.Settings.DisruptionInterval.Duration)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if _, err := d.Once(ctx); err != nil {
				log.FromContext(ctx).Error(err, "disruption pass failed")
			}
		}
	}
}

func (d *Disruptor) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Summary says what a pass did.
type Summary struct {
	DeletedEmpty   int
	DeletedDrifted int
	Replacements   int
	Draining       int
	Blocked        map[string]int
	DriftedWaiting int
	Underutilized  int
}

type nodeInfo struct {
	claim    *v1alpha1.NodeClaim
	node     *corev1.Node
	pods     []*corev1.Pod
	gang     bool
	pinned   bool // do-not-disrupt, or pods nobody would recreate
	usedGPU  int32
	drifted  bool
	deleting bool
	draining bool
}

// Once runs one disruption pass.
func (d *Disruptor) Once(ctx context.Context) (Summary, error) {
	sum := Summary{Blocked: map[string]int{}}
	view, err := d.Cloud.Get(ctx)
	if view.Types == nil {
		return sum, err
	}
	cl, err := LoadCluster(ctx, d.Reader)
	if err != nil {
		return sum, err
	}
	snap, gangs := cl.Snapshot(view)
	reserved := plan.Compute(snap, gangs).Reserved

	byPool := map[string][]*nodeInfo{}
	replacementFor := map[string]*v1alpha1.NodeClaim{}
	for _, claim := range cl.Claims {
		if len(claim.Spec.Reason) > 8 && claim.Spec.Reason[:8] == "replace:" {
			replacementFor[claim.Spec.Reason[8:]] = claim
		}
		ni := &nodeInfo{claim: claim, node: cl.Node(claim), drifted: cl.Drifted(claim), deleting: claim.DeletionTimestamp != nil}
		ni.draining = meta.IsStatusConditionTrue(claim.Status.Conditions, v1alpha1.ConditionDisrupting)
		if ni.node != nil {
			for _, pod := range cl.PodsByNode[ni.node.Name] {
				if podutil.DaemonSet(pod) {
					continue
				}
				ni.pods = append(ni.pods, pod)
				ni.usedGPU += podutil.Requests(pod).GPU
				if g, _ := podutil.Gang(pod); g != "" {
					ni.gang = true
				}
				if pod.Annotations[v1alpha1.AnnotationDoNotDisrupt] == "true" || !podutil.Controlled(pod) {
					ni.pinned = true
				}
			}
		}
		byPool[claim.Spec.Pool] = append(byPool[claim.Spec.Pool], ni)
	}
	names := make([]string, 0, len(byPool))
	for n := range byPool {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		pool := cl.Pools[name]
		if pool == nil {
			continue
		}
		d.pool(ctx, pool, byPool[name], replacementFor, reserved, &sum)
	}
	return sum, nil
}

func budget(pool *v1alpha1.GPUNodePool, total int) int {
	mu := pool.Spec.Disruption.MaxUnavailable
	if mu == nil {
		v := intstr.FromString("10%")
		mu = &v
	}
	n, err := intstr.GetScaledValueFromIntOrPercent(mu, total, true)
	if err != nil || n < 1 {
		n = 1
	}
	return n
}

func (d *Disruptor) pool(ctx context.Context, pool *v1alpha1.GPUNodePool, nodes []*nodeInfo, replacementFor map[string]*v1alpha1.NodeClaim, reserved map[string]bool, sum *Summary) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].claim.Name < nodes[j].claim.Name })
	total := len(nodes)
	room := budget(pool, total)
	for _, n := range nodes {
		if n.deleting || n.draining {
			room--
		}
	}
	now := d.now()
	consolidateAfter := pool.Spec.Disruption.ConsolidateAfter.Duration

	// 1. Finish what was started: drained nodes are deleted, the rest keep draining.
	for _, n := range nodes {
		if !n.draining || n.deleting {
			continue
		}
		if len(n.pods) == 0 {
			reason := "drifted"
			if c := meta.FindStatusCondition(n.claim.Status.Conditions, v1alpha1.ConditionDisrupting); c != nil && c.Reason == "Underutilized" {
				reason = "underutilized"
			}
			if err := DeleteClaim(ctx, d.Client, n.claim, reason, false); err == nil {
				n.deleting = true
			}
			continue
		}
		if _, err := Drain(ctx, d.Client, n.node.Name, false); err != nil {
			log.FromContext(ctx).Info("drain failed", "node", n.node.Name, "err", err.Error())
		}
		sum.Draining++
	}

	// 2. Track emptiness on initialized nodes; it is what consolidateAfter is measured from.
	// It is kept in memory, not in the claim's status: a node goes empty and busy many times,
	// and two status writes per transition per node add up at 20,000 nodes. After a leader
	// change the timers start again, which only delays consolidation.
	d.mu.Lock()
	if d.emptySince == nil {
		d.emptySince = map[string]time.Time{}
	}
	for _, n := range nodes {
		key := n.claim.Name
		if n.deleting || n.claim.Status.Phase != v1alpha1.PhaseInitialized || len(n.pods) > 0 {
			delete(d.emptySince, key)
			continue
		}
		if _, ok := d.emptySince[key]; !ok {
			d.emptySince[key] = now
		}
	}
	d.mu.Unlock()

	alive := func() int {
		k := 0
		for _, n := range nodes {
			if !n.deleting {
				k++
			}
		}
		return k
	}

	// 3. Drift: empty drifted nodes go now; busy ones are replaced with surge, then drained.
	surge := 0
	for _, n := range nodes {
		if n.drifted && !n.deleting && replacementFor[n.claim.Name] != nil {
			surge++
		}
	}
	for _, n := range nodes {
		if !n.drifted || n.deleting || n.draining {
			continue
		}
		if n.claim.Status.Phase != v1alpha1.PhaseInitialized {
			if len(n.pods) == 0 && room > 0 { // never initialized: nothing runs there
				if DeleteClaim(ctx, d.Client, n.claim, "drifted", false) == nil {
					n.deleting = true
					room--
					sum.DeletedDrifted++
				}
			}
			continue
		}
		if len(n.pods) == 0 {
			if room <= 0 {
				d.blocked(sum, "budget")
				continue
			}
			if DeleteClaim(ctx, d.Client, n.claim, "drifted", false) == nil {
				n.deleting = true
				room--
				sum.DeletedDrifted++
			}
			continue
		}
		if n.gang {
			d.blocked(sum, "gang-running")
			sum.DriftedWaiting++
			continue
		}
		if n.pinned {
			d.blocked(sum, "do-not-disrupt")
			sum.DriftedWaiting++
			continue
		}
		repl := replacementFor[n.claim.Name]
		if repl == nil && pool.Spec.Disruption.MaxSurge > 0 {
			if surge >= int(pool.Spec.Disruption.MaxSurge) {
				d.blocked(sum, "surge")
				sum.DriftedWaiting++
				continue
			}
			if pool.Spec.Limits.MaxNodes != nil && alive()+surge >= int(*pool.Spec.Limits.MaxNodes)+int(pool.Spec.Disruption.MaxSurge) {
				d.blocked(sum, "max-nodes")
				continue
			}
			c := NewClaim(pool, n.claim.Spec.Fabric, "replace:"+n.claim.Name)
			if err := d.Client.Create(ctx, c); err == nil {
				surge++
				sum.Replacements++
				metrics.ClaimsCreated.WithLabelValues(pool.Name, "replace").Inc()
			}
			sum.DriftedWaiting++
			continue
		}
		if repl != nil && repl.Status.Phase != v1alpha1.PhaseInitialized {
			sum.DriftedWaiting++
			continue // wait for the replacement to be ready
		}
		if room <= 0 {
			d.blocked(sum, "budget")
			sum.DriftedWaiting++
			continue
		}
		if d.startDrain(ctx, n, "Drifted") {
			room--
			sum.Draining++
		}
	}

	if !pool.ConsolidationEnabled() {
		return
	}

	// 4. Empty nodes nobody has planned work for, after consolidateAfter, down to minNodes.
	for _, n := range nodes {
		if n.deleting || n.draining || n.claim.Status.Phase != v1alpha1.PhaseInitialized || len(n.pods) > 0 {
			continue
		}
		d.mu.Lock()
		since, ok := d.emptySince[n.claim.Name]
		d.mu.Unlock()
		if !ok || now.Sub(since) < consolidateAfter {
			continue
		}
		if reserved[n.claim.Name] {
			d.blocked(sum, "reserved")
			continue
		}
		if alive() <= int(pool.Spec.Limits.MinNodes) {
			d.blocked(sum, "min-nodes")
			break
		}
		if room <= 0 {
			d.blocked(sum, "budget")
			break
		}
		if DeleteClaim(ctx, d.Client, n.claim, "empty", false) == nil {
			n.deleting = true
			room--
			sum.DeletedEmpty++
		}
	}

	// 5. One underutilized node per pass: only single pods with controllers, which fit on the
	// other initialized nodes of the pool.
	d.mu.Lock()
	if d.underutilized == nil {
		d.underutilized = map[string]time.Time{}
	}
	d.mu.Unlock()
	for _, n := range nodes {
		key := n.claim.Name
		ok := !n.deleting && !n.draining && !n.drifted && !n.gang && !n.pinned && len(n.pods) > 0 &&
			n.claim.Status.Phase == v1alpha1.PhaseInitialized && !reserved[key] &&
			n.usedGPU*2 <= n.claim.Status.GPUs && fitsElsewhere(n, nodes)
		d.mu.Lock()
		since, seen := d.underutilized[key]
		if !ok {
			delete(d.underutilized, key)
			d.mu.Unlock()
			continue
		}
		if !seen {
			d.underutilized[key] = now
			d.mu.Unlock()
			continue
		}
		d.mu.Unlock()
		if now.Sub(since) < consolidateAfter || room <= 0 {
			continue
		}
		if d.startDrain(ctx, n, "Underutilized") {
			sum.Underutilized++
			d.mu.Lock()
			delete(d.underutilized, key)
			d.mu.Unlock()
		}
		break
	}
}

func fitsElsewhere(target *nodeInfo, nodes []*nodeInfo) bool {
	var free []int32
	for _, n := range nodes {
		if n == target || n.deleting || n.draining || n.drifted || n.node == nil || n.node.Spec.Unschedulable ||
			n.claim.Status.Phase != v1alpha1.PhaseInitialized {
			continue
		}
		if f := n.claim.Status.GPUs - n.usedGPU; f > 0 {
			free = append(free, f)
		}
	}
	reqs := make([]int32, 0, len(target.pods))
	for _, p := range target.pods {
		reqs = append(reqs, podutil.Requests(p).GPU)
	}
	sort.Slice(reqs, func(i, j int) bool { return reqs[i] > reqs[j] })
	for _, r := range reqs {
		best := -1
		for i, f := range free {
			if f >= r && (best < 0 || f < free[best]) {
				best = i
			}
		}
		if best < 0 {
			return false
		}
		free[best] -= r
	}
	return true
}

func (d *Disruptor) startDrain(ctx context.Context, n *nodeInfo, reason string) bool {
	c := n.claim.DeepCopy()
	setCondition(&c.Status.Conditions, v1alpha1.ConditionDisrupting, true, reason, "draining before removal", c.Generation)
	if err := d.Client.Status().Update(ctx, c); err != nil {
		return false
	}
	n.claim, n.draining = c, true
	if n.node != nil {
		if err := Cordon(ctx, d.Client, n.node); err != nil {
			return false
		}
		if _, err := Drain(ctx, d.Client, n.node.Name, false); err != nil {
			log.FromContext(ctx).Info("drain failed", "node", n.node.Name, "err", err.Error())
		}
	}
	if d.Recorder != nil {
		d.Recorder.Eventf(c, corev1.EventTypeNormal, "Disrupting", "%s: cordoned and draining", reason)
	}
	return true
}

func (d *Disruptor) blocked(sum *Summary, reason string) {
	sum.Blocked[reason]++
	metrics.DisruptionBlocked.WithLabelValues(reason).Inc()
}
