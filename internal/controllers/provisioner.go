package controllers

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/config"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/metrics"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/plan"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/podutil"
)

// Provisioner turns pending GPU pods into NodeClaims and releases gangs. It runs only on the
// leader. Events (new pods, nodes becoming ready) only trigger it; each run reads the whole
// cluster from the cache, so a missed or duplicated event never changes the outcome.
type Provisioner struct {
	Client   client.Client
	Reader   client.Reader
	Cloud    *CloudState
	Settings config.Settings
	Recorder record.EventRecorder

	trigger  chan struct{}
	once     sync.Once
	mu       sync.Mutex
	waits    map[string]string
	lastPlan *plan.Plan
	// created are claims this provisioner made that the cache may not show yet. The next plan
	// waits for them, or it would buy the same servers again.
	created map[string]bool
}

// Trigger asks for a plan soon. It never blocks.
func (p *Provisioner) Trigger() {
	p.init()
	select {
	case p.trigger <- struct{}{}:
	default:
	}
}

func (p *Provisioner) init() {
	p.once.Do(func() {
		p.trigger = make(chan struct{}, 1)
		p.waits = map[string]string{}
		p.created = map[string]bool{}
	})
}

// NeedLeaderElection: only the leader provisions.
func (p *Provisioner) NeedLeaderElection() bool { return true }

// Start runs the batching loop until ctx ends.
func (p *Provisioner) Start(ctx context.Context) error {
	p.init()
	logger := log.FromContext(ctx).WithName("provisioner")
	resync := time.NewTicker(p.Settings.Resync.Duration)
	defer resync.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-resync.C:
		case <-p.trigger:
			p.batch(ctx)
		}
		if ctx.Err() != nil {
			return nil
		}
		if _, err := p.Once(ctx); err != nil {
			logger.Error(err, "plan failed")
		}
	}
}

// batch waits until triggers stop arriving for BatchIdle, or BatchMax has passed, so a job
// that creates 512 pods produces one plan instead of 512.
func (p *Provisioner) batch(ctx context.Context) {
	deadline := time.NewTimer(p.Settings.BatchMax.Duration)
	defer deadline.Stop()
	idle := time.NewTimer(p.Settings.BatchIdle.Duration)
	defer idle.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-idle.C:
			return
		case <-p.trigger:
			if !idle.Stop() {
				<-idle.C
			}
			idle.Reset(p.Settings.BatchIdle.Duration)
		}
	}
}

// LastPlan returns the most recent plan (for status and tests).
func (p *Provisioner) LastPlan() *plan.Plan {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastPlan
}

// awaitCache waits until the cache shows every claim created by earlier plans.
func (p *Provisioner) awaitCache(ctx context.Context) error {
	p.mu.Lock()
	want := len(p.created)
	p.mu.Unlock()
	if want == 0 {
		return nil
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var claims v1alpha1.NodeClaimList
		if err := p.Reader.List(ctx, &claims, client.UnsafeDisableDeepCopy); err != nil {
			return err
		}
		seen := map[string]bool{}
		for i := range claims.Items {
			seen[claims.Items[i].Name] = true
		}
		p.mu.Lock()
		for name := range p.created {
			if seen[name] {
				delete(p.created, name)
			}
		}
		left := len(p.created)
		if left > 0 && time.Now().After(deadline) {
			// Claims deleted before the cache ever saw them (an immediate launch failure) never show up.
			p.created = map[string]bool{}
			left = 0
		}
		p.mu.Unlock()
		if left == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Observed tells the provisioner the cache has seen an event for a claim (created or deleted).
func (p *Provisioner) Observed(name string) {
	p.init()
	p.mu.Lock()
	delete(p.created, name)
	p.mu.Unlock()
}

// Once computes and applies one plan.
func (p *Provisioner) Once(ctx context.Context) (*plan.Plan, error) {
	p.init()
	start := time.Now()
	if err := p.awaitCache(ctx); err != nil {
		return nil, err
	}
	view, err := p.Cloud.Get(ctx)
	if view.Types == nil {
		return nil, fmt.Errorf("cloud state: %w", err)
	}
	cl, err := LoadCluster(ctx, p.Reader)
	if err != nil {
		return nil, err
	}
	snap, gangs := cl.Snapshot(view)
	pl := plan.Compute(snap, gangs)
	metrics.PlanDuration.Observe(time.Since(start).Seconds())
	metrics.PendingPods.Set(float64(pl.Stats.Pods))
	metrics.Gangs.WithLabelValues("waiting").Set(float64(pl.Stats.GangsWaiting))
	metrics.Gangs.WithLabelValues("placed").Set(float64(pl.Stats.Gangs - pl.Stats.GangsWaiting))
	p.mu.Lock()
	p.lastPlan = pl
	p.mu.Unlock()
	return pl, p.apply(ctx, cl, pl)
}

func (p *Provisioner) apply(ctx context.Context, cl *Cluster, pl *plan.Plan) error {
	var errs []string
	if err := p.place(ctx, pl.Placements); err != nil {
		errs = append(errs, err.Error())
	}
	if err := p.create(ctx, cl, pl.Create); err != nil {
		errs = append(errs, err.Error())
	}
	if err := p.release(ctx, cl, pl.Releases); err != nil {
		errs = append(errs, err.Error())
	}
	p.explain(ctx, cl, pl.Waits)
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func (p *Provisioner) place(ctx context.Context, ps []plan.Placement) error {
	var first error
	for _, pl := range ps {
		for _, key := range pl.Pods {
			ns, name, _ := strings.Cut(key, "/")
			patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, v1alpha1.AnnotationPlacement, pl.Fabric)
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
			if err := p.Client.Patch(ctx, pod, client.RawPatch(types.MergePatchType, []byte(patch))); err != nil && !apierrors.IsNotFound(err) && first == nil {
				first = fmt.Errorf("annotate %s: %w", key, err)
			}
		}
	}
	return first
}

func (p *Provisioner) create(ctx context.Context, cl *Cluster, cs []plan.NewClaims) error {
	type job struct {
		pool   *v1alpha1.GPUNodePool
		fabric string
		reason string
	}
	var jobs []job
	for _, c := range cs {
		pool := cl.Pools[c.Pool]
		if pool == nil {
			continue
		}
		for i := 0; i < c.Count && len(jobs) < p.Settings.MaxCreatesPerPlan; i++ {
			jobs = append(jobs, job{pool, c.Fabric, c.Reason})
		}
	}
	sem := make(chan struct{}, p.Settings.CreateConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	for _, j := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(j job) {
			defer wg.Done()
			defer func() { <-sem }()
			claim := NewClaim(j.pool, j.fabric, j.reason)
			if err := p.Client.Create(ctx, claim); err != nil {
				mu.Lock()
				if first == nil {
					first = fmt.Errorf("create claim: %w", err)
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			p.mu.Lock()
			p.created[claim.Name] = true
			p.mu.Unlock()
			mu.Unlock()
			metrics.ClaimsCreated.WithLabelValues(j.pool.Name, reasonKind(j.reason)).Inc()
		}(j)
	}
	wg.Wait()
	return first
}

func reasonKind(reason string) string {
	switch {
	case reason == "singleton", reason == "min-nodes":
		return reason
	case strings.HasPrefix(reason, "replace:"):
		return "replace"
	default:
		return "gang"
	}
}

// NewClaim builds a NodeClaim for a pool in a fabric.
func NewClaim(pool *v1alpha1.GPUNodePool, fabric, reason string) *v1alpha1.NodeClaim {
	labels := map[string]string{v1alpha1.LabelPool: pool.Name, v1alpha1.LabelFabric: fabric}
	return &v1alpha1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{GenerateName: pool.Name + "-", Labels: labels},
		Spec: v1alpha1.NodeClaimSpec{
			Pool: pool.Name, InstanceType: pool.Spec.InstanceType, Fabric: fabric, Image: pool.Spec.Image,
			TemplateHash: pool.TemplateHash(), Labels: pool.Spec.Labels, Taints: pool.Spec.Taints, Reason: reason,
		},
	}
}

func (p *Provisioner) release(ctx context.Context, cl *Cluster, rs []plan.Release) error {
	var first error
	byKey := map[string]*corev1.Pod{}
	for _, pod := range cl.Pending {
		byKey[pod.Namespace+"/"+pod.Name] = pod
	}
	for _, r := range rs {
		var oldest time.Time
		for _, key := range r.Pods {
			pod := byKey[key]
			if pod == nil {
				continue
			}
			if oldest.IsZero() || pod.CreationTimestamp.Time.Before(oldest) {
				oldest = pod.CreationTimestamp.Time
			}
			if err := p.Client.Update(ctx, podutil.Release(pod, r.Fabric)); err != nil {
				if !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) && first == nil {
					first = fmt.Errorf("release %s: %w", key, err)
				}
				continue
			}
		}
		metrics.GangsReleased.Inc()
		if !oldest.IsZero() {
			metrics.GangReleaseLatency.Observe(time.Since(oldest).Seconds())
		}
		if pod := byKey[r.Pods[0]]; pod != nil && p.Recorder != nil {
			p.Recorder.Eventf(pod, corev1.EventTypeNormal, "GangReleased", "gang %s starts in fabric %s (%d pods)", r.Gang, r.Fabric, len(r.Pods))
		}
		p.mu.Lock()
		delete(p.waits, r.Gang)
		p.mu.Unlock()
	}
	return first
}

// explain records why gangs wait, as an event on one member, only when the reason changes.
func (p *Provisioner) explain(ctx context.Context, cl *Cluster, ws []plan.Wait) {
	if p.Recorder == nil {
		return
	}
	first := map[string]*corev1.Pod{}
	for _, pod := range cl.Pending {
		key := pod.Namespace + "/" + pod.Name
		if g, _ := podutil.Gang(pod); g != "" {
			key = g
		}
		if first[key] == nil {
			first[key] = pod
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range ws {
		key := w.Gang
		if key == "" {
			key = w.Pod
		}
		if p.waits[key] == w.Reason {
			continue
		}
		p.waits[key] = w.Reason
		if pod := first[key]; pod != nil {
			p.Recorder.Event(pod, corev1.EventTypeNormal, "WaitingForCapacity", w.Reason)
		}
	}
	_ = ctx
}
