package controllers_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/config"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/podutil"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/sim"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/simenv"
)

func needAPIServer(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs kube-apiserver and etcd")
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set")
	}
}

func start(t *testing.T, hosts map[string]int32, mutate func(*simenv.Options)) (*simenv.Env, context.Context) {
	t.Helper()
	needAPIServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	o := simenv.Options{Cloud: simenv.FastCloud(hosts), Kubelet: simenv.FastKubelet()}
	if mutate != nil {
		mutate(&o)
	}
	env, err := simenv.Start(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = env.Stop()
	})
	return env, ctx
}

func manager(t *testing.T, ctx context.Context, env *simenv.Env, s config.Settings) *simenv.Manager {
	t.Helper()
	m, err := env.StartManager(ctx, simenv.ManagerOptions{Settings: s})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	return m
}

func newPool(name string, mutate func(*v1alpha1.GPUNodePool)) *v1alpha1.GPUNodePool {
	p := &v1alpha1.GPUNodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.GPUNodePoolSpec{
			InstanceType: simenv.H100.Name, Image: "ubuntu24-cuda12-v1",
			Taints: []corev1.Taint{{Key: "nvidia.com/gpu", Effect: corev1.TaintEffectNoSchedule}},
			Disruption: v1alpha1.Disruption{
				ConsolidateAfter: metav1.Duration{Duration: time.Hour}, MaxSurge: 1,
				MaxUnavailable: ptr.To(intstr.FromString("50%")), GangPolicy: v1alpha1.GangWaitForCompletion,
			},
		},
	}
	if mutate != nil {
		mutate(p)
	}
	return p
}

var gpuToleration = []corev1.Toleration{{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists}}

func workload(t *testing.T, ctx context.Context, env *simenv.Env, gate bool, jobs ...sim.JobSpec) *sim.Workload {
	t.Helper()
	for i := range jobs {
		if jobs[i].Namespace == "" {
			jobs[i].Namespace = "default"
		}
	}
	w := &sim.Workload{Client: env.Client, Cloud: env.Cloud, Jobs: jobs, Gate: gate, Tolerations: gpuToleration, Tick: 100 * time.Millisecond}
	go func() { _ = w.Start(ctx) }()
	return w
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		ok, msg := cond()
		if ok {
			return
		}
		last = msg
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s: %s", timeout, what, last)
}

func claims(t *testing.T, ctx context.Context, env *simenv.Env) []v1alpha1.NodeClaim {
	t.Helper()
	var l v1alpha1.NodeClaimList
	if err := env.Direct.List(ctx, &l); err != nil {
		t.Fatal(err)
	}
	return l.Items
}

func TestGangGetsOneFabricAndStartsTogether(t *testing.T) {
	env, ctx := start(t, map[string]int32{"fab-a": 10, "fab-b": 6}, nil)
	manager(t, ctx, env, simenv.FastSettings())
	if err := env.Direct.Create(ctx, newPool("train", nil)); err != nil {
		t.Fatal(err)
	}
	w := workload(t, ctx, env, true, sim.JobSpec{Name: "llm", Members: 4, GPUs: 8, Duration: 2 * time.Second, Gang: true})

	// Watch for the moment the gate comes off: every claim must be initialized by then.
	released := false
	eventually(t, 60*time.Second, "gang to complete", func() (bool, string) {
		var pods corev1.PodList
		_ = env.Direct.List(ctx, &pods, client.HasLabels{sim.LabelJob})
		if !released {
			for i := range pods.Items {
				if !podutil.Gated(&pods.Items[i]) {
					released = true
					for _, c := range claims(t, ctx, env) {
						if c.Status.Phase != v1alpha1.PhaseInitialized {
							t.Errorf("gang released while claim %s is %s", c.Name, c.Status.Phase)
						}
					}
					break
				}
			}
		}
		return w.Done(), fmt.Sprintf("records %+v %s", w.Records(), dump(ctx, env))
	})
	rec := w.Records()[0]
	if len(rec.Fabrics) != 1 || rec.Fabrics[0] != "fab-b" {
		t.Fatalf("gang ran in %v, want only fab-b (the best fit for 4 hosts)", rec.Fabrics)
	}
	if l := env.Cloud.Ledger(); l.Instances != 4 {
		t.Fatalf("cloud created %d instances for a 4-node gang", l.Instances)
	}
}

// The API server accepts narrowing node affinity and removing the gate in one update only while
// the pod is gated. This is what makes fabric placement safe without a custom scheduler.
func TestReleaseIsAcceptedOnlyForGatedPods(t *testing.T) {
	env, ctx := start(t, map[string]int32{"fab-a": 1}, func(o *simenv.Options) { o.NoKubelet, o.NoScheduler = true, true })
	mk := func(name string, gated bool) *corev1.Pod {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "c", Image: "x"}},
				Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"z1"}}}}},
				}}},
			},
		}
		if gated {
			p.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: v1alpha1.SchedulingGate}}
		}
		if err := env.Direct.Create(ctx, p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	gated := mk("gated", true)
	if err := env.Direct.Update(ctx, podutil.Release(gated, "fab-a")); err != nil {
		t.Fatalf("release of a gated pod rejected: %v", err)
	}
	got := &corev1.Pod{}
	_ = env.Direct.Get(ctx, client.ObjectKeyFromObject(gated), got)
	if podutil.Gated(got) || podutil.Fabric(got) != "fab-a" {
		t.Fatalf("after release: gates %v fabric %q", got.Spec.SchedulingGates, podutil.Fabric(got))
	}
	open := mk("open", false)
	if err := env.Direct.Update(ctx, podutil.Release(open, "fab-a")); err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("narrowing affinity of an ungated pod should be rejected, got %v", err)
	}
}

func TestInsufficientCapacityMovesWorkToAnotherFabric(t *testing.T) {
	env, ctx := start(t, map[string]int32{"fab-a": 2, "fab-b": 10}, func(o *simenv.Options) {
		o.Cloud.Faults.ICE = map[string]float64{"fab-a": 1}
	})
	manager(t, ctx, env, simenv.FastSettings())
	if err := env.Direct.Create(ctx, newPool("train", nil)); err != nil {
		t.Fatal(err)
	}
	w := workload(t, ctx, env, true, sim.JobSpec{Name: "job", Members: 2, GPUs: 8, Duration: time.Second, Gang: true})
	eventually(t, 60*time.Second, "job to complete", func() (bool, string) { return w.Done(), fmt.Sprint(w.Records()) })
	if f := w.Records()[0].Fabrics; len(f) != 1 || f[0] != "fab-b" {
		t.Fatalf("ran in %v", f)
	}
	if l := env.Cloud.Ledger(); l.ICERejections == 0 {
		t.Fatalf("expected the first attempt in fab-a (best fit) to be rejected: %+v", l)
	}
}

func TestStuckLaunchIsReplacedAfterRegistrationTTL(t *testing.T) {
	env, ctx := start(t, map[string]int32{"fab-a": 10}, func(o *simenv.Options) { o.Cloud.Faults.StuckRate = 1 })
	s := simenv.FastSettings()
	s.RegistrationTTL = config.D(3 * time.Second)
	manager(t, ctx, env, s)
	if err := env.Direct.Create(ctx, newPool("train", nil)); err != nil {
		t.Fatal(err)
	}
	w := workload(t, ctx, env, true, sim.JobSpec{Name: "job", Members: 1, GPUs: 8, Duration: time.Second})
	time.Sleep(2 * time.Second)
	env.Cloud.SetFaults(sim.Faults{})
	eventually(t, 60*time.Second, "job to complete", func() (bool, string) { return w.Done(), fmt.Sprint(w.Records()) })
	eventually(t, 20*time.Second, "stuck instance to be deleted", func() (bool, string) {
		live := env.Cloud.LiveInstances()
		cs := claims(t, ctx, env)
		return len(live) == len(cs), fmt.Sprintf("%d live instances, %d claims", len(live), len(cs))
	})
	if l := env.Cloud.Ledger(); l.Instances < 2 || l.DeleteCalls < 1 {
		t.Fatalf("ledger %+v", l)
	}
}

func TestEmptyNodesAreConsolidatedDownToMinNodes(t *testing.T) {
	env, ctx := start(t, map[string]int32{"fab-a": 10}, nil)
	manager(t, ctx, env, simenv.FastSettings())
	pool := newPool("train", func(p *v1alpha1.GPUNodePool) {
		p.Spec.Limits.MinNodes = 1
		p.Spec.Disruption.ConsolidateAfter = metav1.Duration{Duration: time.Second}
		p.Spec.Disruption.MaxUnavailable = ptr.To(intstr.FromInt32(10))
	})
	if err := env.Direct.Create(ctx, pool); err != nil {
		t.Fatal(err)
	}
	w := workload(t, ctx, env, true, sim.JobSpec{Name: "batch", Members: 3, GPUs: 8, Duration: 2 * time.Second})
	eventually(t, 60*time.Second, "job to complete", func() (bool, string) { return w.Done(), "" })
	eventually(t, 30*time.Second, "scale down to minNodes", func() (bool, string) {
		cs := claims(t, ctx, env)
		return len(cs) == 1, fmt.Sprintf("%d claims", len(cs))
	})
	time.Sleep(3 * time.Second)
	if cs := claims(t, ctx, env); len(cs) != 1 {
		t.Fatalf("min nodes not kept: %d claims", len(cs))
	}
}

func TestDriftReplacesNodesButWaitsForRunningGangs(t *testing.T) {
	env, ctx := start(t, map[string]int32{"fab-a": 10}, nil)
	manager(t, ctx, env, simenv.FastSettings())
	if err := env.Direct.Create(ctx, newPool("train", nil)); err != nil {
		t.Fatal(err)
	}
	w := workload(t, ctx, env, true,
		sim.JobSpec{Name: "gang", Members: 2, GPUs: 8, Duration: 8 * time.Second, Gang: true},
		sim.JobSpec{Name: "serve", Members: 1, GPUs: 8, Duration: 20 * time.Second},
	)
	eventually(t, 60*time.Second, "both jobs running", func() (bool, string) {
		rs := w.Records()
		return !rs[0].Started.IsZero() && !rs[1].Started.IsZero(), ""
	})
	pool := &v1alpha1.GPUNodePool{}
	_ = env.Direct.Get(ctx, client.ObjectKey{Name: "train"}, pool)
	pool.Spec.Image = "ubuntu24-cuda12-v2"
	if err := env.Direct.Update(ctx, pool); err != nil {
		t.Fatal(err)
	}
	hash := pool.TemplateHash()
	eventually(t, 90*time.Second, "every node on the new image", func() (bool, string) {
		cs := claims(t, ctx, env)
		old := 0
		for _, c := range cs {
			if c.Spec.TemplateHash != hash {
				old++
			}
		}
		return old == 0 && w.Done(), fmt.Sprintf("%d of %d claims on the old template; done=%v", old, len(cs), w.Done())
	})
	rs := w.Records()
	if rs[0].Restarts != 0 {
		t.Fatalf("the gang was disrupted by the upgrade: %d restarts", rs[0].Restarts)
	}
	if rs[1].Evictions == 0 {
		t.Fatalf("the single pod should have been evicted to its replacement node")
	}
}

func TestPodDisruptionBudgetBlocksDrain(t *testing.T) {
	env, ctx := start(t, map[string]int32{"fab-a": 10}, nil)
	manager(t, ctx, env, simenv.FastSettings())
	if err := env.Direct.Create(ctx, newPool("train", nil)); err != nil {
		t.Fatal(err)
	}
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "serve", Namespace: "default"},
		Spec:       policyv1.PodDisruptionBudgetSpec{MinAvailable: ptr.To(intstr.FromInt32(1)), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{sim.LabelJob: "serve"}}},
	}
	if err := env.Direct.Create(ctx, pdb); err != nil {
		t.Fatal(err)
	}
	w := workload(t, ctx, env, true, sim.JobSpec{Name: "serve", Members: 1, GPUs: 8, Duration: time.Hour})
	eventually(t, 60*time.Second, "serving pod running", func() (bool, string) { return !w.Records()[0].Started.IsZero(), "" })
	pool := &v1alpha1.GPUNodePool{}
	_ = env.Direct.Get(ctx, client.ObjectKey{Name: "train"}, pool)
	pool.Spec.Image = "v2"
	if err := env.Direct.Update(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// No disruption controller runs in this API server, so the PDB never reports allowed
	// disruptions: evictions must keep being refused and the pod must keep running.
	time.Sleep(8 * time.Second)
	if r := w.Records()[0]; r.Evictions != 0 {
		t.Fatalf("pod evicted despite the PDB: %+v", r)
	}
	// Now the budget allows one disruption.
	_ = env.Direct.Get(ctx, client.ObjectKeyFromObject(pdb), pdb)
	pdb.Status = policyv1.PodDisruptionBudgetStatus{ObservedGeneration: pdb.Generation, DisruptionsAllowed: 1, CurrentHealthy: 1, DesiredHealthy: 1, ExpectedPods: 1}
	if err := env.Direct.Status().Update(ctx, pdb); err != nil {
		t.Fatal(err)
	}
	eventually(t, 60*time.Second, "pod moved to the new node", func() (bool, string) {
		return w.Records()[0].Evictions >= 1, fmt.Sprint(w.Records()[0])
	})
}

func TestGCDeletesInstancesNoClaimOwns(t *testing.T) {
	env, ctx := start(t, map[string]int32{"fab-a": 10}, nil)
	m := manager(t, ctx, env, simenv.FastSettings())
	in, err := env.Cloud.Create(ctx, cloud.CreateRequest{ClaimName: "ghost", Fabric: "fab-a", InstanceType: simenv.H100.Name, Image: "x"})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "leaked instance deleted", func() (bool, string) {
		_, err := env.Cloud.Get(ctx, in.ID)
		return cloud.IsNotFound(err), fmt.Sprint(err)
	})
	_ = m
}

func TestAPIValidation(t *testing.T) {
	env, ctx := start(t, map[string]int32{"fab-a": 1}, func(o *simenv.Options) { o.NoKubelet, o.NoScheduler = true, true })
	bad := newPool("bad", func(p *v1alpha1.GPUNodePool) {
		p.Spec.Limits = v1alpha1.PoolLimits{MinNodes: 5, MaxNodes: ptr.To(int32(2))}
	})
	if err := env.Direct.Create(ctx, bad); err == nil || !strings.Contains(err.Error(), "minNodes must not exceed") {
		t.Fatalf("want CEL rejection, got %v", err)
	}
	good := newPool("good", nil)
	if err := env.Direct.Create(ctx, good); err != nil {
		t.Fatal(err)
	}
	if good.Spec.Disruption.MaxSurge != 1 || good.Spec.Disruption.ConsolidateAfter.Duration != time.Hour {
		t.Fatalf("defaults: %+v", good.Spec.Disruption)
	}
	claim := &v1alpha1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "c1"}, Spec: v1alpha1.NodeClaimSpec{
		Pool: "good", InstanceType: simenv.H100.Name, Fabric: "fab-a", Image: "x", TemplateHash: "h"}}
	if err := env.Direct.Create(ctx, claim); err != nil {
		t.Fatal(err)
	}
	claim.Spec.Fabric = "fab-b"
	if err := env.Direct.Update(ctx, claim); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("want immutable spec, got %v", err)
	}
}

func TestLeaderFailoverMidProvisioning(t *testing.T) {
	env, ctx := start(t, map[string]int32{"fab-a": 20}, nil)
	s := simenv.FastSettings()
	a, err := env.StartManager(ctx, simenv.ManagerOptions{Settings: s, LeaderElection: true, ID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	<-a.Elected()
	b, err := env.StartManager(ctx, simenv.ManagerOptions{Settings: s, LeaderElection: true, ID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Stop)
	if err := env.Direct.Create(ctx, newPool("train", nil)); err != nil {
		t.Fatal(err)
	}
	w := workload(t, ctx, env, true, sim.JobSpec{Name: "llm", Members: 8, GPUs: 8, Duration: 2 * time.Second, Gang: true})
	eventually(t, 30*time.Second, "first claims", func() (bool, string) { return len(claims(t, ctx, env)) > 0, "" })
	a.Stop() // crash: the lease is not released
	select {
	case <-b.Elected():
	case <-time.After(30 * time.Second):
		t.Fatal("standby never took over")
	}
	eventually(t, 90*time.Second, "job to complete under the new leader", func() (bool, string) { return w.Done(), fmt.Sprint(w.Records()) })
	l := env.Cloud.Ledger()
	if l.DuplicateClaims != 0 || l.Instances != 8 {
		t.Fatalf("failover bought servers twice: %+v", l)
	}
}
