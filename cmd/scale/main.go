// Command scale measures the control plane at size: how fast one manager brings N GPU nodes
// from nothing to initialized through a real API server, how many API writes each node costs,
// how long a plan takes with N nodes in the cache, and how fast a gang is released at that size.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/config"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/podutil"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/simenv"
)

// Result is one size.
type Result struct {
	Nodes             int            `json:"nodes"`
	Fabrics           int            `json:"fabrics"`
	KubeQPS           float32        `json:"kubeQPS"`
	SecondsTo10Pct    float64        `json:"secondsTo10Pct"`
	SecondsTo50Pct    float64        `json:"secondsTo50Pct"`
	SecondsToAll      float64        `json:"secondsToAll"`
	NodesPerMinute    float64        `json:"nodesPerMinute"`
	Initialized       int            `json:"initialized"`
	TimedOut          bool           `json:"timedOut"`
	ManagerRequests   int            `json:"managerRequests"`
	ManagerWrites     int            `json:"managerWrites"`
	WritesPerNode     float64        `json:"writesPerNode"`
	WritesByKind      map[string]int `json:"writesByKind"`
	PlanMillisP50     float64        `json:"planMillisP50"`
	PlanMillisMax     float64        `json:"planMillisMax"`
	GangPods          int            `json:"gangPods"`
	GangReleaseMillis float64        `json:"gangReleaseMillis"`
	HeapMiB           float64        `json:"heapMiB"`
	CloudCreateCalls  int            `json:"cloudCreateCalls"`
	DuplicateClaims   int            `json:"duplicateClaims"`
}

func main() {
	sizes := flag.String("nodes", "1000", "comma-separated cluster sizes")
	fabrics := flag.Int("fabrics", 10, "fabrics the nodes spread over")
	qps := flag.Float64("qps", 200, "manager API client QPS (production default 200)")
	burst := flag.Int("burst", 400, "manager API client burst")
	gang := flag.Int("gang", 64, "members of the gang released at the end")
	timeout := flag.Duration("timeout", 45*time.Minute, "per size")
	out := flag.String("out", "reports/scale.json", "results")
	flag.Parse()
	ctrllog.SetLogger(logr.Discard())
	var results []Result
	for _, s := range strings.Split(*sizes, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			fail(err)
		}
		r, err := run(n, *fabrics, float32(*qps), *burst, *gang, *timeout)
		if err != nil {
			fail(err)
		}
		fmt.Fprintf(os.Stderr, "%d nodes: all initialized in %.0fs (%.0f nodes/min), %.2f writes/node, plan p50 %.0f ms, gang of %d released in %.0f ms, heap %.0f MiB\n",
			r.Nodes, r.SecondsToAll, r.NodesPerMinute, r.WritesPerNode, r.PlanMillisP50, r.GangPods, r.GangReleaseMillis, r.HeapMiB)
		results = append(results, r)
		_ = os.MkdirAll(filepath.Dir(*out), 0o755)
		b, _ := json.MarshalIndent(results, "", "  ")
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			fail(err)
		}
	}
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "::error::%v\n", err)
	os.Exit(1)
}

func run(n, fabrics int, qps float32, burst, gang int, timeout time.Duration) (Result, error) {
	// The gang must fit in one fabric, next to that fabric's share of the nodes.
	gang = min(gang, n/fabrics/2)
	res := Result{Nodes: n, Fabrics: fabrics, KubeQPS: qps, GangPods: gang}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hosts := map[string]int32{}
	per := int32((n+gang)*12/10/fabrics + 1)
	for i := 0; i < fabrics; i++ {
		hosts[fmt.Sprintf("fab-%02d", i)] = per
	}
	cc := simenv.FastCloud(hosts)
	cc.LaunchDelayMin, cc.LaunchDelayMax = 500*time.Millisecond, 1500*time.Millisecond
	kc := simenv.FastKubelet()
	kc.GPUDelay, kc.Tick = 500*time.Millisecond, 500*time.Millisecond
	env, err := simenv.Start(ctx, simenv.Options{Cloud: cc, Kubelet: kc, NoScheduler: true})
	if err != nil {
		return res, err
	}
	defer func() { _ = env.Stop() }()
	s := simenv.FastSettings()
	s.KubeQPS, s.KubeBurst = qps, burst
	s.DisruptionInterval = config.D(10 * time.Second)
	s.GCInterval = config.D(time.Minute)
	s.PoolStatusInterval = config.D(5 * time.Second)
	s.RegistrationTTL = config.D(30 * time.Minute)
	s.InitializationTTL = config.D(30 * time.Minute)
	m, err := env.StartManager(ctx, simenv.ManagerOptions{Settings: s})
	if err != nil {
		return res, err
	}
	defer m.Stop()

	pool := &v1alpha1.GPUNodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "train"},
		Spec: v1alpha1.GPUNodePoolSpec{
			InstanceType: simenv.H100.Name, Image: "v1",
			Limits:     v1alpha1.PoolLimits{MinNodes: int32(n), MaxNodes: ptr.To(int32(n + gang))},
			Disruption: v1alpha1.Disruption{Consolidation: ptr.To(false), MaxSurge: 1},
		},
	}
	start := time.Now()
	if err := env.Direct.Create(ctx, pool); err != nil {
		return res, err
	}
	deadline := start.Add(timeout)
	for {
		time.Sleep(time.Second)
		var cs v1alpha1.NodeClaimList
		if err := env.Client.List(ctx, &cs, client.UnsafeDisableDeepCopy); err != nil {
			return res, err
		}
		init := 0
		phases := map[v1alpha1.NodeClaimPhase]int{}
		for i := range cs.Items {
			phases[cs.Items[i].Status.Phase]++
			if cs.Items[i].Status.Phase == v1alpha1.PhaseInitialized {
				init++
			}
		}
		el := time.Since(start).Seconds()
		if int(el)%10 == 0 {
			var nodes corev1.NodeList
			_ = env.Client.List(ctx, &nodes, client.UnsafeDisableDeepCopy)
			fmt.Fprintf(os.Stderr, "  %4.0fs claims %d %v nodes %d\n", el, len(cs.Items), phases, len(nodes.Items))
		}
		if res.SecondsTo10Pct == 0 && init*10 >= n {
			res.SecondsTo10Pct = el
		}
		if res.SecondsTo50Pct == 0 && init*2 >= n {
			res.SecondsTo50Pct = el
		}
		res.Initialized = init
		if init >= n {
			res.SecondsToAll = el
			break
		}
		if time.Now().After(deadline) {
			res.TimedOut = true
			res.SecondsToAll = el
			break
		}
	}
	res.NodesPerMinute = float64(res.Initialized) / res.SecondsToAll * 60
	snap := m.Requests.Snapshot()
	res.WritesByKind = map[string]int{}
	for k, v := range snap {
		res.ManagerRequests += v
		if !strings.HasPrefix(k, "get ") && !strings.HasPrefix(k, "watch ") {
			res.ManagerWrites += v
			res.WritesByKind[k] = v
		}
	}
	res.WritesPerNode = float64(res.ManagerWrites) / float64(max(1, res.Initialized))

	// A plan with every node in the cache.
	var plans []float64
	for i := 0; i < 5; i++ {
		t := time.Now()
		if _, err := m.Components.Provisioner.Once(ctx); err != nil {
			return res, err
		}
		plans = append(plans, float64(time.Since(t).Microseconds())/1000)
	}
	sort.Float64s(plans)
	res.PlanMillisP50, res.PlanMillisMax = plans[len(plans)/2], plans[len(plans)-1]

	// A gang arrives: it fits on initialized empty nodes, so it should be released at once.
	gpu := *resource.NewQuantity(8, resource.DecimalSI)
	t := time.Now()
	for i := 0; i < gang; i++ {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("big-%d", i), Namespace: "default",
				Labels: map[string]string{v1alpha1.LabelGang: "big"}, Annotations: map[string]string{v1alpha1.AnnotationGangSize: strconv.Itoa(gang)}},
			Spec: corev1.PodSpec{
				Containers:      []corev1.Container{{Name: "c", Image: "x", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{v1alpha1.ResourceGPU: gpu}, Limits: corev1.ResourceList{v1alpha1.ResourceGPU: gpu}}}},
				SchedulingGates: []corev1.PodSchedulingGate{{Name: v1alpha1.SchedulingGate}},
			},
		}
		if err := env.Direct.Create(ctx, p); err != nil {
			return res, err
		}
	}
	for {
		var pods corev1.PodList
		if err := env.Client.List(ctx, &pods, client.MatchingLabels{v1alpha1.LabelGang: "big"}); err != nil {
			return res, err
		}
		released := 0
		for i := range pods.Items {
			if !podutil.Gated(&pods.Items[i]) {
				released++
			}
		}
		if released == gang {
			res.GangReleaseMillis = float64(time.Since(t).Milliseconds())
			break
		}
		if time.Since(t) > 2*time.Minute {
			return res, fmt.Errorf("gang of %d not released at %d nodes (%d released)", gang, n, released)
		}
		time.Sleep(50 * time.Millisecond)
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	res.HeapMiB = float64(ms.HeapInuse) / (1 << 20)
	l := env.Cloud.Ledger()
	res.CloudCreateCalls, res.DuplicateClaims = l.CreateCalls, l.DuplicateClaims
	return res, nil
}
