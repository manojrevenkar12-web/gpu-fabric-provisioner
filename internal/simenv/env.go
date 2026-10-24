// Package simenv starts a real Kubernetes API server (envtest: kube-apiserver and etcd), the
// simulated cloud, kubelets and scheduler, and managers running the provisioner. Integration
// tests, drills and the scale benchmark all use it.
package simenv

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/config"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/controllers"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/kube"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/naive"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/sim"
)

// Scheme has the core types and this project's API.
func Scheme() *kruntime.Scheme { return kube.Scheme() }

// RepoRoot is the module root, found from this file's location.
func RepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// Options configure an environment.
type Options struct {
	Cloud         sim.CloudConfig
	Kubelet       sim.KubeletConfig
	SchedulerTick time.Duration
	// NoKubelet and NoScheduler leave those simulators out (some tests drive nodes by hand).
	NoKubelet   bool
	NoScheduler bool
}

// Env is a running API server with the simulated cloud, kubelets and scheduler.
type Env struct {
	API       *envtest.Environment
	Config    *rest.Config
	Direct    client.Client // no cache
	Cluster   cluster.Cluster
	Client    client.Client // cached, for the simulators and assertions
	Cloud     *sim.Cloud
	Kubelet   *sim.Kubelet
	Scheduler *sim.Scheduler
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// Start brings an environment up.
func Start(ctx context.Context, o Options) (*Env, error) {
	te := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(RepoRoot(), "config", "crd")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := te.Start()
	if err != nil {
		return nil, fmt.Errorf("envtest: %w (set KUBEBUILDER_ASSETS to kube-apiserver and etcd)", err)
	}
	cfg.QPS, cfg.Burst = 2000, 4000
	e := &Env{API: te, Config: cfg}
	ctx, e.cancel = context.WithCancel(ctx)
	scheme := Scheme()
	if e.Direct, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		return nil, err
	}
	if e.Cluster, err = cluster.New(cfg, func(co *cluster.Options) {
		co.Scheme = scheme
		co.Cache.DefaultTransform = cache.TransformStripManagedFields()
	}); err != nil {
		return nil, err
	}
	e.wg.Add(1)
	go func() { defer e.wg.Done(); _ = e.Cluster.Start(ctx) }()
	if !e.Cluster.GetCache().WaitForCacheSync(ctx) {
		return nil, fmt.Errorf("sim cache did not sync")
	}
	e.Client = e.Cluster.GetClient()
	e.Cloud = sim.NewCloud(o.Cloud)
	if !o.NoKubelet {
		e.Kubelet = &sim.Kubelet{Client: e.Client, Cloud: e.Cloud, Cfg: o.Kubelet}
		e.wg.Add(1)
		go func() { defer e.wg.Done(); _ = e.Kubelet.Start(ctx) }()
	}
	if !o.NoScheduler {
		tick := o.SchedulerTick
		if tick == 0 {
			tick = 100 * time.Millisecond
		}
		e.Scheduler = &sim.Scheduler{Client: e.Client, Tick: tick}
		e.wg.Add(1)
		go func() { defer e.wg.Done(); _ = e.Scheduler.Start(ctx) }()
	}
	return e, nil
}

// Stop tears everything down.
func (e *Env) Stop() error {
	e.cancel()
	e.wg.Wait()
	return e.API.Stop()
}

// ManagerOptions configure a manager.
type ManagerOptions struct {
	Settings       config.Settings
	Naive          bool
	NaiveSeed      int64
	LeaderElection bool
	ID             string
}

// Manager is a running controller manager.
type Manager struct {
	Mgr        ctrl.Manager
	Components *controllers.Components
	Requests   *Counter
	cancel     context.CancelFunc
	done       chan error
}

// StartManager starts a controller manager running the provisioner (or the naive baseline).
func (e *Env) StartManager(ctx context.Context, o ManagerOptions) (*Manager, error) {
	cfg := rest.CopyConfig(e.Config)
	cfg.QPS, cfg.Burst = o.Settings.KubeQPS, o.Settings.KubeBurst
	counter := &Counter{byKey: map[string]int{}}
	cfg.Wrap(counter.wrap)
	opts := ctrl.Options{
		Scheme:                 Scheme(),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache:                  cache.Options{DefaultTransform: cache.TransformStripManagedFields()},
		Controller:             ctrlconfig.Controller{SkipNameValidation: ptr.To(true)},
	}
	if o.LeaderElection {
		opts.LeaderElection = true
		opts.LeaderElectionID = "gpu-fabric-provisioner"
		opts.LeaderElectionNamespace = "default"
		opts.LeaseDuration = ptr.To(3 * time.Second)
		opts.RenewDeadline = ptr.To(2 * time.Second)
		opts.RetryPeriod = ptr.To(500 * time.Millisecond)
		opts.LeaderElectionReleaseOnCancel = false
	}
	mgr, err := ctrl.NewManager(cfg, opts)
	if err != nil {
		return nil, err
	}
	mctx, cancel := context.WithCancel(ctx)
	comps, err := controllers.Setup(mctx, mgr, controllers.Options{Settings: o.Settings, Cloud: e.Cloud, NoProvisioner: o.Naive})
	if err != nil {
		cancel()
		return nil, err
	}
	if o.Naive {
		np := &naive.Provisioner{Client: mgr.GetClient(), Reader: mgr.GetCache(), Cloud: comps.Cloud, Interval: o.Settings.Resync.Duration / 3, Seed: o.NaiveSeed}
		if err := mgr.Add(np); err != nil {
			cancel()
			return nil, err
		}
	}
	m := &Manager{Mgr: mgr, Components: comps, Requests: counter, cancel: cancel, done: make(chan error, 1)}
	go func() { m.done <- mgr.Start(mctx) }()
	if !mgr.GetCache().WaitForCacheSync(mctx) {
		cancel()
		return nil, fmt.Errorf("manager cache did not sync")
	}
	return m, nil
}

// Stop stops the manager abruptly (no lease release), as a crash would.
func (m *Manager) Stop() {
	m.cancel()
	select {
	case <-m.done:
	case <-time.After(20 * time.Second):
	}
}

// Elected is closed when the manager becomes leader.
func (m *Manager) Elected() <-chan struct{} { return m.Mgr.Elected() }

// Counter counts API requests by verb and resource.
type Counter struct {
	mu    sync.Mutex
	byKey map[string]int
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func (c *Counter) wrap(rt http.RoundTripper) http.RoundTripper {
	return rtFunc(func(r *http.Request) (*http.Response, error) {
		c.mu.Lock()
		c.byKey[verb(r)+" "+resourceOf(r.URL.Path)]++
		c.mu.Unlock()
		return rt.RoundTrip(r)
	})
}

func verb(r *http.Request) string {
	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Get("watch") == "true" {
			return "watch"
		}
		return "get"
	case http.MethodPost:
		return "create"
	case http.MethodPut:
		return "update"
	case http.MethodPatch:
		return "patch"
	case http.MethodDelete:
		return "delete"
	}
	return strings.ToLower(r.Method)
}

// resourceOf turns /api/v1/namespaces/ns/pods/name/status into "pods/status".
func resourceOf(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	// skip api/v1 or apis/group/version
	if len(parts) > 0 && parts[0] == "api" {
		parts = parts[min(2, len(parts)):]
	} else if len(parts) > 0 && parts[0] == "apis" {
		parts = parts[min(3, len(parts)):]
	}
	if len(parts) >= 2 && parts[0] == "namespaces" {
		parts = parts[2:]
		if len(parts) == 0 {
			return "namespaces"
		}
	}
	switch len(parts) {
	case 0:
		return ""
	case 1, 2:
		return parts[0]
	default:
		return parts[0] + "/" + parts[2]
	}
}

// Snapshot returns counts so far.
func (c *Counter) Snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for k, v := range c.byKey {
		out[k] = v
	}
	return out
}

// Writes sums non-read requests.
func (c *Counter) Writes() int {
	n := 0
	for k, v := range c.Snapshot() {
		if !strings.HasPrefix(k, "get ") && !strings.HasPrefix(k, "watch ") {
			n += v
		}
	}
	return n
}

// Top lists the busiest keys.
func (c *Counter) Top(n int) []string {
	snap := c.Snapshot()
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return snap[keys[i]] > snap[keys[j]] })
	if len(keys) > n {
		keys = keys[:n]
	}
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = fmt.Sprintf("%s=%d", k, snap[k])
	}
	return out
}

// FastSettings are controller settings with every timing compressed for tests and drills.
func FastSettings() config.Settings {
	s := config.Defaults()
	s.BatchIdle = config.D(200 * time.Millisecond)
	s.BatchMax = config.D(1500 * time.Millisecond)
	s.Resync = config.D(2 * time.Second)
	s.RegistrationTTL = config.D(10 * time.Second)
	s.InitializationTTL = config.D(10 * time.Second)
	s.RepairAfter = config.D(4 * time.Second)
	s.LaunchBackoff = config.D(200 * time.Millisecond)
	s.UnavailableTTL = config.D(8 * time.Second)
	s.OfferingsRefresh = config.D(time.Second)
	s.DisruptionInterval = config.D(time.Second)
	s.GCInterval = config.D(2 * time.Second)
	s.GCGrace = config.D(3 * time.Second)
	return s
}

// H100 is the simulated 8-GPU instance type used throughout tests and drills.
var H100 = cloudType()

// FastCloud is a simulated cloud with three fabrics and compressed delays.
func FastCloud(hosts map[string]int32) sim.CloudConfig {
	var fabrics []sim.FabricConfig
	names := make([]string, 0, len(hosts))
	for f := range hosts {
		names = append(names, f)
	}
	sort.Strings(names)
	for _, f := range names {
		fabrics = append(fabrics, sim.FabricConfig{Name: f, Hosts: map[string]int32{H100.Name: hosts[f]}})
	}
	return sim.CloudConfig{
		Fabrics: fabrics, InstanceTypes: []cloudInstanceType{H100}, GPUQuota: 1 << 20,
		LaunchDelayMin: 1500 * time.Millisecond, LaunchDelayMax: 3 * time.Second, DeleteDelay: 500 * time.Millisecond, Seed: 1,
	}
}

// FastKubelet joins nodes quickly but advertises GPUs two seconds after registration.
func FastKubelet() sim.KubeletConfig {
	return sim.KubeletConfig{JoinDelay: 300 * time.Millisecond, GPUDelay: 2 * time.Second, PodStartDelay: 200 * time.Millisecond, Tick: 100 * time.Millisecond}
}
