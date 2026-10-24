package drill

import (
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/sim"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/simenv"
)

// SplitSlowdown is the assumed slowdown of a gang that runs across fabrics: its collective
// operations go over the datacenter network instead of InfiniBand. It is an assumption, stated
// in the report; with 1 the naive baseline is charged nothing for splitting gangs.
const SplitSlowdown = 3.0

// Mode is who decides what to buy.
type Mode string

const (
	Provisioner Mode = "provisioner"
	Naive       Mode = "naive"
)

// Result is what a drill run measured.
type Result struct {
	Scenario string `json:"scenario"`
	Mode     Mode   `json:"mode"`
	TimedOut bool   `json:"timedOut"`

	Jobs           int `json:"jobs"`
	Completed      int `json:"completed"`
	Gangs          int `json:"gangs"`
	GangsCompleted int `json:"gangsCompleted"`
	SplitGangs     int `json:"splitGangs"`
	GangRestarts   int `json:"gangRestarts"`
	Evictions      int `json:"evictions"`

	MakespanSeconds    float64 `json:"makespanSeconds"`
	GangWaitP50Seconds float64 `json:"gangWaitP50Seconds"`
	GangWaitP95Seconds float64 `json:"gangWaitP95Seconds"`

	// From the cloud's ledger and GPU samples, until the last job finished.
	Instances          int     `json:"instances"`
	PeakInstances      int     `json:"peakInstances"`
	PaidGPUSeconds     float64 `json:"paidGPUSeconds"`
	UsefulGPUSeconds   float64 `json:"usefulGPUSeconds"`
	StrandedGPUSeconds float64 `json:"strandedGPUSeconds"`
	SplitGPUSeconds    float64 `json:"splitGPUSeconds"`
	Utilization        float64 `json:"utilization"`
	ICERejections      int     `json:"iceRejections"`
	QuotaRejections    int     `json:"quotaRejections"`
	CreateCalls        int     `json:"createCalls"`

	// After the workload.
	ScaleInSeconds   float64 `json:"scaleInSeconds"`
	FinalInstances   int     `json:"finalInstances"`
	LeakedInstances  int     `json:"leakedInstances"`
	DuplicateClaims  int     `json:"duplicateClaims"`
	OldTemplateNodes int     `json:"oldTemplateNodes"`
	RolloutSeconds   float64 `json:"rolloutSeconds,omitempty"`
	PDBViolations    int     `json:"pdbViolations"`
	MaxNodes         int     `json:"maxNodes"`

	APIRequests int      `json:"apiRequests"`
	APIWrites   int      `json:"apiWrites"`
	TopRequests []string `json:"topRequests"`
	Events      []string `json:"events"`
	Incomplete  []string `json:"incomplete,omitempty"`
}

// Harness is a running drill, handed to scenario events.
type Harness struct {
	Env      *simenv.Env
	Scenario Scenario
	Mode     Mode
	mu       sync.Mutex
	managers []*simenv.Manager
	retired  []*simenv.Manager
	nextID   int
	events   []string
	start    time.Time
	log      io.Writer
}

func (h *Harness) note(format string, args ...any) {
	msg := fmt.Sprintf("%5.1fs ", time.Since(h.start).Seconds()) + fmt.Sprintf(format, args...)
	h.mu.Lock()
	h.events = append(h.events, msg)
	h.mu.Unlock()
	if h.log != nil {
		_, _ = fmt.Fprintf(h.log, "  [%s/%s] %s\n", h.Scenario.Name, h.Mode, msg)
	}
}

func (h *Harness) startManager(ctx context.Context) (*simenv.Manager, error) {
	s := simenv.FastSettings()
	if h.Scenario.Settings != nil {
		h.Scenario.Settings(&s)
	}
	h.nextID++
	m, err := h.Env.StartManager(ctx, simenv.ManagerOptions{
		Settings: s, Naive: h.Mode == Naive, NaiveSeed: 7,
		LeaderElection: h.Scenario.Replicas > 1, ID: fmt.Sprintf("m%d", h.nextID),
	})
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.managers = append(h.managers, m)
	h.mu.Unlock()
	return m, nil
}

// KillLeader stops the current leader without releasing its lease and starts a new standby.
func (h *Harness) KillLeader(ctx context.Context) error {
	h.mu.Lock()
	var leader *simenv.Manager
	idx := -1
	for i, m := range h.managers {
		select {
		case <-m.Elected():
			leader, idx = m, i
		default:
		}
		if leader != nil {
			break
		}
	}
	if leader != nil {
		h.managers = append(h.managers[:idx], h.managers[idx+1:]...)
		h.retired = append(h.retired, leader)
	}
	h.mu.Unlock()
	if leader == nil {
		h.note("no leader to kill")
		return nil
	}
	leader.Stop()
	h.note("leader killed")
	_, err := h.startManager(ctx)
	return err
}

// UpdatePool changes the pool.
func (h *Harness) UpdatePool(ctx context.Context, f func(*v1alpha1.GPUNodePool)) error {
	p := &v1alpha1.GPUNodePool{}
	if err := h.Env.Direct.Get(ctx, client.ObjectKey{Name: "train"}, p); err != nil {
		return err
	}
	f(p)
	return h.Env.Direct.Update(ctx, p)
}

func pool(sc Scenario) *v1alpha1.GPUNodePool {
	p := &v1alpha1.GPUNodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "train"},
		Spec: v1alpha1.GPUNodePoolSpec{
			InstanceType: simenv.H100.Name, Image: "ubuntu24-cuda12-v1",
			Taints: []corev1.Taint{{Key: "nvidia.com/gpu", Effect: corev1.TaintEffectNoSchedule}},
			Disruption: v1alpha1.Disruption{
				ConsolidateAfter: metav1.Duration{Duration: 3 * time.Second}, MaxSurge: 1,
				MaxUnavailable: ptr.To(intstr.FromString("20%")), GangPolicy: v1alpha1.GangWaitForCompletion,
			},
		},
	}
	if sc.Pool != nil {
		sc.Pool(p)
	}
	return p
}

// Run executes one scenario in one mode.
func Run(ctx context.Context, sc Scenario, mode Mode, log io.Writer) (*Result, error) {
	cc := simenv.FastCloud(sc.Hosts)
	cc.Faults = sc.Faults
	if sc.Quota > 0 {
		cc.GPUQuota = sc.Quota
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	env, err := simenv.Start(ctx, simenv.Options{Cloud: cc, Kubelet: simenv.FastKubelet()})
	if err != nil {
		return nil, err
	}
	defer func() { _ = env.Stop() }()
	h := &Harness{Env: env, Scenario: sc, Mode: mode, start: time.Now(), log: log}
	go func() { _ = (&sim.PDBController{Client: env.Client, Tick: 200 * time.Millisecond}).Start(ctx) }()
	replicas := sc.Replicas
	if replicas < 1 {
		replicas = 1
	}
	for i := 0; i < replicas; i++ {
		m, err := h.startManager(ctx)
		if err != nil {
			return nil, err
		}
		if i == 0 && replicas > 1 {
			<-m.Elected()
		}
	}
	defer func() {
		h.mu.Lock()
		ms := append([]*simenv.Manager(nil), h.managers...)
		h.mu.Unlock()
		for _, m := range ms {
			m.Stop()
		}
	}()
	p := pool(sc)
	if err := env.Direct.Create(ctx, p); err != nil {
		return nil, err
	}
	if sc.PDB != nil {
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: sc.PDB.Job, Namespace: "default"},
			Spec: policyv1.PodDisruptionBudgetSpec{MinAvailable: ptr.To(intstr.FromInt32(sc.PDB.MinAvailable)),
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{sim.LabelJob: sc.PDB.Job}}},
		}
		if err := env.Direct.Create(ctx, pdb); err != nil {
			return nil, err
		}
	}
	jobs := make([]sim.JobSpec, len(sc.Jobs))
	copy(jobs, sc.Jobs)
	for i := range jobs {
		jobs[i].Namespace = "default"
	}
	w := &sim.Workload{Client: env.Client, Cloud: env.Cloud, Jobs: jobs, Gate: mode == Provisioner,
		Tolerations: []corev1.Toleration{{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists}}, Tick: 100 * time.Millisecond,
		SplitSlowdown: SplitSlowdown}
	go func() { _ = w.Start(ctx) }()

	var rolloutStart, rolloutEnd time.Time
	for _, ev := range sc.Events {

		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(ev.At):
			}
			h.note("%s", ev.Name)
			t := time.Now()
			if err := ev.Do(ctx, h); err != nil {
				h.note("%s failed: %v", ev.Name, err)
				return
			}
			if ev.Name == "new node image" {
				h.mu.Lock()
				rolloutStart = t
				h.mu.Unlock()
			}
		}()
	}

	res := &Result{Scenario: sc.Name, Mode: mode, Jobs: len(jobs)}
	pdbHealthy := false
	maxNodes := 0
	deadline := time.Now().Add(sc.Timeout)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	var doneAt time.Time
	for {
		<-tick.C
		if n := len(env.Cloud.LiveInstances()); n > maxNodes {
			maxNodes = n
		}
		if sc.PDB != nil {
			// A violation is fewer healthy pods than minAvailable while the service has all its
			// replicas and none has finished on its own: only disruption can explain it.
			sel := labels.SelectorFromSet(map[string]string{sim.LabelJob: sc.PDB.Job})
			healthy, _ := sim.Healthy(ctx, env.Client, "default", sel)
			finished := anySucceeded(ctx, env, sc.PDB.Job)
			if healthy >= sc.PDB.MinAvailable {
				pdbHealthy = true
			} else if pdbHealthy && !finished {
				res.PDBViolations++
			}
		}
		drifted, measured := 0, time.Now()
		rolloutSeen := false
		if sc.UntilNoDrift {
			drifted = h.oldTemplate(ctx)
			h.mu.Lock()
			// Only a measurement taken after the image change counts.
			rolloutSeen = !rolloutStart.IsZero() && measured.After(rolloutStart)
			if rolloutSeen && drifted == 0 && rolloutEnd.IsZero() {
				rolloutEnd = time.Now()
			}
			h.mu.Unlock()
		}
		if w.Done() && doneAt.IsZero() {
			doneAt = time.Now()
		}
		if !doneAt.IsZero() && (!sc.UntilNoDrift || (drifted == 0 && rolloutSeen)) {
			break
		}
		if time.Now().After(deadline) {
			res.TimedOut = true
			if doneAt.IsZero() {
				doneAt = time.Now()
			}
			break
		}
	}
	h.note("workload finished (timed out: %v)", res.TimedOut)
	res.MaxNodes = maxNodes
	if !rolloutStart.IsZero() && !rolloutEnd.IsZero() {
		res.RolloutSeconds = rolloutEnd.Sub(rolloutStart).Seconds()
	}
	recs := w.Records()
	h.score(res, recs, w.Samples(), doneAt)
	ledger := env.Cloud.Ledger()
	res.Instances, res.PeakInstances = ledger.Instances, ledger.PeakInstances
	res.ICERejections, res.QuotaRejections, res.CreateCalls = ledger.ICERejections, ledger.QuotaRejections, ledger.CreateCalls

	// Scale-in: how long until only minNodes remain, then look for leaks.
	scaleStart := time.Now()
	minNodes := int(p.Spec.Limits.MinNodes)
	for time.Since(scaleStart) < 45*time.Second && !res.TimedOut {
		if len(env.Cloud.LiveInstances()) <= minNodes {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	res.ScaleInSeconds = time.Since(scaleStart).Seconds()
	time.Sleep(4 * time.Second) // past GC grace, so a leak would have been collected
	res.FinalInstances = len(env.Cloud.LiveInstances())
	res.LeakedInstances = h.leaks(ctx)
	res.DuplicateClaims = env.Cloud.Ledger().DuplicateClaims
	res.OldTemplateNodes = h.oldTemplate(ctx)
	h.mu.Lock()
	all := append(append([]*simenv.Manager(nil), h.managers...), h.retired...)
	h.mu.Unlock()
	agg := map[string]int{}
	for _, m := range all {
		for k, v := range m.Requests.Snapshot() {
			agg[k] += v
			res.APIRequests += v
		}
		res.APIWrites += m.Requests.Writes()
	}
	res.TopRequests = top(agg, 6)
	h.mu.Lock()
	res.Events = append(res.Events, h.events...)
	h.mu.Unlock()
	return res, nil
}

func anySucceeded(ctx context.Context, env *simenv.Env, job string) bool {
	var pods corev1.PodList
	if err := env.Client.List(ctx, &pods, client.MatchingLabels{sim.LabelJob: job}); err != nil {
		return false
	}
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodSucceeded {
			return true
		}
	}
	return false
}

func (h *Harness) oldTemplate(ctx context.Context) int {
	p := &v1alpha1.GPUNodePool{}
	if err := h.Env.Direct.Get(ctx, client.ObjectKey{Name: "train"}, p); err != nil {
		return -1
	}
	var cs v1alpha1.NodeClaimList
	if err := h.Env.Direct.List(ctx, &cs); err != nil {
		return -1
	}
	hash, n := p.TemplateHash(), 0
	for _, c := range cs.Items {
		if c.Spec.TemplateHash != hash {
			n++
		}
	}
	return n
}

// leaks counts instances that exist in the cloud without a claim that points at them.
func (h *Harness) leaks(ctx context.Context) int {
	var cs v1alpha1.NodeClaimList
	if err := h.Env.Direct.List(ctx, &cs); err != nil {
		return -1
	}
	owned := map[string]bool{}
	for _, c := range cs.Items {
		owned[c.Name] = true
	}
	n := 0
	for claim, count := range h.Env.Cloud.LiveInstances() {
		if !owned[claim] {
			n += count
		} else if count > 1 {
			n += count - 1
		}
	}
	return n
}

func (h *Harness) score(res *Result, recs []sim.JobRecord, samples []sim.Sample, doneAt time.Time) {
	var waits []float64
	var first, last time.Time
	for _, r := range recs {
		if !r.Completed.IsZero() {
			res.Completed++
		} else {
			state := "waiting"
			if !r.Started.IsZero() {
				state = "running"
			}
			res.Incomplete = append(res.Incomplete, fmt.Sprintf("%s (%s)", r.Spec.Name, state))
		}
		if first.IsZero() || (!r.Created.IsZero() && r.Created.Before(first)) {
			first = r.Created
		}
		if r.Completed.After(last) {
			last = r.Completed
		}
		res.Evictions += r.Evictions
		if !r.Spec.Gang {
			continue
		}
		res.Gangs++
		res.GangRestarts += r.Restarts
		if !r.Completed.IsZero() {
			res.GangsCompleted++
		}
		if len(r.Fabrics) > 1 {
			res.SplitGangs++
		}
		if !r.Started.IsZero() {
			waits = append(waits, r.Started.Sub(r.Created).Seconds())
		}
	}
	if !last.IsZero() {
		res.MakespanSeconds = last.Sub(first).Seconds()
	}
	res.GangWaitP50Seconds = percentile(waits, 0.5)
	res.GangWaitP95Seconds = percentile(waits, 0.95)
	end := doneAt.Sub(h.start)
	var prev time.Duration
	for _, s := range samples {
		if s.At > end {
			break
		}
		dt := (s.At - prev).Seconds()
		prev = s.At
		res.PaidGPUSeconds += float64(s.CloudGPUs) * dt
		res.UsefulGPUSeconds += float64(s.RunningGPUs) * dt
		res.StrandedGPUSeconds += float64(s.StrandedGPUs) * dt
		res.SplitGPUSeconds += float64(s.SplitGPUs) * dt
	}
	if res.PaidGPUSeconds > 0 {
		res.Utilization = res.UsefulGPUSeconds / res.PaidGPUSeconds
	}
	res.PaidGPUSeconds = math.Round(res.PaidGPUSeconds)
	res.UsefulGPUSeconds = math.Round(res.UsefulGPUSeconds)
	res.StrandedGPUSeconds = math.Round(res.StrandedGPUSeconds)
	res.SplitGPUSeconds = math.Round(res.SplitGPUSeconds)
}

func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := int(math.Ceil(p*float64(len(s)))) - 1
	if i < 0 {
		i = 0
	}
	return math.Round(s[i]*10) / 10
}

func top(m map[string]int, n int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return m[keys[i]] > m[keys[j]] })
	if len(keys) > n {
		keys = keys[:n]
	}
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = fmt.Sprintf("%s=%d", k, m[k])
	}
	return out
}
