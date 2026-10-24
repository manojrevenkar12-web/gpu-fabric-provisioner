package sim

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
)

// LabelJob ties simulated pods to their job.
const LabelJob = "sim.gpufabric.dev/job"

// JobSpec is a simulated training or batch job.
type JobSpec struct {
	Name      string        `json:"name"`
	Namespace string        `json:"namespace"`
	Members   int           `json:"members"`
	GPUs      int32         `json:"gpus"`
	Duration  time.Duration `json:"duration"`
	Priority  int32         `json:"priority"`
	// Gang: all members must run together, in one fabric (a distributed training job). Otherwise
	// the members are independent pods (inference replicas, single-GPU experiments) that a
	// controller recreates when evicted.
	Gang bool          `json:"gang"`
	At   time.Duration `json:"at"`
}

// JobRecord is what happened to a job.
type JobRecord struct {
	Spec      JobSpec
	Created   time.Time
	Started   time.Time // all members running (gang) or first member running
	Completed time.Time
	Fabrics   []string // fabrics the gang ran in when it started
	Restarts  int
	Evictions int
}

// Sample is one observation of GPU use.
type Sample struct {
	At           time.Duration
	CloudGPUs    int64 // GPUs of every instance that exists
	RunningGPUs  int64 // GPUs requested by running pods doing useful work
	StrandedGPUs int64 // GPUs held by members of gangs that are not fully running
}

// Workload creates jobs' pods, completes them after their duration, restarts gangs that lost a
// member and recreates evicted independent pods.
type Workload struct {
	Client client.Client
	Cloud  *Cloud
	Jobs   []JobSpec
	// Gate adds the placement scheduling gate to gang pods, as the admission webhook does.
	Gate        bool
	Tolerations []corev1.Toleration
	Tick        time.Duration

	mu      sync.Mutex
	t0      time.Time
	recs    map[string]*JobRecord
	gen     map[string]int
	created map[string]bool
	samples []Sample
	seq     map[string]int
	// unseen are pods created but not yet visible in the cache; they count as alive so a lagging
	// cache does not make the workload recreate them.
	unseen map[string]unseenPod
}

type unseenPod struct {
	job string
	at  time.Time
}

// Start runs the workload until ctx ends.
func (w *Workload) Start(ctx context.Context) error {
	w.mu.Lock()
	w.t0 = time.Now()
	w.recs, w.gen, w.created, w.seq, w.unseen = map[string]*JobRecord{}, map[string]int{}, map[string]bool{}, map[string]int{}, map[string]unseenPod{}
	for _, j := range w.Jobs {
		w.recs[j.Name] = &JobRecord{Spec: j}
	}
	w.mu.Unlock()
	t := time.NewTicker(w.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			_ = w.step(ctx)
		}
	}
}

// Done reports whether every job completed.
func (w *Workload) Done() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.recs == nil {
		return false
	}
	for _, r := range w.recs {
		if r.Completed.IsZero() {
			return false
		}
	}
	return true
}

// Records returns job records sorted by name.
func (w *Workload) Records() []JobRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.recs == nil {
		out := make([]JobRecord, 0, len(w.Jobs))
		for _, j := range w.Jobs {
			out = append(out, JobRecord{Spec: j})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Spec.Name < out[j].Spec.Name })
		return out
	}
	out := make([]JobRecord, 0, len(w.recs))
	for _, r := range w.recs {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Spec.Name < out[j].Spec.Name })
	return out
}

// Samples returns GPU-use samples.
func (w *Workload) Samples() []Sample {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]Sample(nil), w.samples...)
}

// Elapsed is time since the workload started.
func (w *Workload) Elapsed() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return time.Since(w.t0)
}

func (w *Workload) pod(j JobSpec, name string) *corev1.Pod {
	q := func(s string) resource.Quantity { return resource.MustParse(s) }
	gpu := *resource.NewQuantity(int64(j.GPUs), resource.DecimalSI)
	res := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: q(fmt.Sprintf("%d", 8*j.GPUs)), corev1.ResourceMemory: q(fmt.Sprintf("%dGi", 64*j.GPUs)), v1alpha1.ResourceGPU: gpu},
		Limits:   corev1.ResourceList{v1alpha1.ResourceGPU: gpu},
	}
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: j.Namespace, Labels: map[string]string{LabelJob: j.Name},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: j.Name,
				UID: types.UID("sim-" + j.Name), Controller: ptr.To(true)}},
		},
		Spec: corev1.PodSpec{
			Containers:  []corev1.Container{{Name: "main", Image: "sim", Resources: res}},
			Tolerations: w.Tolerations,
			Priority:    ptr.To(j.Priority),
		},
	}
	if j.Gang {
		p.Labels[v1alpha1.LabelGang] = j.Name
		p.Annotations = map[string]string{v1alpha1.AnnotationGangSize: strconv.Itoa(j.Members)}
		if w.Gate {
			p.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: v1alpha1.SchedulingGate}}
		}
	}
	return p
}

func (w *Workload) createAll(ctx context.Context, j JobSpec, gen int) error {
	for i := 0; i < j.Members; i++ {
		name := fmt.Sprintf("%s-g%d-%d", j.Name, gen, i)
		if !j.Gang {
			name = fmt.Sprintf("%s-%d", j.Name, i)
		}
		if err := w.Client.Create(ctx, w.pod(j, name)); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		w.mu.Lock()
		w.unseen[name] = unseenPod{j.Name, time.Now()}
		w.mu.Unlock()
	}
	return nil
}

// pending counts pods of a job (with a name prefix) that were created but are not in the cache yet.
func (w *Workload) pendingCreates(job, prefix string, now time.Time) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for name, u := range w.unseen {
		if now.Sub(u.at) > 10*time.Second {
			delete(w.unseen, name)
			continue
		}
		if u.job == job && len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

func (w *Workload) step(ctx context.Context) error {
	now := time.Now()
	w.mu.Lock()
	elapsed := now.Sub(w.t0)
	var toCreate []JobSpec
	for _, j := range w.Jobs {
		if !w.created[j.Name] && elapsed >= j.At {
			w.created[j.Name] = true
			w.recs[j.Name].Created = now
			toCreate = append(toCreate, j)
		}
	}
	w.mu.Unlock()
	for _, j := range toCreate {
		if err := w.createAll(ctx, j, 0); err != nil {
			return err
		}
	}
	var pods corev1.PodList
	if err := w.Client.List(ctx, &pods, client.HasLabels{LabelJob}); err != nil {
		return err
	}
	var nodes corev1.NodeList
	if err := w.Client.List(ctx, &nodes); err != nil {
		return err
	}
	fabricOf := map[string]string{}
	for i := range nodes.Items {
		fabricOf[nodes.Items[i].Name] = nodes.Items[i].Labels[v1alpha1.LabelFabric]
	}
	byJob := map[string][]*corev1.Pod{}
	w.mu.Lock()
	for i := range pods.Items {
		delete(w.unseen, pods.Items[i].Name)
	}
	w.mu.Unlock()
	for i := range pods.Items {
		byJob[pods.Items[i].Labels[LabelJob]] = append(byJob[pods.Items[i].Labels[LabelJob]], &pods.Items[i])
	}
	var running, stranded int64
	for _, j := range w.Jobs {
		w.mu.Lock()
		rec, made, gen := w.recs[j.Name], w.created[j.Name], w.gen[j.Name]
		w.mu.Unlock()
		if !made || !rec.Completed.IsZero() {
			continue
		}
		if j.Gang {
			r, s := w.stepGang(ctx, j, rec, gen, byJob[j.Name], fabricOf, now)
			running += r
			stranded += s
		} else {
			running += w.stepIndependent(ctx, j, rec, byJob[j.Name], now)
		}
	}
	w.mu.Lock()
	w.samples = append(w.samples, Sample{At: elapsed, CloudGPUs: w.Cloud.GPUsInUse(), RunningGPUs: running, StrandedGPUs: stranded})
	w.mu.Unlock()
	return nil
}

func (w *Workload) stepGang(ctx context.Context, j JobSpec, rec *JobRecord, gen int, pods []*corev1.Pod, fabricOf map[string]string, now time.Time) (int64, int64) {
	prefix := fmt.Sprintf("%s-g%d-", j.Name, gen)
	var cur []*corev1.Pod
	for _, p := range pods {
		if len(p.Name) > len(prefix) && p.Name[:len(prefix)] == prefix {
			cur = append(cur, p)
		}
	}
	nRunning, alive := 0, 0
	for _, p := range cur {
		if p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodFailed {
			continue
		}
		alive++
		if p.Status.Phase == corev1.PodRunning {
			nRunning++
		}
	}
	if alive+w.pendingCreates(j.Name, prefix, now) < j.Members {
		// A member was lost (its node failed or it was evicted): the whole gang restarts, as
		// JobSet and the training operators do.
		for _, p := range cur {
			_ = w.Client.Delete(ctx, p, client.GracePeriodSeconds(0))
		}
		w.mu.Lock()
		w.gen[j.Name]++
		rec.Restarts++
		rec.Started = time.Time{}
		gen = w.gen[j.Name]
		w.mu.Unlock()
		_ = w.createAll(ctx, j, gen)
		return 0, 0
	}
	if nRunning < j.Members {
		return 0, int64(nRunning) * int64(j.GPUs)
	}
	if rec.Started.IsZero() {
		fs := map[string]bool{}
		for _, p := range cur {
			fs[fabricOf[p.Spec.NodeName]] = true
		}
		var list []string
		for f := range fs {
			list = append(list, f)
		}
		sort.Strings(list)
		w.mu.Lock()
		rec.Started, rec.Fabrics = now, list
		w.mu.Unlock()
	}
	if now.Sub(rec.Started) >= j.Duration {
		for _, p := range cur {
			p.Status.Phase = corev1.PodSucceeded
			_ = w.Client.Status().Update(ctx, p)
		}
		w.mu.Lock()
		rec.Completed = now
		w.mu.Unlock()
		return 0, 0
	}
	return int64(j.Members) * int64(j.GPUs), 0
}

func (w *Workload) stepIndependent(ctx context.Context, j JobSpec, rec *JobRecord, pods []*corev1.Pod, now time.Time) int64 {
	var running int64
	done, alive := 0, 0
	for _, p := range pods {
		switch {
		case p.Status.Phase == corev1.PodSucceeded:
			done++
		case p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodFailed:
		case p.Status.Phase == corev1.PodRunning:
			alive++
			if rec.Started.IsZero() {
				w.mu.Lock()
				rec.Started = now
				w.mu.Unlock()
			}
			if p.Status.StartTime != nil && now.Sub(p.Status.StartTime.Time) >= j.Duration {
				p.Status.Phase = corev1.PodSucceeded
				_ = w.Client.Status().Update(ctx, p)
				done++
				alive--
				continue
			}
			running += int64(j.GPUs)
		default:
			alive++
		}
	}
	if done >= j.Members {
		w.mu.Lock()
		rec.Completed = now
		w.mu.Unlock()
		return running
	}
	// Recreate evicted or lost pods, as a ReplicaSet or Job controller would.
	for missing := j.Members - done - alive - w.pendingCreates(j.Name, j.Name+"-", now); missing > 0; missing-- {
		w.mu.Lock()
		w.seq[j.Name]++
		n := w.seq[j.Name]
		rec.Evictions++
		name := fmt.Sprintf("%s-r%d", j.Name, n)
		w.unseen[name] = unseenPod{j.Name, now}
		w.mu.Unlock()
		_ = w.Client.Create(ctx, w.pod(j, name))
	}
	return running
}
