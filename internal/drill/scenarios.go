// Package drill runs fault drills: a workload of gangs and single pods on the real API server,
// the simulated cloud misbehaving in one way per drill, and either this provisioner or the
// naive baseline deciding what to buy. Results are judged from the cloud's own ledger and the
// jobs' records, never from the provisioner's view of itself.
package drill

import (
	"context"
	"fmt"
	"time"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/config"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/sim"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/simenv"
)

// Event is something a drill does at a point in time.
type Event struct {
	At   time.Duration
	Name string
	Do   func(ctx context.Context, h *Harness) error
}

// Scenario is one drill.
type Scenario struct {
	Name    string
	Summary string
	Hosts   map[string]int32
	// Quota in GPUs; 0 means unlimited.
	Quota    int64
	Faults   sim.Faults
	Jobs     []sim.JobSpec
	Pool     func(*v1alpha1.GPUNodePool)
	Settings func(*config.Settings)
	Events   []Event
	// PDB protects the pods of one job with minAvailable.
	PDB *PDB
	// Replicas > 1 runs that many managers with leader election.
	Replicas int
	// CompareNaive also runs the naive baseline on the same scenario.
	CompareNaive bool
	// UntilNoDrift keeps the drill running until no node is on an old template.
	UntilNoDrift bool
	Timeout      time.Duration
}

// PDB is a PodDisruptionBudget on a job's pods.
type PDB struct {
	Job          string
	MinAvailable int32
}

func gangs(prefix string, n, members int, dur, at time.Duration) []sim.JobSpec {
	var out []sim.JobSpec
	for i := 1; i <= n; i++ {
		out = append(out, sim.JobSpec{Name: fmt.Sprintf("%s-%d", prefix, i), Members: members, GPUs: 8, Duration: dur, Gang: true,
			At: at + time.Duration(i-1)*300*time.Millisecond})
	}
	return out
}

func singles(prefix string, n, pods int, gpus int32, dur, at time.Duration) []sim.JobSpec {
	var out []sim.JobSpec
	for i := 1; i <= n; i++ {
		out = append(out, sim.JobSpec{Name: fmt.Sprintf("%s-%d", prefix, i), Members: pods, GPUs: gpus, Duration: dur, At: at})
	}
	return out
}

func concat(js ...[]sim.JobSpec) []sim.JobSpec {
	var out []sim.JobSpec
	for _, j := range js {
		out = append(out, j...)
	}
	return out
}

// burstJobs: 51 nodes' worth of work arriving within two seconds.
func burstJobs() []sim.JobSpec {
	return concat(
		gangs("train8", 3, 8, 12*time.Second, 0),
		gangs("train4", 4, 4, 10*time.Second, 500*time.Millisecond),
		gangs("train2", 4, 2, 8*time.Second, time.Second),
		singles("notebook", 3, 8, 2, 8*time.Second, 1500*time.Millisecond),
	)
}

// Scenarios are the drills, in report order.
func Scenarios() []Scenario {
	return []Scenario{
		{
			Name:    "burst",
			Summary: "51 nodes of gangs (2 to 8 nodes) and single pods arrive within 2 s; GPUs are advertised 2 s after a node registers",
			Hosts:   map[string]int32{"fab-a": 40, "fab-b": 24, "fab-c": 12},
			Jobs:    burstJobs(), CompareNaive: true, Timeout: 150 * time.Second,
		},
		{
			Name:    "contention",
			Summary: "50 nodes of gangs for 32 hosts in three fabrics (12, 12, 8): gangs must queue for whole fabrics",
			Hosts:   map[string]int32{"fab-a": 12, "fab-b": 12, "fab-c": 8},
			Jobs: concat(
				gangs("train12", 2, 12, 10*time.Second, 0),
				gangs("train6", 3, 6, 8*time.Second, time.Second),
				gangs("train2", 4, 2, 6*time.Second, 2*time.Second),
			),
			CompareNaive: true, Timeout: 180 * time.Second,
		},
		{
			Name:    "ice",
			Summary: "another tenant takes 30 of the largest fabric's 40 free hosts just before the burst, and the cloud's offerings keep showing them free for 10 s",
			Hosts:   map[string]int32{"fab-a": 40, "fab-b": 30, "fab-c": 20},
			Faults:  sim.Faults{OfferingsStaleFor: 10 * time.Second},
			Jobs:    burstJobs(), CompareNaive: true, Timeout: 150 * time.Second,
			Events: []Event{{At: 0, Name: "another tenant takes 30 hosts in fab-a", Do: takeHosts("fab-a", 30)}},
		},
		{
			Name:    "quota",
			Summary: "the project's GPU quota is 160 GPUs (20 servers) and the queued gangs need 32",
			Hosts:   map[string]int32{"fab-a": 40, "fab-b": 40},
			Quota:   160,
			Jobs: concat(
				gangs("train8", 2, 8, 10*time.Second, 0),
				gangs("train4", 3, 4, 8*time.Second, 500*time.Millisecond),
				gangs("train2", 2, 2, 6*time.Second, time.Second),
			),
			CompareNaive: true, Timeout: 180 * time.Second,
		},
		{
			Name:     "crash",
			Summary:  "two replicas with leader election; the leader is killed (no lease release) at 2 s, 6 s and 12 s",
			Hosts:    map[string]int32{"fab-a": 40, "fab-b": 24, "fab-c": 12},
			Jobs:     burstJobs(),
			Replicas: 2,
			Events: []Event{
				{At: 2 * time.Second, Name: "kill leader", Do: killLeader},
				{At: 6 * time.Second, Name: "kill leader", Do: killLeader},
				{At: 12 * time.Second, Name: "kill leader", Do: killLeader},
			},
			Timeout: 180 * time.Second,
		},
		{
			Name:    "flaky",
			Summary: "15% of launches never boot, 10% of nodes never advertise GPUs, 5% of cloud calls fail",
			Hosts:   map[string]int32{"fab-a": 40, "fab-b": 24, "fab-c": 12},
			Faults:  sim.Faults{StuckRate: 0.15, NoGPURate: 0.10, UnavailableRate: 0.05},
			Jobs:    burstJobs(),
			Settings: func(s *config.Settings) {
				s.RegistrationTTL = config.D(6 * time.Second)
				s.InitializationTTL = config.D(6 * time.Second)
			},
			Timeout: 180 * time.Second,
		},
		{
			Name:    "upgrade",
			Summary: "the pool's image changes while two training gangs and a 6-replica service with a PDB (minAvailable 5) run",
			Hosts:   map[string]int32{"fab-a": 20, "fab-b": 20},
			Jobs: []sim.JobSpec{
				{Name: "train-1", Members: 4, GPUs: 8, Duration: 15 * time.Second, Gang: true},
				{Name: "train-2", Members: 4, GPUs: 8, Duration: 25 * time.Second, Gang: true},
				{Name: "serve", Members: 6, GPUs: 4, Duration: 40 * time.Second},
			},
			PDB: &PDB{Job: "serve", MinAvailable: 5},
			Pool: func(p *v1alpha1.GPUNodePool) {
				p.Spec.Disruption.MaxSurge = 1
			},
			Events:       []Event{{At: 8 * time.Second, Name: "new node image", Do: newImage}},
			UntilNoDrift: true,
			Timeout:      180 * time.Second,
		},
	}
}

func takeHosts(fabric string, n int32) func(context.Context, *Harness) error {
	return func(ctx context.Context, h *Harness) error {
		if _, err := h.Env.Cloud.Offerings(ctx); err != nil { // the cloud's answer is cached from here
			return err
		}
		h.Env.Cloud.TakeHosts(fabric, simenv.H100.Name, n)
		return nil
	}
}

func killLeader(ctx context.Context, h *Harness) error {
	return h.KillLeader(ctx)
}

func newImage(ctx context.Context, h *Harness) error {
	return h.UpdatePool(ctx, func(p *v1alpha1.GPUNodePool) { p.Spec.Image = "ubuntu24-cuda12-v2" })
}
