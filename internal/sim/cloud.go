// Package sim simulates what the provisioner runs against: a GPU cloud with fabrics, capacity,
// quota, slow and failing launches (Cloud); kubelets that register nodes and advertise GPUs late
// (Kubelet); a scheduler that binds pods (Scheduler); and training jobs (Workload). It is used by
// tests, drills and the scale benchmark, never by the provisioner itself.
package sim

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
)

// FabricConfig is one fabric and how many hosts of each instance type it has.
type FabricConfig struct {
	Name  string           `json:"name"`
	Hosts map[string]int32 `json:"hosts"`
}

// Faults are the ways the simulated cloud misbehaves. They can be changed while it runs.
type Faults struct {
	// ICE makes Create fail with InsufficientCapacity in a fabric even when hosts are free,
	// with the given probability (1 = the fabric is out of capacity).
	ICE map[string]float64 `json:"ice,omitempty"`
	// UnavailableRate is the probability that any call fails with a transient error.
	UnavailableRate float64 `json:"unavailableRate,omitempty"`
	// StuckRate is the probability that an instance never leaves Creating.
	StuckRate float64 `json:"stuckRate,omitempty"`
	// NoGPURate is the probability that an instance boots but its GPUs are never advertised.
	NoGPURate float64 `json:"noGPURate,omitempty"`
	// OfferingsStaleFor is how old the Offerings answer may be.
	OfferingsStaleFor time.Duration `json:"offeringsStaleFor,omitempty"`
}

// CloudConfig configures a simulated cloud.
type CloudConfig struct {
	Fabrics       []FabricConfig       `json:"fabrics"`
	InstanceTypes []cloud.InstanceType `json:"instanceTypes"`
	GPUQuota      int64                `json:"gpuQuota"`
	// LaunchDelay is how long an instance stays Creating (uniform between Min and Max).
	LaunchDelayMin time.Duration `json:"launchDelayMin"`
	LaunchDelayMax time.Duration `json:"launchDelayMax"`
	// DeleteDelay is how long a deleted instance keeps its host.
	DeleteDelay time.Duration `json:"deleteDelay"`
	// APILatency is added to every call.
	APILatency time.Duration `json:"apiLatency"`
	Seed       int64         `json:"seed"`
	Faults     Faults        `json:"faults"`
}

type simInstance struct {
	cloud.Instance
	runningAt time.Time
	deletedAt time.Time // zero while not deleted
	goneAt    time.Time
	stuck     bool
	noGPU     bool
	gpus      int32
}

// Ledger is the cloud's own record of what it was asked to do, used to judge drills.
type Ledger struct {
	CreateCalls     int     `json:"createCalls"`
	CreateFailures  int     `json:"createFailures"`
	Instances       int     `json:"instances"`
	DuplicateClaims int     `json:"duplicateClaims"` // claims for which more than one instance was ever made
	PeakInstances   int     `json:"peakInstances"`
	PeakGPUs        int64   `json:"peakGPUs"`
	GPUSeconds      float64 `json:"gpuSeconds"` // GPUs × seconds from create to delete
	DeleteCalls     int     `json:"deleteCalls"`
	QuotaRejections int     `json:"quotaRejections"`
	ICERejections   int     `json:"iceRejections"`
}

// Cloud is an in-memory GPU cloud.
type Cloud struct {
	mu         sync.Mutex
	cfg        CloudConfig
	rng        *rand.Rand
	types      map[string]cloud.InstanceType
	fabrics    map[string]map[string]int32
	instances  map[string]*simInstance
	byClaim    map[string]string
	everClaims map[string]int
	nextID     int
	ledger     Ledger
	offerCache []cloud.Offering
	offerAt    time.Time
	now        func() time.Time
}

// NewCloud builds a simulated cloud.
func NewCloud(cfg CloudConfig) *Cloud {
	c := &Cloud{
		cfg:        cfg,
		rng:        rand.New(rand.NewSource(cfg.Seed)),
		types:      map[string]cloud.InstanceType{},
		fabrics:    map[string]map[string]int32{},
		instances:  map[string]*simInstance{},
		byClaim:    map[string]string{},
		everClaims: map[string]int{},
		now:        time.Now,
	}
	for _, t := range cfg.InstanceTypes {
		c.types[t.Name] = t
	}
	for _, f := range cfg.Fabrics {
		hosts := map[string]int32{}
		for k, v := range f.Hosts {
			hosts[k] = v
		}
		c.fabrics[f.Name] = hosts
	}
	return c
}

var _ cloud.Provider = (*Cloud)(nil)

// SetFaults replaces the fault settings.
func (c *Cloud) SetFaults(f Faults) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg.Faults = f
	c.offerAt = time.Time{}
}

// TakeHosts removes free hosts from a fabric, as another tenant launching there would. Offerings
// keep reporting the old number until their cache expires.
func (c *Cloud) TakeHosts(fabric, instanceType string, n int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h := c.fabrics[fabric]; h != nil {
		h[instanceType] -= n
		if h[instanceType] < 0 {
			h[instanceType] = 0
		}
	}
}

// SetQuota changes the project's GPU quota.
func (c *Cloud) SetQuota(gpus int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg.GPUQuota = gpus
}

func (c *Cloud) call(ctx context.Context) error {
	if c.cfg.APILatency > 0 {
		select {
		case <-time.After(c.cfg.APILatency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.Faults.UnavailableRate > 0 && c.rng.Float64() < c.cfg.Faults.UnavailableRate {
		return cloud.Errorf(cloud.Unavailable, "simulated API failure")
	}
	return nil
}

// advance moves instances along by the clock and frees hosts of deleted ones. Caller holds mu.
func (c *Cloud) advance() {
	now := c.now()
	live, gpus := 0, int64(0)
	for id, in := range c.instances {
		switch {
		case !in.deletedAt.IsZero() && !now.Before(in.goneAt):
			c.ledger.GPUSeconds += float64(in.gpus) * in.goneAt.Sub(in.CreatedAt).Seconds()
			c.fabrics[in.Fabric][in.InstanceType]++
			delete(c.instances, id)
			if c.byClaim[in.ClaimName] == id {
				delete(c.byClaim, in.ClaimName)
			}
			continue
		case !in.deletedAt.IsZero():
			in.State = cloud.StateDeleting
		case in.State == cloud.StateCreating && !in.stuck && !now.Before(in.runningAt):
			in.State = cloud.StateRunning
		}
		live++
		gpus += int64(in.gpus)
	}
	if live > c.ledger.PeakInstances {
		c.ledger.PeakInstances = live
	}
	if gpus > c.ledger.PeakGPUs {
		c.ledger.PeakGPUs = gpus
	}
}

func (c *Cloud) usedGPUs() int64 {
	var n int64
	for _, in := range c.instances {
		n += int64(in.gpus)
	}
	return n
}

// InstanceTypes lists what the cloud sells.
func (c *Cloud) InstanceTypes(ctx context.Context) ([]cloud.InstanceType, error) {
	if err := c.call(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]cloud.InstanceType, 0, len(c.types))
	for _, t := range c.types {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Offerings returns free hosts per fabric, possibly stale.
func (c *Cloud) Offerings(ctx context.Context) ([]cloud.Offering, error) {
	if err := c.call(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	now := c.now()
	if c.offerCache != nil && now.Sub(c.offerAt) < c.cfg.Faults.OfferingsStaleFor {
		return append([]cloud.Offering(nil), c.offerCache...), nil
	}
	var out []cloud.Offering
	for f, hosts := range c.fabrics {
		for t, n := range hosts {
			out = append(out, cloud.Offering{Fabric: f, InstanceType: t, Available: n})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Fabric != out[j].Fabric {
			return out[i].Fabric < out[j].Fabric
		}
		return out[i].InstanceType < out[j].InstanceType
	})
	c.offerCache, c.offerAt = out, now
	return append([]cloud.Offering(nil), out...), nil
}

// Quota returns the GPU quota and current use.
func (c *Cloud) Quota(ctx context.Context) (cloud.Quota, error) {
	if err := c.call(ctx); err != nil {
		return cloud.Quota{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	return cloud.Quota{GPULimit: c.cfg.GPUQuota, GPUUsed: c.usedGPUs()}, nil
}

// Create starts an instance for a claim, or returns the one that already exists for it.
func (c *Cloud) Create(ctx context.Context, req cloud.CreateRequest) (*cloud.Instance, error) {
	if err := c.call(ctx); err != nil {
		c.mu.Lock()
		c.ledger.CreateCalls++
		c.ledger.CreateFailures++
		c.mu.Unlock()
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	c.ledger.CreateCalls++
	if id, ok := c.byClaim[req.ClaimName]; ok {
		in := c.instances[id]
		if in.deletedAt.IsZero() {
			cp := in.Instance
			return &cp, nil
		}
	}
	t, ok := c.types[req.InstanceType]
	if !ok {
		c.ledger.CreateFailures++
		return nil, cloud.Errorf(cloud.NotFound, "instance type %q", req.InstanceType)
	}
	hosts, ok := c.fabrics[req.Fabric]
	if !ok {
		c.ledger.CreateFailures++
		return nil, cloud.Errorf(cloud.NotFound, "fabric %q", req.Fabric)
	}
	if c.usedGPUs()+int64(t.GPUs) > c.cfg.GPUQuota {
		c.ledger.CreateFailures++
		c.ledger.QuotaRejections++
		return nil, cloud.Errorf(cloud.QuotaExceeded, "GPU quota %d reached", c.cfg.GPUQuota)
	}
	if hosts[req.InstanceType] <= 0 || c.rng.Float64() < c.cfg.Faults.ICE[req.Fabric] {
		c.ledger.CreateFailures++
		c.ledger.ICERejections++
		return nil, cloud.Errorf(cloud.InsufficientCapacity, "no %s host free in %s", req.InstanceType, req.Fabric)
	}
	hosts[req.InstanceType]--
	c.nextID++
	id := fmt.Sprintf("i-%06d", c.nextID)
	now := c.now()
	delay := c.cfg.LaunchDelayMin
	if span := c.cfg.LaunchDelayMax - c.cfg.LaunchDelayMin; span > 0 {
		delay += time.Duration(c.rng.Int63n(int64(span)))
	}
	in := &simInstance{
		Instance: cloud.Instance{
			ID: id, ProviderID: "sim://" + req.Fabric + "/" + id, ClaimName: req.ClaimName, Fabric: req.Fabric,
			InstanceType: req.InstanceType, Image: req.Image, State: cloud.StateCreating, CreatedAt: now,
		},
		runningAt: now.Add(delay),
		stuck:     c.rng.Float64() < c.cfg.Faults.StuckRate,
		noGPU:     c.rng.Float64() < c.cfg.Faults.NoGPURate,
		gpus:      t.GPUs,
	}
	c.instances[id] = in
	c.byClaim[req.ClaimName] = id
	c.everClaims[req.ClaimName]++
	if c.everClaims[req.ClaimName] == 2 {
		c.ledger.DuplicateClaims++
	}
	c.ledger.Instances++
	c.advance()
	cp := in.Instance
	return &cp, nil
}

// Get returns one instance.
func (c *Cloud) Get(ctx context.Context, id string) (*cloud.Instance, error) {
	if err := c.call(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	in, ok := c.instances[id]
	if !ok {
		return nil, cloud.Errorf(cloud.NotFound, "instance %s", id)
	}
	cp := in.Instance
	return &cp, nil
}

// List returns every instance that still exists.
func (c *Cloud) List(ctx context.Context) ([]*cloud.Instance, error) {
	if err := c.call(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	out := make([]*cloud.Instance, 0, len(c.instances))
	for _, in := range c.instances {
		cp := in.Instance
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Delete starts deleting an instance. Deleting an unknown instance returns NotFound.
func (c *Cloud) Delete(ctx context.Context, id string) error {
	if err := c.call(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	c.ledger.DeleteCalls++
	in, ok := c.instances[id]
	if !ok {
		return cloud.Errorf(cloud.NotFound, "instance %s", id)
	}
	if in.deletedAt.IsZero() {
		now := c.now()
		in.deletedAt = now
		in.goneAt = now.Add(c.cfg.DeleteDelay)
		in.State = cloud.StateDeleting
	}
	return nil
}

// Ledger returns the cloud's record so far. GPU-seconds include instances that still exist.
func (c *Cloud) Ledger() Ledger {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	l := c.ledger
	now := c.now()
	for _, in := range c.instances {
		end := now
		if !in.deletedAt.IsZero() {
			end = in.goneAt
		}
		l.GPUSeconds += float64(in.gpus) * end.Sub(in.CreatedAt).Seconds()
	}
	return l
}

// running is what the simulated kubelet needs to know about instances. Caller must not hold mu.
type runningInstance struct {
	cloud.Instance
	noGPU bool
	gpus  int32
	typ   cloud.InstanceType
}

func (c *Cloud) runningInstances() []runningInstance {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	out := make([]runningInstance, 0, len(c.instances))
	for _, in := range c.instances {
		if in.State != cloud.StateRunning {
			continue
		}
		out = append(out, runningInstance{Instance: in.Instance, noGPU: in.noGPU, gpus: in.gpus, typ: c.types[in.InstanceType]})
	}
	return out
}

// LiveInstances counts instances not being deleted, by claim name.
func (c *Cloud) LiveInstances() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	out := map[string]int{}
	for _, in := range c.instances {
		if in.deletedAt.IsZero() {
			out[in.ClaimName]++
		}
	}
	return out
}

// GPUsInUse counts GPUs of instances that exist (any state).
func (c *Cloud) GPUsInUse() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	return c.usedGPUs()
}
