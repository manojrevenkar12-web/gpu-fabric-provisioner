package controllers

import (
	"context"
	"sync"
	"time"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/metrics"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/plan"
)

// instrumented wraps a Provider with call metrics.
type instrumented struct{ p cloud.Provider }

// Instrument adds call counts and latencies to a provider.
func Instrument(p cloud.Provider) cloud.Provider { return instrumented{p} }

func observe(op string, start time.Time, err error) {
	code := "ok"
	if err != nil {
		code = string(cloud.CodeOf(err))
		if code == "" {
			code = "error"
		}
	}
	metrics.CloudCalls.WithLabelValues(op, code).Inc()
	metrics.CloudLatency.WithLabelValues(op).Observe(time.Since(start).Seconds())
}

func (i instrumented) InstanceTypes(ctx context.Context) (out []cloud.InstanceType, err error) {
	defer func(t time.Time) { observe("instance_types", t, err) }(time.Now())
	return i.p.InstanceTypes(ctx)
}

func (i instrumented) Offerings(ctx context.Context) (out []cloud.Offering, err error) {
	defer func(t time.Time) { observe("offerings", t, err) }(time.Now())
	return i.p.Offerings(ctx)
}

func (i instrumented) Quota(ctx context.Context) (out cloud.Quota, err error) {
	defer func(t time.Time) { observe("quota", t, err) }(time.Now())
	return i.p.Quota(ctx)
}

func (i instrumented) Create(ctx context.Context, req cloud.CreateRequest) (out *cloud.Instance, err error) {
	defer func(t time.Time) { observe("create", t, err) }(time.Now())
	return i.p.Create(ctx, req)
}

func (i instrumented) Get(ctx context.Context, id string) (out *cloud.Instance, err error) {
	defer func(t time.Time) { observe("get", t, err) }(time.Now())
	return i.p.Get(ctx, id)
}

func (i instrumented) List(ctx context.Context) (out []*cloud.Instance, err error) {
	defer func(t time.Time) { observe("list", t, err) }(time.Now())
	return i.p.List(ctx)
}

func (i instrumented) Delete(ctx context.Context, id string) (err error) {
	defer func(t time.Time) { observe("delete", t, err) }(time.Now())
	return i.p.Delete(ctx, id)
}

// CloudState caches what the planner needs from the cloud: instance types, offerings and quota,
// refreshed at most every refresh interval, plus fabrics that recently ran out of capacity.
type CloudState struct {
	Provider cloud.Provider
	Refresh  time.Duration
	TTL      time.Duration
	Now      func() time.Time

	mu          sync.Mutex
	types       map[string]cloud.InstanceType
	offerings   map[string]map[string]int32
	quota       cloud.Quota
	fetched     time.Time
	unavailable map[string]time.Time
}

// NewCloudState builds a cache over a provider.
func NewCloudState(p cloud.Provider, refresh, ttl time.Duration) *CloudState {
	return &CloudState{Provider: p, Refresh: refresh, TTL: ttl, Now: time.Now, unavailable: map[string]time.Time{}}
}

// View is a consistent copy of the cached cloud state.
type View struct {
	Types       map[string]cloud.InstanceType
	Available   map[string]map[string]int32
	Quota       cloud.Quota
	Unavailable map[string]bool
}

// Get returns the cached state, refreshing it if it is older than the refresh interval. If the
// refresh fails and a previous answer exists, the previous answer is returned with the error.
func (c *CloudState) Get(ctx context.Context) (View, error) {
	c.mu.Lock()
	stale := c.types == nil || c.Now().Sub(c.fetched) >= c.Refresh
	c.mu.Unlock()
	var err error
	if stale {
		err = c.refresh(ctx)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.types == nil {
		return View{}, err
	}
	v := View{Types: c.types, Available: map[string]map[string]int32{}, Quota: c.quota, Unavailable: map[string]bool{}}
	for f, ts := range c.offerings {
		m := map[string]int32{}
		for t, n := range ts {
			m[t] = n
		}
		v.Available[f] = m
	}
	now := c.Now()
	for k, until := range c.unavailable {
		if now.Before(until) {
			v.Unavailable[k] = true
		} else {
			delete(c.unavailable, k)
		}
	}
	metrics.Unavailable.Set(float64(len(v.Unavailable)))
	return v, err
}

// Invalidate forces the next Get to read the cloud.
func (c *CloudState) Invalidate() {
	c.mu.Lock()
	c.fetched = time.Time{}
	c.mu.Unlock()
}

func (c *CloudState) refresh(ctx context.Context) error {
	types, err := c.Provider.InstanceTypes(ctx)
	if err != nil {
		return err
	}
	offs, err := c.Provider.Offerings(ctx)
	if err != nil {
		return err
	}
	q, err := c.Provider.Quota(ctx)
	if err != nil {
		return err
	}
	tm := map[string]cloud.InstanceType{}
	for _, t := range types {
		tm[t.Name] = t
	}
	om := map[string]map[string]int32{}
	for _, o := range offs {
		if om[o.Fabric] == nil {
			om[o.Fabric] = map[string]int32{}
		}
		om[o.Fabric][o.InstanceType] = o.Available
	}
	c.mu.Lock()
	c.types, c.offerings, c.quota, c.fetched = tm, om, q, c.Now()
	c.mu.Unlock()
	return nil
}

// MarkUnavailable records that a fabric just answered InsufficientCapacity for an instance type.
func (c *CloudState) MarkUnavailable(fabric, instanceType string) {
	c.mu.Lock()
	c.unavailable[plan.UnavailableKey(fabric, instanceType)] = c.Now().Add(c.TTL)
	c.fetched = time.Time{}
	c.mu.Unlock()
}

// Type returns an instance type from the cached catalog.
func (c *CloudState) Type(ctx context.Context, name string) (cloud.InstanceType, bool) {
	v, _ := c.Get(ctx)
	t, ok := v.Types[name]
	return t, ok
}
