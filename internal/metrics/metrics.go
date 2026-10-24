// Package metrics defines the provisioner's Prometheus metrics, registered with
// controller-runtime's registry so they are served on the manager's /metrics endpoint.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	PlanDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "gpufabric_plan_duration_seconds", Help: "Time to build a snapshot and compute a plan.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	})
	PendingPods = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gpufabric_pending_pods", Help: "Unbound pods requesting GPUs, gated or not, at the last plan.",
	})
	Gangs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gpufabric_gangs", Help: "Pending gangs at the last plan by state (waiting, placed).",
	}, []string{"state"})
	GangsReleased = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gpufabric_gangs_released_total", Help: "Gangs whose placement gate was removed.",
	})
	GangReleaseLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "gpufabric_gang_release_latency_seconds", Help: "From the gang's first pod to its release.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14),
	})
	ClaimsCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gpufabric_nodeclaims_created_total", Help: "NodeClaims created, by pool and reason kind.",
	}, []string{"pool", "reason"})
	ClaimsDeleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gpufabric_nodeclaims_deleted_total", Help: "NodeClaims deleted, by pool and reason.",
	}, []string{"pool", "reason"})
	Claims = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gpufabric_nodeclaims", Help: "NodeClaims by pool and phase.",
	}, []string{"pool", "phase"})
	PoolGPUs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gpufabric_pool_gpus", Help: "GPU capacity by pool and state (initialized, inflight).",
	}, []string{"pool", "state"})
	CloudCalls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gpufabric_cloud_calls_total", Help: "Cloud API calls by operation and result code.",
	}, []string{"op", "code"})
	CloudLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "gpufabric_cloud_call_duration_seconds", Help: "Cloud API call latency.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	}, []string{"op"})
	Unavailable = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gpufabric_unavailable_offerings", Help: "Fabric/instance-type pairs skipped after InsufficientCapacity.",
	})
	DisruptionBlocked = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gpufabric_disruption_blocked_total", Help: "Voluntary disruptions not done, by reason.",
	}, []string{"reason"})
	Evictions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gpufabric_evictions_total", Help: "Pod evictions requested, by result (ok, blocked, error).",
	}, []string{"result"})
	LeakedInstances = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gpufabric_gc_instances_deleted_total", Help: "Cloud instances deleted because no NodeClaim owned them.",
	})
)

func init() {
	crmetrics.Registry.MustRegister(PlanDuration, PendingPods, Gangs, GangsReleased, GangReleaseLatency,
		ClaimsCreated, ClaimsDeleted, Claims, PoolGPUs, CloudCalls, CloudLatency, Unavailable,
		DisruptionBlocked, Evictions, LeakedInstances)
}
