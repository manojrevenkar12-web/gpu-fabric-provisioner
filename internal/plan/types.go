// Package plan decides, from a snapshot of the cluster and the cloud, which servers to create,
// in which fabric each gang runs, and when a gang may start. It is pure: no API calls, no clock,
// no randomness, so the same snapshot always gives the same plan and it can be tested and
// benchmarked at 20,000 nodes without an API server.
package plan

import (
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
)

// Resources are what a pod requests and a node offers.
type Resources struct {
	GPU      int32
	MilliCPU int64
	Memory   int64
}

func (r Resources) fits(in Resources) bool {
	return r.GPU <= in.GPU && r.MilliCPU <= in.MilliCPU && r.Memory <= in.Memory
}

func (r Resources) sub(o Resources) Resources {
	return Resources{GPU: r.GPU - o.GPU, MilliCPU: r.MilliCPU - o.MilliCPU, Memory: r.Memory - o.Memory}
}

// Pool is a GPUNodePool as the planner sees it.
type Pool struct {
	Name         string
	InstanceType cloud.InstanceType
	// Fabrics the pool may use; empty means any fabric with an offering for the instance type.
	Fabrics      []string
	MinNodes     int32
	MaxNodes     *int32
	Weight       int32
	Labels       map[string]string
	Taints       []corev1.Taint
	TemplateHash string
}

// Node is a NodeClaim and, once registered, its Node.
type Node struct {
	// Claim is the NodeClaim name; it identifies the node in a plan.
	Claim    string
	NodeName string
	Pool     string
	Fabric   string
	Capacity Resources
	// Used is the sum of requests of non-terminal pods bound to the node.
	Used Resources
	// Initialized: Ready and advertising every GPU of the instance type.
	Initialized bool
	// Schedulable is false for nodes that are cordoned, terminating or drifted: nothing new is
	// planned onto them, but they still count against pool limits.
	Schedulable bool
	// Launched: the cloud already has an instance (and so already took it out of Offerings).
	Launched bool
}

// Pod is a pending pod that needs GPUs.
type Pod struct {
	// Key is namespace/name.
	Key string
	// Gang is namespace/gang-name, or "" for a pod that is not part of a gang.
	Gang string
	// GangSize is the number of members the gang declares.
	GangSize int
	Requests Resources
	// Gated: still held by the placement scheduling gate.
	Gated bool
	// Fabric the pod is bound to by node affinity (a released gang member), or "".
	Fabric string
	// PlannedFabric is the fabric recorded for a gated gang by an earlier plan, or "".
	PlannedFabric string
	Priority      int32
	Created       time.Time
	// Spec is used for node selector, affinity and toleration matching.
	Spec *corev1.PodSpec
}

// Snapshot is everything the planner needs.
type Snapshot struct {
	Pools []Pool
	Nodes []Node
	// Pods are unbound, non-terminal pods requesting GPUs (gated or not).
	Pods []Pod
	// Available hosts per fabric and instance type, from the cloud's offerings, before claims
	// that the cloud has not seen yet (Launched=false) are subtracted.
	Available map[string]map[string]int32
	// Unavailable marks fabric/instance-type pairs that recently failed with InsufficientCapacity.
	Unavailable map[string]bool
	// QuotaFreeGPUs is how many more GPUs the project may use.
	QuotaFreeGPUs int64
}

// UnavailableKey is the key of Snapshot.Unavailable.
func UnavailableKey(fabric, instanceType string) string { return fabric + "/" + instanceType }

// NewClaims asks for Count new servers of a pool in a fabric.
type NewClaims struct {
	Pool   string
	Fabric string
	Count  int
	Reason string
}

// Placement records the fabric chosen for a gated gang.
type Placement struct {
	Gang   string
	Fabric string
	Pods   []string
}

// Release says a gated gang may start: every member fits on initialized nodes of its fabric.
type Release struct {
	Gang   string
	Fabric string
	Pods   []string
}

// Wait explains why a gang or pod gets nothing in this plan.
type Wait struct {
	Gang   string
	Pod    string
	Reason string
}

// Plan is the planner's output.
type Plan struct {
	Create     []NewClaims
	Placements []Placement
	Releases   []Release
	Waits      []Wait
	// Reserved are claims of existing nodes that pending pods were planned onto. Consolidation
	// must leave them alone.
	Reserved map[string]bool
	Stats    Stats
}

// Stats summarise a plan.
type Stats struct {
	Pods             int
	Gangs            int
	GangsReleased    int
	GangsWaiting     int
	SingletonsPlaced int
	NewNodes         int
	InFlightNodes    int
	InitializedNodes int
}
