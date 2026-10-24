package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// GPUNodePoolSpec says which GPU servers the provisioner may create for a cluster and how it may
// disrupt them.
// +kubebuilder:validation:XValidation:rule="!has(self.limits.maxNodes) || self.limits.minNodes <= self.limits.maxNodes",message="limits.minNodes must not exceed limits.maxNodes"
type GPUNodePoolSpec struct {
	// InstanceType is the cloud instance type (platform and preset) of every node in the pool.
	// +kubebuilder:validation:MinLength=1
	InstanceType string `json:"instanceType"`

	// Fabrics the pool may place nodes in. Empty means every fabric that offers the instance type.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=64
	Fabrics []string `json:"fabrics,omitempty"`

	// Image is the node image. Changing it marks every existing node of the pool as drifted, and
	// nodes are replaced by a rolling upgrade.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Labels added to every node of the pool.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Taints added to every node of the pool. Pods must tolerate them to be provisioned for.
	// +optional
	Taints []corev1.Taint `json:"taints,omitempty"`

	// +kubebuilder:default={}
	Limits PoolLimits `json:"limits"`

	// +kubebuilder:default={}
	Disruption Disruption `json:"disruption"`

	// Weight orders pools that could both serve a pod; the higher weight is tried first.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Weight int32 `json:"weight,omitempty"`
}

// PoolLimits bounds the size of a pool.
type PoolLimits struct {
	// MinNodes is kept running even when nothing needs them.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinNodes int32 `json:"minNodes,omitempty"`
	// MaxNodes counts nodes being created and nodes being replaced.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxNodes *int32 `json:"maxNodes,omitempty"`
}

// GangPolicy says what a rolling upgrade does with nodes that run members of a gang.
// +kubebuilder:validation:Enum=WaitForCompletion;Never
type GangPolicy string

const (
	// GangWaitForCompletion replaces a node that runs gang members only after the gang has finished.
	GangWaitForCompletion GangPolicy = "WaitForCompletion"
	// GangNever never voluntarily replaces a node that runs gang members.
	GangNever GangPolicy = "Never"
)

// Disruption controls voluntary disruption: consolidation and drift.
type Disruption struct {
	// ConsolidateAfter is how long a node must be empty, or replaceable, before it is removed.
	// +kubebuilder:default="5m"
	// +optional
	ConsolidateAfter metav1.Duration `json:"consolidateAfter,omitempty"`

	// MaxUnavailable bounds how many nodes of the pool may be disrupted at once (count or percent).
	// +kubebuilder:default="10%"
	// +kubebuilder:validation:XIntOrString
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// MaxSurge is how many replacement nodes a rolling upgrade may create ahead of removing old ones.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxSurge int32 `json:"maxSurge,omitempty"`

	// GangPolicy decides when nodes running gang members may be replaced.
	// +kubebuilder:default=WaitForCompletion
	// +optional
	GangPolicy GangPolicy `json:"gangPolicy,omitempty"`

	// Consolidation can be switched off for a pool (drift and repair still happen).
	// +kubebuilder:default=true
	// +optional
	Consolidation *bool `json:"consolidation,omitempty"`
}

// GPUNodePoolStatus is observed state.
type GPUNodePoolStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Nodes counts NodeClaims of the pool, in any phase.
	// +optional
	Nodes int32 `json:"nodes"`
	// Initialized counts nodes that are ready and advertise all their GPUs.
	// +optional
	Initialized int32 `json:"initialized"`
	// InFlight counts nodes that have been requested but are not initialized yet.
	// +optional
	InFlight int32 `json:"inFlight"`
	// Drifted counts nodes whose template differs from the pool's.
	// +optional
	Drifted int32 `json:"drifted"`
	// GPUs is the GPU capacity of all nodes of the pool, initialized or not.
	// +optional
	GPUs int64 `json:"gpus"`
	// TemplateHash identifies the current node template; NodeClaims with another hash are drifted.
	// +optional
	TemplateHash string `json:"templateHash,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// GPUNodePool is a set of GPU servers of one instance type the provisioner manages.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=gnp
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.instanceType`
// +kubebuilder:printcolumn:name="Nodes",type=integer,JSONPath=`.status.nodes`
// +kubebuilder:printcolumn:name="Initialized",type=integer,JSONPath=`.status.initialized`
// +kubebuilder:printcolumn:name="InFlight",type=integer,JSONPath=`.status.inFlight`
// +kubebuilder:printcolumn:name="Drifted",type=integer,JSONPath=`.status.drifted`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type GPUNodePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GPUNodePoolSpec   `json:"spec,omitempty"`
	Status GPUNodePoolStatus `json:"status,omitempty"`
}

// GPUNodePoolList is a list of GPUNodePools.
// +kubebuilder:object:root=true
type GPUNodePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GPUNodePool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GPUNodePool{}, &GPUNodePoolList{})
}
