package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NodeClaimSpec is the server the provisioner asked for. It never changes after creation.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable; create a new NodeClaim instead"
type NodeClaimSpec struct {
	// Pool that owns this claim.
	// +kubebuilder:validation:MinLength=1
	Pool string `json:"pool"`
	// +kubebuilder:validation:MinLength=1
	InstanceType string `json:"instanceType"`
	// Fabric the server must be attached to.
	// +kubebuilder:validation:MinLength=1
	Fabric string `json:"fabric"`
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
	// TemplateHash of the pool when the claim was made; a different current hash means drift.
	// +kubebuilder:validation:MinLength=1
	TemplateHash string `json:"templateHash"`
	// Labels and Taints are copied to the Node when it registers.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// +optional
	Taints []corev1.Taint `json:"taints,omitempty"`
	// Reason the claim was created: the gang it was made for, "singleton", "min-nodes" or "replace:<claim>".
	// +optional
	Reason string `json:"reason,omitempty"`
}

// NodeClaimPhase is the lifecycle position of a claim.
// +kubebuilder:validation:Enum=Pending;Launched;Registered;Initialized;Terminating
type NodeClaimPhase string

const (
	// PhasePending: not yet accepted by the cloud.
	PhasePending NodeClaimPhase = "Pending"
	// PhaseLaunched: the cloud has an instance for the claim; no Node yet.
	PhaseLaunched NodeClaimPhase = "Launched"
	// PhaseRegistered: a Node with the instance's provider ID exists and carries the claim's labels.
	PhaseRegistered NodeClaimPhase = "Registered"
	// PhaseInitialized: the Node is Ready and advertises every GPU of the instance type.
	PhaseInitialized NodeClaimPhase = "Initialized"
	// PhaseTerminating: the claim is being deleted.
	PhaseTerminating NodeClaimPhase = "Terminating"
)

// Condition types on NodeClaims.
const (
	ConditionLaunched    = "Launched"
	ConditionRegistered  = "Registered"
	ConditionInitialized = "Initialized"
	ConditionDrifted     = "Drifted"
	ConditionDisrupting  = "Disrupting"
	// Condition types on GPUNodePools.
	ConditionCapacity = "CapacityAvailable"
	ConditionReady    = "Ready"
)

// NodeClaimStatus is observed state.
type NodeClaimStatus struct {
	// +optional
	Phase NodeClaimPhase `json:"phase,omitempty"`
	// InstanceID in the cloud.
	// +optional
	InstanceID string `json:"instanceID,omitempty"`
	// ProviderID the Node will carry.
	// +optional
	ProviderID string `json:"providerID,omitempty"`
	// NodeName once registered.
	// +optional
	NodeName string `json:"nodeName,omitempty"`
	// GPUs the instance type has.
	// +optional
	GPUs int32 `json:"gpus,omitempty"`
	// LaunchAttempts counts failed create calls that were retried.
	// +optional
	LaunchAttempts int32 `json:"launchAttempts,omitempty"`
	// LastError from the cloud, if the last call failed.
	// +optional
	LastError string `json:"lastError,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// NodeClaim is one GPU server requested from the cloud and the Node it becomes.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=nc
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="Fabric",type=string,JSONPath=`.spec.fabric`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.nodeName`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.spec.reason`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type NodeClaim struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NodeClaimSpec   `json:"spec,omitempty"`
	Status NodeClaimStatus `json:"status,omitempty"`
}

// NodeClaimList is a list of NodeClaims.
// +kubebuilder:object:root=true
type NodeClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeClaim `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NodeClaim{}, &NodeClaimList{})
}
