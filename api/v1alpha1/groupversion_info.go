// Package v1alpha1 contains the API of gpu-fabric-provisioner: GPUNodePool (what capacity a
// cluster may have) and NodeClaim (one GPU server the provisioner has asked the cloud for).
// +kubebuilder:object:generate=true
// +groupName=gpufabric.dev
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the API group and version of every type in this package.
	GroupVersion = schema.GroupVersion{Group: "gpufabric.dev", Version: "v1alpha1"}

	// SchemeBuilder registers the types of this package with a runtime.Scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types of this package to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// Well-known labels, annotations, taints and scheduling gates.
const (
	// LabelPool is set on every node the provisioner created.
	LabelPool = "gpufabric.dev/pool"
	// LabelFabric names the high-speed interconnect (InfiniBand) fabric a node is attached to.
	// Pods of one gang must all run in one fabric.
	LabelFabric = "gpufabric.dev/fabric"
	// LabelNodeClaim names the NodeClaim a node belongs to.
	LabelNodeClaim = "gpufabric.dev/nodeclaim"
	// LabelInstanceType is the cloud instance type of the node.
	LabelInstanceType = "node.kubernetes.io/instance-type"

	// LabelGang groups the pods of one gang (all members start together or not at all).
	LabelGang = "gpufabric.dev/gang"
	// AnnotationGangSize is the number of members of a gang.
	AnnotationGangSize = "gpufabric.dev/gang-size"
	// LabelKueuePodGroup and AnnotationKueuePodGroupCount are Kueue's plain-pod group conventions,
	// accepted as an alternative to LabelGang/AnnotationGangSize.
	LabelKueuePodGroup           = "kueue.x-k8s.io/pod-group-name"
	AnnotationKueuePodGroupCount = "kueue.x-k8s.io/pod-group-total-count"

	// SchedulingGate holds gang pods until the provisioner has chosen a fabric for the whole gang
	// and capacity for every member is ready.
	SchedulingGate = "gpufabric.dev/placement"
	// AnnotationPlacement records the fabric chosen for a gang that is still gated. It is a
	// decision, not a constraint: until the gate is removed the gang may be moved to another fabric.
	AnnotationPlacement = "gpufabric.dev/planned-fabric"

	// TaintUnregistered is put on a node by its bootstrap configuration and removed by the
	// provisioner once the node carries its pool and fabric labels.
	TaintUnregistered = "gpufabric.dev/unregistered"

	// ResourceGPU is the extended resource advertised by the GPU device plugin.
	ResourceGPU = "nvidia.com/gpu"

	// Finalizer keeps a NodeClaim until its cloud instance and Node are gone.
	Finalizer = "gpufabric.dev/termination"
	// AnnotationDoNotDisrupt on a pod keeps its node from voluntary disruption.
	AnnotationDoNotDisrupt = "gpufabric.dev/do-not-disrupt"
)
