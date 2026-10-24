// Package podutil reads what the provisioner needs from pods: gang membership, GPU requests,
// scheduling gates and fabric constraints.
package podutil

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
	resourcehelper "k8s.io/component-helpers/resource"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/plan"
)

// Gang returns the pod's gang key (namespace/name) and declared size, or "" for other pods.
func Gang(pod *corev1.Pod) (string, int) {
	name := pod.Labels[v1alpha1.LabelGang]
	sizeS := pod.Annotations[v1alpha1.AnnotationGangSize]
	if name == "" {
		name = pod.Labels[v1alpha1.LabelKueuePodGroup]
		sizeS = pod.Annotations[v1alpha1.AnnotationKueuePodGroupCount]
	}
	if name == "" {
		return "", 0
	}
	size, err := strconv.Atoi(sizeS)
	if err != nil || size < 1 {
		size = 0 // unknown: the gang is planned with the members that exist
	}
	return pod.Namespace + "/" + name, size
}

// Requests is the pod's effective request (init containers and overhead included).
func Requests(pod *corev1.Pod) plan.Resources {
	r := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
	gpu := r[corev1.ResourceName(v1alpha1.ResourceGPU)]
	return plan.Resources{GPU: int32(gpu.Value()), MilliCPU: r.Cpu().MilliValue(), Memory: r.Memory().Value()}
}

// WantsGPU reports whether the pod requests GPUs.
func WantsGPU(pod *corev1.Pod) bool {
	for _, cs := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
		for _, c := range cs {
			if q, ok := c.Resources.Requests[corev1.ResourceName(v1alpha1.ResourceGPU)]; ok && !q.IsZero() {
				return true
			}
			if q, ok := c.Resources.Limits[corev1.ResourceName(v1alpha1.ResourceGPU)]; ok && !q.IsZero() {
				return true
			}
		}
	}
	return false
}

// Gated reports whether the pod still carries the placement gate.
func Gated(pod *corev1.Pod) bool {
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == v1alpha1.SchedulingGate {
			return true
		}
	}
	return false
}

// ForeignGated reports whether some other controller still holds the pod with its own gate.
func ForeignGated(pod *corev1.Pod) bool {
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name != v1alpha1.SchedulingGate {
			return true
		}
	}
	return false
}

// Terminal reports whether the pod is done or going away.
func Terminal(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// Fabric returns the fabric a pod is constrained to by node selector or required node affinity.
func Fabric(pod *corev1.Pod) string {
	if f := pod.Spec.NodeSelector[v1alpha1.LabelFabric]; f != "" {
		return f
	}
	a := pod.Spec.Affinity
	if a == nil || a.NodeAffinity == nil || a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return ""
	}
	for _, term := range a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, e := range term.MatchExpressions {
			if e.Key == v1alpha1.LabelFabric && e.Operator == corev1.NodeSelectorOpIn && len(e.Values) == 1 {
				return e.Values[0]
			}
		}
	}
	return ""
}

// DaemonSet reports whether a DaemonSet owns the pod (it never needs to move).
func DaemonSet(pod *corev1.Pod) bool {
	for _, o := range pod.OwnerReferences {
		if o.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

// Controlled reports whether a controller will recreate the pod if it is evicted.
func Controlled(pod *corev1.Pod) bool {
	for _, o := range pod.OwnerReferences {
		if o.Controller != nil && *o.Controller {
			return true
		}
	}
	return false
}

// Release returns a copy of the pod pinned to fabric and without the placement gate. The API
// server accepts both changes in one update because the old pod is still gated: required node
// affinity may then be narrowed (KEP-3838).
func Release(pod *corev1.Pod, fabric string) *corev1.Pod {
	p := pod.DeepCopy()
	gates := p.Spec.SchedulingGates[:0]
	for _, g := range p.Spec.SchedulingGates {
		if g.Name != v1alpha1.SchedulingGate {
			gates = append(gates, g)
		}
	}
	p.Spec.SchedulingGates = gates
	if Fabric(p) == fabric {
		return p
	}
	req := corev1.NodeSelectorRequirement{Key: v1alpha1.LabelFabric, Operator: corev1.NodeSelectorOpIn, Values: []string{fabric}}
	if p.Spec.Affinity == nil {
		p.Spec.Affinity = &corev1.Affinity{}
	}
	if p.Spec.Affinity.NodeAffinity == nil {
		p.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	na := p.Spec.Affinity.NodeAffinity
	if na.RequiredDuringSchedulingIgnoredDuringExecution == nil || len(na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 0 {
		na.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{req}}},
		}
		return p
	}
	terms := na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	for i := range terms {
		terms[i].MatchExpressions = append(terms[i].MatchExpressions, req)
	}
	return p
}
