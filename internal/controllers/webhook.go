package controllers

import (
	"context"
	"encoding/json"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/podutil"
)

// PodGater is a mutating admission webhook that adds the placement scheduling gate to gang pods
// that request GPUs, so no member is scheduled before the gang has a fabric and capacity.
// Pods that are not in a gang, or request no GPUs, pass unchanged.
//
// +kubebuilder:webhook:path=/mutate-pods,mutating=true,failurePolicy=ignore,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=gate.gpufabric.dev,admissionReviewVersions=v1
type PodGater struct {
	Decoder admission.Decoder
}

// Handle implements admission.Handler.
func (g *PodGater) Handle(_ context.Context, req admission.Request) admission.Response {
	pod := &corev1.Pod{}
	if err := g.Decoder.Decode(req, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if gang, _ := podutil.Gang(pod); gang == "" || !podutil.WantsGPU(pod) || podutil.Gated(pod) || pod.Spec.NodeName != "" {
		return admission.Allowed("not a gated gang pod")
	}
	pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: v1alpha1.SchedulingGate})
	b, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, b)
}
