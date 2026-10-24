package controllers

import (
	"context"
	"encoding/json"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/kube"
)

func review(t *testing.T, pod *corev1.Pod) admission.Response {
	t.Helper()
	raw, _ := json.Marshal(pod)
	g := &PodGater{Decoder: admission.NewDecoder(kube.Scheme())}
	return g.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Namespace: "ml", Operation: admissionv1.Create, Object: runtime.RawExtension{Raw: raw},
	}})
}

func TestWebhookGatesOnlyGangGPUPods(t *testing.T) {
	gpu := corev1.ResourceRequirements{Limits: corev1.ResourceList{v1alpha1.ResourceGPU: resource.MustParse("8")}}
	gang := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "w0", Labels: map[string]string{v1alpha1.LabelGang: "train"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Resources: gpu}}},
	}
	resp := review(t, gang)
	if !resp.Allowed || len(resp.Patches) != 1 || resp.Patches[0].Path != "/spec/schedulingGates" {
		t.Fatalf("gang pod: %+v", resp.Patches)
	}
	single := gang.DeepCopy()
	single.Labels = nil
	if resp := review(t, single); !resp.Allowed || len(resp.Patches) != 0 {
		t.Fatalf("single pod must pass unchanged: %+v", resp.Patches)
	}
	cpu := gang.DeepCopy()
	cpu.Spec.Containers[0].Resources = corev1.ResourceRequirements{}
	if resp := review(t, cpu); len(resp.Patches) != 0 {
		t.Fatalf("CPU pod must pass unchanged")
	}
}
