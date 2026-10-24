package podutil

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
)

func gpuPod(gpus string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ml"},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "init", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("20")}}}},
			Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), v1alpha1.ResourceGPU: resource.MustParse(gpus)},
				Limits:   corev1.ResourceList{v1alpha1.ResourceGPU: resource.MustParse(gpus)}}}},
		},
	}
}

func TestGangConventions(t *testing.T) {
	p := gpuPod("8")
	if g, _ := Gang(p); g != "" {
		t.Fatalf("no labels: %q", g)
	}
	p.Labels = map[string]string{v1alpha1.LabelKueuePodGroup: "train"}
	p.Annotations = map[string]string{v1alpha1.AnnotationKueuePodGroupCount: "16"}
	if g, n := Gang(p); g != "ml/train" || n != 16 {
		t.Fatalf("kueue: %q %d", g, n)
	}
	p.Labels[v1alpha1.LabelGang] = "own"
	p.Annotations[v1alpha1.AnnotationGangSize] = "x"
	if g, n := Gang(p); g != "ml/own" || n != 0 {
		t.Fatalf("own label wins, bad size is unknown: %q %d", g, n)
	}
}

func TestRequestsIncludeInitContainers(t *testing.T) {
	r := Requests(gpuPod("4"))
	if r.GPU != 4 || r.MilliCPU != 20000 {
		t.Fatalf("got %+v", r)
	}
	if !WantsGPU(gpuPod("1")) || WantsGPU(&corev1.Pod{}) {
		t.Fatal("WantsGPU")
	}
}

func TestReleaseKeepsOtherGatesAndExistingTerms(t *testing.T) {
	p := gpuPod("8")
	p.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}, {Name: v1alpha1.SchedulingGate}}
	p.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{
			{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "a", Operator: corev1.NodeSelectorOpExists}}},
			{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "b", Operator: corev1.NodeSelectorOpExists}}},
		}}}}
	r := Release(p, "fab-1")
	if len(r.Spec.SchedulingGates) != 1 || r.Spec.SchedulingGates[0].Name != "kueue.x-k8s.io/admission" {
		t.Fatalf("gates %v", r.Spec.SchedulingGates)
	}
	for _, term := range r.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		if len(term.MatchExpressions) != 2 || term.MatchExpressions[1].Values[0] != "fab-1" {
			t.Fatalf("every OR-term must be narrowed to the fabric: %+v", term)
		}
	}
	if Fabric(r) != "fab-1" || len(p.Spec.SchedulingGates) != 2 {
		t.Fatal("release must not modify its input")
	}
	if !ForeignGated(r) || Gated(r) {
		t.Fatal("gate checks")
	}
}
