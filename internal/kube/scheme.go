// Package kube holds the runtime scheme shared by the manager, tests and tools.
package kube

import (
	kruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
)

// Scheme has the core Kubernetes types and this project's API.
func Scheme() *kruntime.Scheme {
	s := kruntime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		panic(err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		panic(err)
	}
	return s
}
