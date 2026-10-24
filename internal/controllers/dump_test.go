package controllers_test

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/simenv"
)

// dump describes cluster state for failure messages.
func dump(ctx context.Context, env *simenv.Env) string {
	var b strings.Builder
	var pods corev1.PodList
	_ = env.Direct.List(ctx, &pods)
	for _, p := range pods.Items {
		fmt.Fprintf(&b, "\npod %s node=%q phase=%s gates=%d conds=%v", p.Name, p.Spec.NodeName, p.Status.Phase, len(p.Spec.SchedulingGates), p.Status.Conditions)
	}
	var nodes corev1.NodeList
	_ = env.Direct.List(ctx, &nodes)
	for _, n := range nodes.Items {
		g := n.Status.Allocatable["nvidia.com/gpu"]
		fmt.Fprintf(&b, "\nnode %s labels=%v taints=%v gpu=%s unsched=%v conds=%d", n.Name, n.Labels, n.Spec.Taints, g.String(), n.Spec.Unschedulable, len(n.Status.Conditions))
	}
	return b.String()
}
