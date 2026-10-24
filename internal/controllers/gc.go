package controllers

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/config"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/metrics"
)

// GC reconciles the cloud with the cluster, for the cases no event covers: an instance created
// for a claim that was deleted before its status was written (a crash between the two), a claim
// whose instance the cloud removed, and a Node left behind by a claim that is gone.
type GC struct {
	Client    client.Client
	APIReader client.Reader // uncached: the cache may lag a claim that was just created
	Cloud     *CloudState
	Settings  config.Settings
	Now       func() time.Time
}

// NeedLeaderElection: only the leader collects.
func (g *GC) NeedLeaderElection() bool { return true }

// Start runs collection passes until ctx ends.
func (g *GC) Start(ctx context.Context) error {
	t := time.NewTicker(g.Settings.GCInterval.Duration)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if _, err := g.Once(ctx); err != nil {
				log.FromContext(ctx).Error(err, "gc pass failed")
			}
		}
	}
}

// GCResult counts what a pass removed.
type GCResult struct {
	Instances int
	Claims    int
	Nodes     int
}

// Once runs one pass.
func (g *GC) Once(ctx context.Context) (GCResult, error) {
	var res GCResult
	now := time.Now()
	if g.Now != nil {
		now = g.Now()
	}
	grace := g.Settings.GCGrace.Duration
	instances, err := g.Cloud.Provider.List(ctx)
	if err != nil {
		return res, err
	}
	var claims v1alpha1.NodeClaimList
	if err := g.Client.List(ctx, &claims); err != nil {
		return res, err
	}
	byName := map[string]*v1alpha1.NodeClaim{}
	for i := range claims.Items {
		byName[claims.Items[i].Name] = &claims.Items[i]
	}
	live := map[string]bool{}
	for _, in := range instances {
		live[in.ID] = true
		if in.State == cloud.StateDeleting || now.Sub(in.CreatedAt) < grace {
			continue
		}
		if c := byName[in.ClaimName]; c != nil && (c.Status.InstanceID == "" || c.Status.InstanceID == in.ID) {
			continue
		}
		// Not in the cache, or the claim points at another instance: confirm against the API server.
		c := &v1alpha1.NodeClaim{}
		err := g.APIReader.Get(ctx, types.NamespacedName{Name: in.ClaimName}, c)
		if err == nil && (c.Status.InstanceID == "" || c.Status.InstanceID == in.ID) {
			continue
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return res, err
		}
		if err := g.Cloud.Provider.Delete(ctx, in.ID); err != nil && !cloud.IsNotFound(err) {
			return res, err
		}
		metrics.LeakedInstances.Inc()
		res.Instances++
	}
	for _, c := range byName {
		if c.DeletionTimestamp != nil || c.Status.InstanceID == "" || live[c.Status.InstanceID] {
			continue
		}
		if since := conditionSince(c.Status.Conditions, v1alpha1.ConditionLaunched, c.CreationTimestamp.Time); now.Sub(since) < grace {
			continue
		}
		if err := DeleteClaim(ctx, g.Client, c, "instance-gone", true); err == nil {
			res.Claims++
		}
	}
	var nodes corev1.NodeList
	if err := g.Client.List(ctx, &nodes, client.HasLabels{v1alpha1.LabelNodeClaim}); err != nil {
		return res, err
	}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if byName[n.Labels[v1alpha1.LabelNodeClaim]] != nil || now.Sub(n.CreationTimestamp.Time) < grace {
			continue
		}
		c := &v1alpha1.NodeClaim{}
		if err := g.APIReader.Get(ctx, types.NamespacedName{Name: n.Labels[v1alpha1.LabelNodeClaim]}, c); !apierrors.IsNotFound(err) {
			continue
		}
		if _, err := Drain(ctx, g.Client, n.Name, true); err != nil {
			return res, err
		}
		if err := g.Client.Delete(ctx, n); err != nil && !apierrors.IsNotFound(err) {
			return res, err
		}
		res.Nodes++
	}
	return res, nil
}
