package controllers

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/config"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/metrics"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/podutil"
)

// Annotations used during termination.
const (
	AnnotationTerminationReason = "gpufabric.dev/termination-reason"
	AnnotationForce             = "gpufabric.dev/force-termination"
)

// Field indexes.
const (
	IndexPodNode        = "spec.nodeName"
	IndexNodeProviderID = "spec.providerID"
	IndexClaimProvider  = "status.providerID"
)

// ClaimReconciler drives a NodeClaim from request to initialized node, and back to nothing.
//
//	Pending ──create──▶ Launched ──Node appears, labels synced──▶ Registered ──Ready, all GPUs──▶ Initialized
//	   │ InsufficientCapacity, quota: deleted, provisioner replans  │ registrationTTL        │ initializationTTL
//	   ▼                                                            ▼                        ▼  (and NotReady > repairAfter)
//	 deleted ◀──── finalizer: cordon, evict, delete instance, delete Node ◀──── replaced
type ClaimReconciler struct {
	Client   client.Client
	Cloud    *CloudState
	Settings config.Settings
	Recorder record.EventRecorder
	Now      func() time.Time
	// OnChange is called when a claim changes in a way the provisioner cares about.
	OnChange func()
}

// +kubebuilder:rbac:groups=gpufabric.dev,resources=nodeclaims;gpunodepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gpufabric.dev,resources=nodeclaims/status;gpunodepools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gpufabric.dev,resources=nodeclaims/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

// Reconcile moves one claim along its lifecycle.
func (r *ClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	claim := &v1alpha1.NodeClaim{}
	if err := r.Client.Get(ctx, req.NamespacedName, claim); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if claim.DeletionTimestamp != nil {
		return r.finalize(ctx, claim)
	}
	if !controllerutil.ContainsFinalizer(claim, v1alpha1.Finalizer) {
		controllerutil.AddFinalizer(claim, v1alpha1.Finalizer)
		if err := r.Client.Update(ctx, claim); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}
	pool := &v1alpha1.GPUNodePool{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: claim.Spec.Pool}, pool); apierrors.IsNotFound(err) {
		return ctrl.Result{}, r.Delete(ctx, claim, "pool-deleted", false)
	} else if err != nil {
		return ctrl.Result{}, err
	}
	before := claim.Status.DeepCopy()
	setCondition(&claim.Status.Conditions, v1alpha1.ConditionDrifted, claim.Spec.TemplateHash != pool.TemplateHash(), "TemplateChanged", "pool template differs from the one this node was made with", claim.Generation)

	var res ctrl.Result
	var err error
	switch claim.Status.Phase {
	case "", v1alpha1.PhasePending:
		res, err = r.launch(ctx, claim)
	case v1alpha1.PhaseLaunched:
		res, err = r.register(ctx, claim)
	case v1alpha1.PhaseRegistered:
		res, err = r.initialize(ctx, claim)
	case v1alpha1.PhaseInitialized:
		res, err = r.health(ctx, claim)
	}
	if err != nil || claim.DeletionTimestamp != nil {
		return res, err
	}
	if !statusEqual(before, &claim.Status) {
		if uerr := r.Client.Status().Update(ctx, claim); uerr != nil {
			return ctrl.Result{}, client.IgnoreNotFound(uerr)
		}
		if before.Phase != claim.Status.Phase && r.OnChange != nil {
			r.OnChange()
		}
	}
	return res, nil
}

func (r *ClaimReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *ClaimReconciler) launch(ctx context.Context, claim *v1alpha1.NodeClaim) (ctrl.Result, error) {
	in, err := r.Cloud.Provider.Create(ctx, cloud.CreateRequest{
		ClaimName: claim.Name, Fabric: claim.Spec.Fabric, InstanceType: claim.Spec.InstanceType,
		Image: claim.Spec.Image, Labels: map[string]string{v1alpha1.LabelPool: claim.Spec.Pool},
	})
	switch cloud.CodeOf(err) {
	case "":
		if err != nil { // not a cloud error: context cancelled, network
			return ctrl.Result{}, err
		}
	case cloud.InsufficientCapacity:
		r.Cloud.MarkUnavailable(claim.Spec.Fabric, claim.Spec.InstanceType)
		r.event(claim, corev1.EventTypeWarning, "InsufficientCapacity", err.Error())
		return ctrl.Result{}, r.Delete(ctx, claim, "insufficient-capacity", true)
	case cloud.QuotaExceeded:
		r.Cloud.Invalidate()
		r.event(claim, corev1.EventTypeWarning, "QuotaExceeded", err.Error())
		return ctrl.Result{}, r.Delete(ctx, claim, "quota-exceeded", true)
	case cloud.NotFound:
		r.event(claim, corev1.EventTypeWarning, "InvalidRequest", err.Error())
		return ctrl.Result{}, r.Delete(ctx, claim, "invalid-request", true)
	default:
		claim.Status.LaunchAttempts++
		claim.Status.LastError = err.Error()
		if claim.Status.LaunchAttempts >= r.Settings.LaunchRetries {
			r.event(claim, corev1.EventTypeWarning, "LaunchFailed", err.Error())
			return ctrl.Result{}, r.Delete(ctx, claim, "launch-failed", true)
		}
		backoff := r.Settings.LaunchBackoff.Duration << (claim.Status.LaunchAttempts - 1)
		return ctrl.Result{RequeueAfter: backoff}, nil
	}
	it, _ := r.Cloud.Type(ctx, claim.Spec.InstanceType)
	claim.Status.InstanceID = in.ID
	claim.Status.ProviderID = in.ProviderID
	claim.Status.GPUs = it.GPUs
	claim.Status.LastError = ""
	claim.Status.Phase = v1alpha1.PhaseLaunched
	setCondition(&claim.Status.Conditions, v1alpha1.ConditionLaunched, true, "Launched", "instance "+in.ID, claim.Generation)
	return ctrl.Result{RequeueAfter: r.Settings.RegistrationTTL.Duration}, nil
}

func (r *ClaimReconciler) register(ctx context.Context, claim *v1alpha1.NodeClaim) (ctrl.Result, error) {
	node, err := r.nodeFor(ctx, claim)
	if err != nil {
		return ctrl.Result{}, err
	}
	if node == nil {
		since := conditionSince(claim.Status.Conditions, v1alpha1.ConditionLaunched, claim.CreationTimestamp.Time)
		if left := r.Settings.RegistrationTTL.Duration - r.now().Sub(since); left > 0 {
			return ctrl.Result{RequeueAfter: left}, nil
		}
		r.event(claim, corev1.EventTypeWarning, "RegistrationTimeout", fmt.Sprintf("no Node registered within %v", r.Settings.RegistrationTTL.Duration))
		return ctrl.Result{}, r.Delete(ctx, claim, "registration-timeout", true)
	}
	if err := r.syncNode(ctx, claim, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	claim.Status.NodeName = node.Name
	claim.Status.Phase = v1alpha1.PhaseRegistered
	setCondition(&claim.Status.Conditions, v1alpha1.ConditionRegistered, true, "Registered", "node "+node.Name, claim.Generation)
	return r.initialize(ctx, claim)
}

// syncNode copies labels and taints to the node, removes the unregistered taint and makes the
// claim the node's owner, in one update.
func (r *ClaimReconciler) syncNode(ctx context.Context, claim *v1alpha1.NodeClaim, node *corev1.Node) error {
	n := node.DeepCopy()
	if n.Labels == nil {
		n.Labels = map[string]string{}
	}
	for k, v := range claim.Spec.Labels {
		n.Labels[k] = v
	}
	n.Labels[v1alpha1.LabelPool] = claim.Spec.Pool
	n.Labels[v1alpha1.LabelFabric] = claim.Spec.Fabric
	n.Labels[v1alpha1.LabelNodeClaim] = claim.Name
	n.Labels[v1alpha1.LabelInstanceType] = claim.Spec.InstanceType
	taints := n.Spec.Taints[:0]
	for _, t := range n.Spec.Taints {
		if t.Key != v1alpha1.TaintUnregistered {
			taints = append(taints, t)
		}
	}
	for _, want := range claim.Spec.Taints {
		found := false
		for _, t := range taints {
			if t.MatchTaint(&want) {
				found = true
				break
			}
		}
		if !found {
			taints = append(taints, want)
		}
	}
	n.Spec.Taints = taints
	owner := metav1.OwnerReference{
		APIVersion: v1alpha1.GroupVersion.String(), Kind: "NodeClaim", Name: claim.Name, UID: claim.UID,
		BlockOwnerDeletion: ptr.To(true),
	}
	hasOwner := false
	for _, o := range n.OwnerReferences {
		if o.UID == claim.UID {
			hasOwner = true
		}
	}
	if !hasOwner {
		n.OwnerReferences = append(n.OwnerReferences, owner)
	}
	return r.Client.Update(ctx, n)
}

func (r *ClaimReconciler) initialize(ctx context.Context, claim *v1alpha1.NodeClaim) (ctrl.Result, error) {
	node := &corev1.Node{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: claim.Status.NodeName}, node); apierrors.IsNotFound(err) {
		return ctrl.Result{}, r.Delete(ctx, claim, "node-gone", true)
	} else if err != nil {
		return ctrl.Result{}, err
	}
	if NodeInitialized(node, claim.Status.GPUs) {
		claim.Status.Phase = v1alpha1.PhaseInitialized
		setCondition(&claim.Status.Conditions, v1alpha1.ConditionInitialized, true, "Initialized", "node ready with all GPUs", claim.Generation)
		return ctrl.Result{}, nil
	}
	since := conditionSince(claim.Status.Conditions, v1alpha1.ConditionRegistered, r.now())
	if left := r.Settings.InitializationTTL.Duration - r.now().Sub(since); left > 0 {
		return ctrl.Result{RequeueAfter: left}, nil
	}
	r.event(claim, corev1.EventTypeWarning, "InitializationTimeout", fmt.Sprintf("node did not become ready with %d GPUs within %v", claim.Status.GPUs, r.Settings.InitializationTTL.Duration))
	return ctrl.Result{}, r.Delete(ctx, claim, "initialization-timeout", true)
}

func (r *ClaimReconciler) health(ctx context.Context, claim *v1alpha1.NodeClaim) (ctrl.Result, error) {
	node := &corev1.Node{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: claim.Status.NodeName}, node); apierrors.IsNotFound(err) {
		return ctrl.Result{}, r.Delete(ctx, claim, "node-gone", true)
	} else if err != nil {
		return ctrl.Result{}, err
	}
	ready := nodeCondition(node, corev1.NodeReady)
	if ready != nil && ready.Status == corev1.ConditionTrue {
		return ctrl.Result{}, nil
	}
	since := claim.CreationTimestamp.Time
	if ready != nil {
		since = ready.LastTransitionTime.Time
	}
	if left := r.Settings.RepairAfter.Duration - r.now().Sub(since); left > 0 {
		return ctrl.Result{RequeueAfter: left}, nil
	}
	r.event(claim, corev1.EventTypeWarning, "Repair", fmt.Sprintf("node NotReady for more than %v", r.Settings.RepairAfter.Duration))
	return ctrl.Result{}, r.Delete(ctx, claim, "repair", true)
}

// Delete marks a claim with a reason and deletes it. force skips eviction during termination
// (the node is broken or never ran anything).
func (r *ClaimReconciler) Delete(ctx context.Context, claim *v1alpha1.NodeClaim, reason string, force bool) error {
	return DeleteClaim(ctx, r.Client, claim, reason, force)
}

// DeleteClaim marks a claim with a reason and deletes it.
func DeleteClaim(ctx context.Context, c client.Client, claim *v1alpha1.NodeClaim, reason string, force bool) error {
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q,%q:%q}}}`, AnnotationTerminationReason, reason, AnnotationForce, fmt.Sprint(force))
	if err := c.Patch(ctx, claim, client.RawPatch(types.MergePatchType, []byte(patch))); err != nil {
		return client.IgnoreNotFound(err)
	}
	if err := c.Delete(ctx, claim); err != nil {
		return client.IgnoreNotFound(err)
	}
	claim.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	return nil
}

func (r *ClaimReconciler) finalize(ctx context.Context, claim *v1alpha1.NodeClaim) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(claim, v1alpha1.Finalizer) {
		return ctrl.Result{}, nil
	}
	logger := log.FromContext(ctx)
	if claim.Status.Phase != v1alpha1.PhaseTerminating {
		claim.Status.Phase = v1alpha1.PhaseTerminating
		if err := r.Client.Status().Update(ctx, claim); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		if r.OnChange != nil {
			r.OnChange()
		}
	}
	force := claim.Annotations[AnnotationForce] == "true"
	node, err := r.nodeFor(ctx, claim)
	if err != nil {
		return ctrl.Result{}, err
	}
	if node != nil {
		if !node.Spec.Unschedulable {
			if err := Cordon(ctx, r.Client, node); err != nil {
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
		}
		left, err := Drain(ctx, r.Client, node.Name, force)
		if err != nil {
			return ctrl.Result{}, err
		}
		if left > 0 {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
	}
	if claim.Status.InstanceID != "" {
		if err := r.Cloud.Provider.Delete(ctx, claim.Status.InstanceID); err != nil && !cloud.IsNotFound(err) {
			logger.Info("cloud delete failed, retrying", "claim", claim.Name, "err", err.Error())
			return ctrl.Result{RequeueAfter: r.Settings.LaunchBackoff.Duration}, nil
		}
	}
	if node != nil {
		if err := r.Client.Delete(ctx, node); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	controllerutil.RemoveFinalizer(claim, v1alpha1.Finalizer)
	if err := r.Client.Update(ctx, claim); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	reason := claim.Annotations[AnnotationTerminationReason]
	if reason == "" {
		reason = "deleted"
	}
	metrics.ClaimsDeleted.WithLabelValues(claim.Spec.Pool, reason).Inc()
	return ctrl.Result{}, nil
}

// nodeFor finds the claim's Node by name once registered, else by provider ID.
func (r *ClaimReconciler) nodeFor(ctx context.Context, claim *v1alpha1.NodeClaim) (*corev1.Node, error) {
	if claim.Status.NodeName != "" {
		node := &corev1.Node{}
		err := r.Client.Get(ctx, types.NamespacedName{Name: claim.Status.NodeName}, node)
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return node, err
	}
	if claim.Status.ProviderID == "" {
		return nil, nil
	}
	var nodes corev1.NodeList
	if err := r.Client.List(ctx, &nodes, client.MatchingFields{IndexNodeProviderID: claim.Status.ProviderID}); err != nil {
		return nil, err
	}
	if len(nodes.Items) == 0 {
		return nil, nil
	}
	return &nodes.Items[0], nil
}

func (r *ClaimReconciler) event(claim *v1alpha1.NodeClaim, kind, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(claim, kind, reason, msg)
	}
}

// Cordon marks a node unschedulable.
func Cordon(ctx context.Context, c client.Client, node *corev1.Node) error {
	return c.Patch(ctx, node, client.RawPatch(types.MergePatchType, []byte(`{"spec":{"unschedulable":true}}`)))
}

// Drain evicts (or, with force, deletes) every pod on a node except DaemonSet pods, and returns
// how many are left. Evictions go through the Eviction API, so PodDisruptionBudgets hold.
func Drain(ctx context.Context, c client.Client, nodeName string, force bool) (int, error) {
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.MatchingFields{IndexPodNode: nodeName}); err != nil {
		return 0, err
	}
	left := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if podutil.DaemonSet(pod) || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		left++
		if pod.DeletionTimestamp != nil {
			continue
		}
		if force {
			if err := c.Delete(ctx, pod, client.GracePeriodSeconds(0)); err != nil && !apierrors.IsNotFound(err) {
				return left, err
			}
			continue
		}
		ev := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name}}
		err := c.SubResource("eviction").Create(ctx, pod, ev)
		switch {
		case err == nil:
			metrics.Evictions.WithLabelValues("ok").Inc()
		case apierrors.IsTooManyRequests(err):
			metrics.Evictions.WithLabelValues("blocked").Inc()
		case apierrors.IsNotFound(err):
		default:
			metrics.Evictions.WithLabelValues("error").Inc()
			return left, err
		}
	}
	return left, nil
}

// NodeInitialized: Ready, without startup or not-ready taints, and advertising every GPU.
func NodeInitialized(node *corev1.Node, gpus int32) bool {
	ready := nodeCondition(node, corev1.NodeReady)
	if ready == nil || ready.Status != corev1.ConditionTrue {
		return false
	}
	for _, t := range node.Spec.Taints {
		if t.Key == v1alpha1.TaintUnregistered || t.Key == corev1.TaintNodeNotReady || t.Key == corev1.TaintNodeUnreachable {
			return false
		}
	}
	q := node.Status.Allocatable[corev1.ResourceName(v1alpha1.ResourceGPU)]
	return q.Value() >= int64(gpus)
}

func nodeCondition(node *corev1.Node, t corev1.NodeConditionType) *corev1.NodeCondition {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == t {
			return &node.Status.Conditions[i]
		}
	}
	return nil
}

func setCondition(conds *[]metav1.Condition, t string, ok bool, reason, msg string, gen int64) {
	status := metav1.ConditionFalse
	if ok {
		status = metav1.ConditionTrue
	}
	if !ok {
		reason = "No" + reason
		msg = ""
	}
	meta.SetStatusCondition(conds, metav1.Condition{Type: t, Status: status, Reason: reason, Message: msg, ObservedGeneration: gen})
}

func conditionSince(conds []metav1.Condition, t string, fallback time.Time) time.Time {
	if c := meta.FindStatusCondition(conds, t); c != nil {
		return c.LastTransitionTime.Time
	}
	return fallback
}

func statusEqual(a, b *v1alpha1.NodeClaimStatus) bool {
	if a.Phase != b.Phase || a.InstanceID != b.InstanceID || a.ProviderID != b.ProviderID || a.NodeName != b.NodeName ||
		a.GPUs != b.GPUs || a.LaunchAttempts != b.LaunchAttempts || a.LastError != b.LastError || len(a.Conditions) != len(b.Conditions) {
		return false
	}
	for i := range a.Conditions {
		x, y := a.Conditions[i], b.Conditions[i]
		if x.Type != y.Type || x.Status != y.Status || x.Reason != y.Reason || x.Message != y.Message {
			return false
		}
	}
	return true
}
