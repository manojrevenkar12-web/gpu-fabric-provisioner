package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// TemplateHash identifies everything about a pool that is baked into a node when it is created:
// instance type, image, labels and taints. NodeClaims made under another hash are drifted.
// Fabrics, limits and disruption settings are not part of it: changing them never replaces a node.
func (p *GPUNodePool) TemplateHash() string {
	var b strings.Builder
	fmt.Fprintf(&b, "type=%s\nimage=%s\n", p.Spec.InstanceType, p.Spec.Image)
	keys := make([]string, 0, len(p.Spec.Labels))
	for k := range p.Spec.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "label=%s=%s\n", k, p.Spec.Labels[k])
	}
	taints := make([]string, 0, len(p.Spec.Taints))
	for _, t := range p.Spec.Taints {
		taints = append(taints, fmt.Sprintf("%s=%s:%s", t.Key, t.Value, t.Effect))
	}
	sort.Strings(taints)
	for _, t := range taints {
		fmt.Fprintf(&b, "taint=%s\n", t)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])[:16]
}

// ConsolidationEnabled reports whether empty or replaceable nodes of the pool may be removed.
func (p *GPUNodePool) ConsolidationEnabled() bool {
	return p.Spec.Disruption.Consolidation == nil || *p.Spec.Disruption.Consolidation
}
