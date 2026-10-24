package plan

import (
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
)

// GangInfo describes gang members that are already bound to nodes (not in Snapshot.Pods).
type GangInfo struct {
	Bound  int
	Fabric string
}

// nstate is a node during planning: an existing node or a "virtual" one this plan creates.
type nstate struct {
	claim    string
	pool     string
	fabric   string
	free     Resources
	ready    bool
	virtual  bool
	dead     bool
	reason   string
	reserved bool
}

// slots holds the nodes of one pool in one fabric that can take more pods, bucketed by free GPUs.
// Buckets may hold stale entries (a node whose free GPUs changed); they are skipped and dropped.
type slots struct {
	ready    [][]*nstate
	inflight [][]*nstate
}

type gang struct {
	key      string
	size     int
	pending  []*Pod
	bound    int
	fixed    string // fabric the gang is bound to (released or bound members), or ""
	planned  string
	priority int32
	created  time.Time
}

type planner struct {
	s         *Snapshot
	pools     []*Pool
	poolByN   map[string]*Pool
	fabrics   map[string][]string // pool -> fabrics it may use
	slots     map[string]*slots
	poolCount map[string]int32
	avail     map[string]int32 // fabric/type -> hosts left
	quota     int64
	maxGPU    int32
	templates map[string]*corev1.Node
	undo      []func()
	virtuals  []*nstate
	existing  []*nstate
	seq       int
	out       *Plan
}

// Compute makes a plan. It never mutates the snapshot.
func Compute(s *Snapshot, gangs map[string]GangInfo) *Plan {
	p := &planner{
		s: s, poolByN: map[string]*Pool{}, fabrics: map[string][]string{}, slots: map[string]*slots{},
		poolCount: map[string]int32{}, avail: map[string]int32{}, quota: s.QuotaFreeGPUs,
		templates: map[string]*corev1.Node{}, out: &Plan{Reserved: map[string]bool{}},
	}
	p.index()
	p.loadNodes()
	gs, singles := p.group(gangs)
	p.out.Stats.Pods = len(s.Pods)
	p.out.Stats.Gangs = len(gs)
	for _, g := range gs {
		p.placeGang(g)
	}
	for _, pod := range singles {
		p.placeSingle(pod)
	}
	p.minNodes()
	p.collect()
	return p.out
}

func (p *planner) index() {
	for i := range p.s.Pools {
		pool := &p.s.Pools[i]
		p.pools = append(p.pools, pool)
		p.poolByN[pool.Name] = pool
		if pool.InstanceType.GPUs > p.maxGPU {
			p.maxGPU = pool.InstanceType.GPUs
		}
	}
	sort.SliceStable(p.pools, func(i, j int) bool {
		if p.pools[i].Weight != p.pools[j].Weight {
			return p.pools[i].Weight > p.pools[j].Weight
		}
		return p.pools[i].Name < p.pools[j].Name
	})
	for f, types := range p.s.Available {
		for t, n := range types {
			p.avail[UnavailableKey(f, t)] = n
		}
	}
	for _, pool := range p.pools {
		var fs []string
		if len(pool.Fabrics) > 0 {
			fs = append(fs, pool.Fabrics...)
		} else {
			for f, types := range p.s.Available {
				if _, ok := types[pool.InstanceType.Name]; ok {
					fs = append(fs, f)
				}
			}
		}
		sort.Strings(fs)
		p.fabrics[pool.Name] = fs
	}
}

func (p *planner) slotsOf(pool, fabric string) *slots {
	k := pool + "|" + fabric
	sl := p.slots[k]
	if sl == nil {
		sl = &slots{ready: make([][]*nstate, p.maxGPU+1), inflight: make([][]*nstate, p.maxGPU+1)}
		p.slots[k] = sl
	}
	return sl
}

func (p *planner) put(n *nstate) {
	if n.dead || n.free.GPU <= 0 {
		return
	}
	sl := p.slotsOf(n.pool, n.fabric)
	g := n.free.GPU
	if g > p.maxGPU {
		g = p.maxGPU
	}
	if n.ready {
		sl.ready[g] = append(sl.ready[g], n)
	} else {
		sl.inflight[g] = append(sl.inflight[g], n)
	}
}

func (p *planner) loadNodes() {
	for i := range p.s.Nodes {
		nd := &p.s.Nodes[i]
		p.poolCount[nd.Pool]++
		pool := p.poolByN[nd.Pool]
		if pool != nil && !nd.Launched {
			// The cloud has not seen this claim yet, so its offering still includes the host.
			p.avail[UnavailableKey(nd.Fabric, pool.InstanceType.Name)]--
		}
		if nd.Initialized {
			p.out.Stats.InitializedNodes++
		} else {
			p.out.Stats.InFlightNodes++
		}
		if !nd.Schedulable || pool == nil {
			continue
		}
		n := &nstate{claim: nd.Claim, pool: nd.Pool, fabric: nd.Fabric, free: nd.Capacity.sub(nd.Used), ready: nd.Initialized}
		p.existing = append(p.existing, n)
		p.put(n)
	}
}

// take removes and returns the best-fitting node (fewest free GPUs that still fit) from buckets.
func (p *planner) take(buckets [][]*nstate, req Resources) *nstate {
	for g := req.GPU; g < int32(len(buckets)); g++ {
		b := buckets[g]
		scanned := 0
		for i := len(b) - 1; i >= 0 && scanned < 64; i-- {
			n := b[i]
			if n.dead || n.free.GPU != g {
				b[i] = b[len(b)-1]
				b = b[:len(b)-1]
				continue
			}
			scanned++
			if req.fits(n.free) {
				b[i] = b[len(b)-1]
				buckets[g] = b[:len(b)-1]
				return n
			}
		}
		buckets[g] = b
	}
	return nil
}

func (p *planner) assign(n *nstate, req Resources) {
	prev, prevRes := n.free, n.reserved
	n.free = n.free.sub(req)
	if !n.virtual {
		n.reserved = true
	}
	p.put(n)
	p.undo = append(p.undo, func() {
		n.free, n.reserved = prev, prevRes
		p.put(n)
	})
}

// cannotCreate says why no new node of pool can be made in fabric, or "".
func (p *planner) cannotCreate(pool *Pool, fabric string) string {
	k := UnavailableKey(fabric, pool.InstanceType.Name)
	switch {
	case p.s.Unavailable[k]:
		return fmt.Sprintf("%s recently reported no %s capacity", fabric, pool.InstanceType.Name)
	case p.avail[k] <= 0:
		return fmt.Sprintf("%s has no free %s hosts", fabric, pool.InstanceType.Name)
	case p.quota < int64(pool.InstanceType.GPUs):
		return fmt.Sprintf("GPU quota allows %d more GPUs", p.quota)
	case pool.MaxNodes != nil && p.poolCount[pool.Name] >= *pool.MaxNodes:
		return fmt.Sprintf("pool %s is at maxNodes %d", pool.Name, *pool.MaxNodes)
	}
	return ""
}

func (p *planner) create(pool *Pool, fabric, reason string) *nstate {
	k := UnavailableKey(fabric, pool.InstanceType.Name)
	it := pool.InstanceType
	p.seq++
	n := &nstate{
		claim: fmt.Sprintf("+%d", p.seq), pool: pool.Name, fabric: fabric, virtual: true, reason: reason,
		free: Resources{GPU: it.GPUs, MilliCPU: it.MilliCPU, Memory: it.MemoryBytes},
	}
	p.poolCount[pool.Name]++
	p.avail[k]--
	p.quota -= int64(it.GPUs)
	p.virtuals = append(p.virtuals, n)
	p.undo = append(p.undo, func() {
		n.dead = true
		p.poolCount[pool.Name]--
		p.avail[k]++
		p.quota += int64(it.GPUs)
	})
	return n
}

func (p *planner) rollback(mark int) {
	for i := len(p.undo) - 1; i >= mark; i-- {
		p.undo[i]()
	}
	p.undo = p.undo[:mark]
}

type fit struct {
	ok       bool
	newNodes int
	allReady bool
	placed   int
	why      string
}

// fitAll places every request into pool/fabric: initialized nodes first, then in-flight ones,
// then new nodes. It is all or nothing only together with rollback.
func (p *planner) fitAll(pool *Pool, fabric string, reqs []Resources, reason string) fit {
	sl := p.slotsOf(pool.Name, fabric)
	it := pool.InstanceType
	nodeCap := Resources{GPU: it.GPUs, MilliCPU: it.MilliCPU, Memory: it.MemoryBytes}
	f := fit{ok: true, allReady: true}
	for _, r := range reqs {
		if !r.fits(nodeCap) {
			return fit{why: fmt.Sprintf("a pod requests more than one %s has", it.Name), placed: f.placed}
		}
		n := p.take(sl.ready, r)
		if n == nil {
			f.allReady = false
			n = p.take(sl.inflight, r)
		}
		if n == nil {
			if why := p.cannotCreate(pool, fabric); why != "" {
				return fit{why: why, placed: f.placed, newNodes: f.newNodes}
			}
			n = p.create(pool, fabric, reason)
			f.newNodes++
		}
		p.assign(n, r)
		f.placed++
	}
	return f
}

func (p *planner) template(pool *Pool, fabric string) *corev1.Node {
	k := pool.Name + "|" + fabric
	if n, ok := p.templates[k]; ok {
		return n
	}
	labels := map[string]string{}
	for k, v := range pool.Labels {
		labels[k] = v
	}
	labels[v1alpha1.LabelPool] = pool.Name
	labels[v1alpha1.LabelFabric] = fabric
	labels[v1alpha1.LabelInstanceType] = pool.InstanceType.Name
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.NodeSpec{Taints: pool.Taints}}
	p.templates[k] = n
	return n
}

// matches reports whether a pod could run on a node of pool in fabric.
func (p *planner) matches(spec *corev1.PodSpec, pool *Pool, fabric string) bool {
	if spec == nil {
		return true
	}
	n := p.template(pool, fabric)
	_, untolerated := corev1helpers.FindMatchingUntoleratedTaint(n.Spec.Taints, spec.Tolerations, func(t *corev1.Taint) bool {
		return t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute
	})
	if untolerated {
		return false
	}
	ok, err := nodeaffinity.GetRequiredNodeAffinity(&corev1.Pod{Spec: *spec}).Match(n)
	return err == nil && ok
}

type candidate struct {
	pool   *Pool
	fabric string
}

func (p *planner) candidates(spec *corev1.PodSpec, fixed string) []candidate {
	var out []candidate
	for _, pool := range p.pools {
		for _, f := range p.fabrics[pool.Name] {
			if fixed != "" && f != fixed {
				continue
			}
			if p.matches(spec, pool, f) {
				out = append(out, candidate{pool, f})
			}
		}
	}
	return out
}

func (p *planner) group(info map[string]GangInfo) ([]*gang, []*Pod) {
	byKey := map[string]*gang{}
	var singles []*Pod
	for i := range p.s.Pods {
		pod := &p.s.Pods[i]
		if pod.Gang == "" {
			singles = append(singles, pod)
			continue
		}
		g := byKey[pod.Gang]
		if g == nil {
			g = &gang{key: pod.Gang, created: pod.Created, priority: pod.Priority}
			if gi, ok := info[pod.Gang]; ok {
				g.bound, g.fixed = gi.Bound, gi.Fabric
			}
			byKey[pod.Gang] = g
		}
		g.pending = append(g.pending, pod)
		if pod.GangSize > g.size {
			g.size = pod.GangSize
		}
		if pod.Created.Before(g.created) {
			g.created = pod.Created
		}
		if pod.Priority > g.priority {
			g.priority = pod.Priority
		}
		if !pod.Gated && pod.Fabric != "" && g.fixed == "" {
			g.fixed = pod.Fabric
		}
		if pod.PlannedFabric != "" {
			g.planned = pod.PlannedFabric
		}
	}
	gs := make([]*gang, 0, len(byKey))
	for _, g := range byKey {
		sort.Slice(g.pending, func(i, j int) bool { return g.pending[i].Key < g.pending[j].Key })
		gs = append(gs, g)
	}
	sort.Slice(gs, func(i, j int) bool {
		a, b := gs[i], gs[j]
		if (a.fixed != "") != (b.fixed != "") {
			return a.fixed != "" // gangs that already started come first: they are committed
		}
		if a.priority != b.priority {
			return a.priority > b.priority
		}
		if !a.created.Equal(b.created) {
			return a.created.Before(b.created)
		}
		return a.key < b.key
	})
	sort.SliceStable(singles, func(i, j int) bool {
		a, b := singles[i], singles[j]
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if !a.Created.Equal(b.Created) {
			return a.Created.Before(b.Created)
		}
		return a.Key < b.Key
	})
	return gs, singles
}

func requests(pods []*Pod) []Resources {
	out := make([]Resources, len(pods))
	for i, pod := range pods {
		out[i] = pod.Requests
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].GPU > out[j].GPU })
	return out
}

func keys(pods []*Pod, gatedOnly bool) []string {
	var out []string
	for _, pod := range pods {
		if !gatedOnly || pod.Gated {
			out = append(out, pod.Key)
		}
	}
	return out
}

func (p *planner) wait(g *gang, reason string) {
	p.out.Stats.GangsWaiting++
	p.out.Waits = append(p.out.Waits, Wait{Gang: g.key, Reason: reason})
}

func (p *planner) placeGang(g *gang) {
	if have := len(g.pending) + g.bound; g.size > 0 && have < g.size {
		p.wait(g, fmt.Sprintf("waiting for members: %d of %d exist", have, g.size))
		return
	}
	cands := p.candidates(g.pending[0].Spec, g.fixed)
	if len(cands) == 0 {
		if g.fixed != "" {
			p.wait(g, fmt.Sprintf("no pool can place the gang's pods in fabric %s, where the gang runs", g.fixed))
		} else {
			p.wait(g, "no pool and fabric match the gang's pods (selectors, affinity, tolerations)")
		}
		return
	}
	reqs := requests(g.pending)
	type scored struct {
		c    candidate
		f    fit
		left int32
	}
	var best *scored
	var bestFail *scored
	for _, c := range cands {
		mark := len(p.undo)
		f := p.fitAll(c.pool, c.fabric, reqs, g.key)
		left := p.avail[UnavailableKey(c.fabric, c.pool.InstanceType.Name)]
		p.rollback(mark)
		s := &scored{c, f, left}
		if !f.ok {
			if bestFail == nil || f.placed > bestFail.f.placed {
				bestFail = s
			}
			continue
		}
		if best == nil || better(s.f, s.left, s.c, best.f, best.left, best.c, g.planned) {
			best = s
		}
	}
	if best == nil {
		p.wait(g, fmt.Sprintf("no single fabric can hold all %d pending members; closest is %s with room for %d (%s)",
			len(reqs), bestFail.c.fabric, bestFail.f.placed, bestFail.f.why))
		return
	}
	f := p.fitAll(best.c.pool, best.c.fabric, reqs, g.key)
	if !f.ok { // cannot happen: same state as the trial
		p.wait(g, "internal: trial and commit disagree")
		return
	}
	gated := keys(g.pending, true)
	if g.fixed == "" && g.planned != best.c.fabric {
		p.out.Placements = append(p.out.Placements, Placement{Gang: g.key, Fabric: best.c.fabric, Pods: keys(g.pending, false)})
	}
	if f.allReady && len(gated) > 0 {
		p.out.Releases = append(p.out.Releases, Release{Gang: g.key, Fabric: best.c.fabric, Pods: gated})
		p.out.Stats.GangsReleased++
	}
}

// better orders feasible gang placements: fewest new nodes; then all members on initialized
// nodes (the gang can start now); then the fabric with the fewest hosts left (best fit, which
// keeps large fabrics free for large gangs); then the fabric already planned; then pool weight
// and name for determinism.
func better(a fit, aLeft int32, ac candidate, b fit, bLeft int32, bc candidate, planned string) bool {
	if a.newNodes != b.newNodes {
		return a.newNodes < b.newNodes
	}
	if a.allReady != b.allReady {
		return a.allReady
	}
	if aLeft != bLeft {
		return aLeft < bLeft
	}
	if (ac.fabric == planned) != (bc.fabric == planned) {
		return ac.fabric == planned
	}
	if ac.pool.Weight != bc.pool.Weight {
		return ac.pool.Weight > bc.pool.Weight
	}
	if ac.pool.Name != bc.pool.Name {
		return ac.pool.Name < bc.pool.Name
	}
	return ac.fabric < bc.fabric
}

func (p *planner) placeSingle(pod *Pod) {
	cands := p.candidates(pod.Spec, "")
	if len(cands) == 0 {
		p.out.Waits = append(p.out.Waits, Wait{Pod: pod.Key, Reason: "no pool and fabric match the pod"})
		return
	}
	// Fabrics with the fewest free hosts first: small work fills fragmented fabrics.
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].pool.Weight != cands[j].pool.Weight {
			return cands[i].pool.Weight > cands[j].pool.Weight
		}
		ai := p.avail[UnavailableKey(cands[i].fabric, cands[i].pool.InstanceType.Name)]
		aj := p.avail[UnavailableKey(cands[j].fabric, cands[j].pool.InstanceType.Name)]
		if ai != aj {
			return ai < aj
		}
		return cands[i].fabric < cands[j].fabric
	})
	for _, ready := range []bool{true, false} {
		for _, c := range cands {
			sl := p.slotsOf(c.pool.Name, c.fabric)
			b := sl.inflight
			if ready {
				b = sl.ready
			}
			if n := p.take(b, pod.Requests); n != nil {
				p.assign(n, pod.Requests)
				p.out.Stats.SingletonsPlaced++
				return
			}
		}
	}
	var whys []string
	for _, c := range cands {
		if why := p.cannotCreate(c.pool, c.fabric); why != "" {
			whys = append(whys, why)
			continue
		}
		mark := len(p.undo)
		if f := p.fitAll(c.pool, c.fabric, []Resources{pod.Requests}, "singleton"); f.ok {
			p.out.Stats.SingletonsPlaced++
			return
		} else {
			whys = append(whys, f.why)
		}
		p.rollback(mark)
	}
	p.out.Waits = append(p.out.Waits, Wait{Pod: pod.Key, Reason: "no capacity: " + strings.Join(dedupe(whys), "; ")})
}

func (p *planner) minNodes() {
	for _, pool := range p.pools {
		for p.poolCount[pool.Name] < pool.MinNodes {
			fs := append([]string(nil), p.fabrics[pool.Name]...)
			sort.SliceStable(fs, func(i, j int) bool {
				return p.avail[UnavailableKey(fs[i], pool.InstanceType.Name)] > p.avail[UnavailableKey(fs[j], pool.InstanceType.Name)]
			})
			made := false
			for _, f := range fs {
				if p.cannotCreate(pool, f) == "" {
					p.create(pool, f, "min-nodes")
					made = true
					break
				}
			}
			if !made {
				break
			}
		}
	}
}

func (p *planner) collect() {
	type key struct{ pool, fabric, reason string }
	counts := map[key]int{}
	var order []key
	for _, n := range p.virtuals {
		if n.dead {
			continue
		}
		k := key{n.pool, n.fabric, n.reason}
		if counts[k] == 0 {
			order = append(order, k)
		}
		counts[k]++
	}
	for _, k := range order {
		p.out.Create = append(p.out.Create, NewClaims{Pool: k.pool, Fabric: k.fabric, Count: counts[k], Reason: k.reason})
		p.out.Stats.NewNodes += counts[k]
	}
	for _, n := range p.existing {
		if n.reserved {
			p.out.Reserved[n.claim] = true
		}
	}
}

func dedupe(ss []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range ss {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
