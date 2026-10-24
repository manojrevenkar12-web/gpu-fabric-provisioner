package plan

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/api/v1alpha1"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
)

var h100 = cloud.InstanceType{Name: "gpu-h100.8", GPUs: 8, MilliCPU: 128000, MemoryBytes: 1600 << 30}

var t0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func pool(name string) Pool {
	return Pool{Name: name, InstanceType: h100, TemplateHash: "h1"}
}

func snap(avail map[string]int32, pools ...Pool) *Snapshot {
	if len(pools) == 0 {
		pools = []Pool{pool("train")}
	}
	a := map[string]map[string]int32{}
	for f, n := range avail {
		a[f] = map[string]int32{h100.Name: n}
	}
	return &Snapshot{Pools: pools, Available: a, Unavailable: map[string]bool{}, QuotaFreeGPUs: 1 << 30}
}

func gangPods(name string, n int, gpus int32, gated bool) []Pod {
	var out []Pod
	for i := 0; i < n; i++ {
		out = append(out, Pod{
			Key: fmt.Sprintf("ns/%s-%d", name, i), Gang: "ns/" + name, GangSize: n, Gated: gated,
			Requests: Resources{GPU: gpus, MilliCPU: 1000, Memory: 1 << 30}, Created: t0, Spec: &corev1.PodSpec{},
		})
	}
	return out
}

func single(name string, gpus int32) Pod {
	return Pod{Key: "ns/" + name, Requests: Resources{GPU: gpus, MilliCPU: 100, Memory: 1 << 20}, Created: t0, Spec: &corev1.PodSpec{}}
}

func node(claim, fabric string, initialized bool, usedGPU int32) Node {
	return Node{
		Claim: claim, NodeName: claim, Pool: "train", Fabric: fabric, Initialized: initialized, Schedulable: true, Launched: true,
		Capacity: Resources{GPU: 8, MilliCPU: h100.MilliCPU, Memory: h100.MemoryBytes},
		Used:     Resources{GPU: usedGPU, MilliCPU: int64(usedGPU) * 1000, Memory: int64(usedGPU) << 30},
	}
}

func created(p *Plan) map[string]int {
	out := map[string]int{}
	for _, c := range p.Create {
		out[c.Fabric] += c.Count
	}
	return out
}

func TestGangGoesToTheFabricItFitsBest(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 10, "fab-b": 5})
	s.Pods = gangPods("job", 4, 8, true)
	p := Compute(s, nil)
	if got := created(p); !reflect.DeepEqual(got, map[string]int{"fab-b": 4}) {
		t.Fatalf("created %v, want 4 in fab-b (best fit keeps fab-a's 10 hosts together)", got)
	}
	if len(p.Placements) != 1 || p.Placements[0].Fabric != "fab-b" || len(p.Releases) != 0 {
		t.Fatalf("placements %+v releases %+v", p.Placements, p.Releases)
	}
}

func TestGangStartsOnlyWhenEveryMemberFitsOnInitializedNodes(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 10})
	s.Nodes = []Node{node("n1", "fab-a", true, 0), node("n2", "fab-a", true, 0), node("n3", "fab-a", true, 0), node("n4", "fab-a", false, 0)}
	s.Pods = gangPods("job", 4, 8, true)
	p := Compute(s, nil)
	if len(p.Releases) != 0 || len(p.Create) != 0 {
		t.Fatalf("3 of 4 nodes ready: want no release and no new node, got releases %+v create %+v", p.Releases, p.Create)
	}
	s.Nodes[3].Initialized = true
	p = Compute(s, nil)
	if len(p.Releases) != 1 || len(p.Releases[0].Pods) != 4 || p.Releases[0].Fabric != "fab-a" {
		t.Fatalf("want the whole gang released in fab-a, got %+v", p.Releases)
	}
	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		if !p.Reserved[n] {
			t.Fatalf("%s should be reserved: %v", n, p.Reserved)
		}
	}
}

// Nodes that are booting, or registered but not yet advertising GPUs, are capacity. Counting only
// allocatable GPUs of ready nodes is what makes autoscalers buy the same capacity twice.
func TestInFlightNodesAreCountedAsCapacity(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 10})
	for i := 0; i < 4; i++ {
		s.Nodes = append(s.Nodes, node(fmt.Sprintf("n%d", i), "fab-a", false, 0))
	}
	s.Nodes[0].Launched = false
	s.Pods = gangPods("job", 4, 8, true)
	for i := range s.Pods {
		s.Pods[i].PlannedFabric = "fab-a"
	}
	p := Compute(s, nil)
	if len(p.Create) != 0 || len(p.Releases) != 0 || len(p.Placements) != 0 {
		t.Fatalf("want nothing new, got %+v", p)
	}
}

func TestGangThatFitsNoSingleFabricGetsNothing(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 10, "fab-b": 12})
	s.Pods = gangPods("big", 16, 8, true)
	p := Compute(s, nil)
	if len(p.Create) != 0 {
		t.Fatalf("an infeasible gang must not buy partial capacity, got %+v", p.Create)
	}
	if len(p.Waits) != 1 || !strings.Contains(p.Waits[0].Reason, "fab-b with room for 12") {
		t.Fatalf("waits %+v", p.Waits)
	}
}

func TestQuotaIsAllOrNothingPerGang(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 10})
	s.QuotaFreeGPUs = 24
	s.Pods = append(gangPods("four", 4, 8, true), gangPods("two", 2, 8, true)...)
	for i := 4; i < 6; i++ {
		s.Pods[i].Created = t0.Add(time.Second)
	}
	p := Compute(s, nil)
	if got := created(p); got["fab-a"] != 2 {
		t.Fatalf("quota of 3 nodes: the 4-node gang waits and the 2-node gang gets 2, got %v (waits %+v)", got, p.Waits)
	}
	if len(p.Waits) != 1 || p.Waits[0].Gang != "ns/four" || !strings.Contains(p.Waits[0].Reason, "quota") {
		t.Fatalf("waits %+v", p.Waits)
	}
}

func TestRecentInsufficientCapacityMovesTheGang(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 10, "fab-b": 5})
	s.Unavailable[UnavailableKey("fab-b", h100.Name)] = true
	s.Pods = gangPods("job", 4, 8, true)
	for i := range s.Pods {
		s.Pods[i].PlannedFabric = "fab-b"
	}
	p := Compute(s, nil)
	if got := created(p); got["fab-a"] != 4 || len(p.Placements) != 1 || p.Placements[0].Fabric != "fab-a" {
		t.Fatalf("want gang moved to fab-a, got create %v placements %+v", got, p.Placements)
	}
}

func TestStartedGangStaysInItsFabric(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 0, "fab-b": 10})
	s.Nodes = []Node{node("n1", "fab-a", true, 8)}
	s.Pods = gangPods("job", 2, 8, true)[1:] // member 0 is bound in fab-a; member 1 was recreated
	p := Compute(s, map[string]GangInfo{"ns/job": {Bound: 1, Fabric: "fab-a"}})
	if len(p.Create) != 0 || len(p.Waits) != 1 || !strings.Contains(p.Waits[0].Reason, "no single fabric") {
		t.Fatalf("a started gang must not get capacity in another fabric: create %+v waits %+v", p.Create, p.Waits)
	}
}

func TestIncompleteGangWaits(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 10})
	s.Pods = gangPods("job", 4, 8, true)[:3]
	for i := range s.Pods {
		s.Pods[i].GangSize = 4
	}
	p := Compute(s, nil)
	if len(p.Create) != 0 || len(p.Waits) != 1 || !strings.Contains(p.Waits[0].Reason, "3 of 4") {
		t.Fatalf("got create %+v waits %+v", p.Create, p.Waits)
	}
}

func TestSingletonsPackBeforeNewNodes(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 10, "fab-b": 3})
	s.Nodes = []Node{node("n1", "fab-a", true, 0)}
	for i := 0; i < 10; i++ {
		s.Pods = append(s.Pods, single(fmt.Sprintf("p%d", i), 1))
	}
	p := Compute(s, nil)
	if got := created(p); !reflect.DeepEqual(got, map[string]int{"fab-b": 1}) {
		t.Fatalf("8 fit on n1, 2 need one new node in the fabric with fewest free hosts; got %v", got)
	}
	if p.Stats.SingletonsPlaced != 10 || !p.Reserved["n1"] {
		t.Fatalf("stats %+v reserved %v", p.Stats, p.Reserved)
	}
}

func TestTaintsAndSelectorsAreRespected(t *testing.T) {
	tainted := pool("train")
	tainted.Taints = []corev1.Taint{{Key: "nvidia.com/gpu", Effect: corev1.TaintEffectNoSchedule}}
	s := snap(map[string]int32{"fab-a": 10, "fab-b": 10}, tainted)
	s.Pods = []Pod{single("untolerating", 1)}
	if p := Compute(s, nil); len(p.Create) != 0 || len(p.Waits) != 1 {
		t.Fatalf("pod without toleration: %+v", p)
	}
	pod := single("pinned", 1)
	pod.Spec = &corev1.PodSpec{
		Tolerations:  []corev1.Toleration{{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists}},
		NodeSelector: map[string]string{v1alpha1.LabelFabric: "fab-b"},
	}
	s.Pods = []Pod{pod}
	if got := created(Compute(s, nil)); !reflect.DeepEqual(got, map[string]int{"fab-b": 1}) {
		t.Fatalf("pinned pod: %v", got)
	}
}

func TestPoolLimits(t *testing.T) {
	limited := pool("train")
	max := int32(3)
	limited.MaxNodes = &max
	limited.MinNodes = 2
	s := snap(map[string]int32{"fab-a": 10, "fab-b": 20}, limited)
	if got := created(Compute(s, nil)); !reflect.DeepEqual(got, map[string]int{"fab-b": 2}) {
		t.Fatalf("minNodes go to the fabric with most room: %v", got)
	}
	s.Pods = gangPods("job", 4, 8, true)
	p := Compute(s, nil)
	if len(p.Waits) != 1 || !strings.Contains(p.Waits[0].Reason, "maxNodes") {
		t.Fatalf("waits %+v", p.Waits)
	}
}

func TestHigherPriorityGangGetsScarceCapacity(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 4})
	low := gangPods("low", 4, 8, true)
	high := gangPods("high", 4, 8, true)
	for i := range high {
		high[i].Priority = 100
		high[i].Created = t0.Add(time.Hour)
	}
	s.Pods = append(low, high...)
	p := Compute(s, nil)
	if len(p.Placements) != 1 || p.Placements[0].Gang != "ns/high" {
		t.Fatalf("placements %+v", p.Placements)
	}
}

func TestDriftedNodesTakeNoNewWork(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 10})
	n := node("old", "fab-a", true, 0)
	n.Schedulable = false
	s.Nodes = []Node{n}
	s.Pods = []Pod{single("p", 1)}
	if got := created(Compute(s, nil)); got["fab-a"] != 1 {
		t.Fatalf("got %v", got)
	}
}

func TestClaimsTheCloudHasNotSeenReduceOfferings(t *testing.T) {
	s := snap(map[string]int32{"fab-a": 2})
	n := node("pending", "fab-a", false, 8)
	n.Launched = false
	s.Nodes = []Node{n}
	s.Pods = gangPods("job", 2, 8, true)
	p := Compute(s, nil)
	if len(p.Create) != 0 || !strings.Contains(p.Waits[0].Reason, "room for 1") {
		t.Fatalf("got create %+v waits %+v", p.Create, p.Waits)
	}
}

func TestPlansAreDeterministic(t *testing.T) {
	build := func() *Snapshot {
		s := snap(map[string]int32{"fab-a": 30, "fab-b": 30, "fab-c": 7})
		for i := 0; i < 20; i++ {
			s.Nodes = append(s.Nodes, node(fmt.Sprintf("n%02d", i), []string{"fab-a", "fab-b", "fab-c"}[i%3], i%2 == 0, int32(i%9)))
		}
		for i := 0; i < 6; i++ {
			s.Pods = append(s.Pods, gangPods(fmt.Sprintf("g%d", i), 2+i, 8, true)...)
		}
		for i := 0; i < 30; i++ {
			s.Pods = append(s.Pods, single(fmt.Sprintf("s%02d", i), int32(1+i%4)))
		}
		return s
	}
	a, b := Compute(build(), nil), Compute(build(), nil)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("plans differ:\n%+v\n%+v", a, b)
	}
}
