package plan

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// bigSnapshot is a cluster at the scale the planner must handle: nodes spread over fabrics,
// most of them busy, with gangs and single pods waiting.
func bigSnapshot(nodes, fabrics, gangs, singles int) *Snapshot {
	rng := rand.New(rand.NewSource(1))
	avail := map[string]int32{}
	for f := 0; f < fabrics; f++ {
		avail[fmt.Sprintf("fab-%02d", f)] = int32(nodes/fabrics/5 + 50)
	}
	s := snap(avail)
	for i := 0; i < nodes; i++ {
		used := int32(8)
		switch r := rng.Float64(); {
		case r < 0.10:
			used = int32(rng.Intn(8))
		case r < 0.15:
			used = 0
		}
		n := node(fmt.Sprintf("n%05d", i), fmt.Sprintf("fab-%02d", i%fabrics), rng.Float64() > 0.05, used)
		s.Nodes = append(s.Nodes, n)
	}
	for g := 0; g < gangs; g++ {
		pods := gangPods(fmt.Sprintf("g%04d", g), 2+rng.Intn(31), 8, true)
		for i := range pods {
			pods[i].Created = t0.Add(time.Duration(g) * time.Second)
		}
		s.Pods = append(s.Pods, pods...)
	}
	for i := 0; i < singles; i++ {
		s.Pods = append(s.Pods, single(fmt.Sprintf("s%05d", i), int32(1+rng.Intn(4))))
	}
	return s
}

func TestPlanAtTwentyThousandNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test")
	}
	s := bigSnapshot(20000, 10, 400, 4000)
	start := time.Now()
	p := Compute(s, nil)
	took := time.Since(start)
	t.Logf("20,000 nodes, %d pending pods (%d gangs): %v; %d new nodes, %d gangs released, %d waiting",
		p.Stats.Pods, p.Stats.Gangs, took, p.Stats.NewNodes, p.Stats.GangsReleased, p.Stats.GangsWaiting)
	if took > 2*time.Second {
		t.Fatalf("plan took %v", took)
	}
}

func BenchmarkCompute(b *testing.B) {
	for _, n := range []int{1000, 5000, 20000} {
		s := bigSnapshot(n, 10, n/50, n/5)
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				Compute(s, nil)
			}
		})
	}
}
