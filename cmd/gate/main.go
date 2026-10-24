// Command gate checks drill results against config/gate.yaml and exits non-zero on failure.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"

	"sigs.k8s.io/yaml"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/drill"
)

type gate struct {
	Provisioner struct {
		TimedOut               bool    `json:"timedOut"`
		IncompleteJobs         int     `json:"incompleteJobs"`
		SplitGangs             int     `json:"splitGangs"`
		GangRestarts           int     `json:"gangRestarts"`
		LeakedInstances        int     `json:"leakedInstances"`
		DuplicateClaims        int     `json:"duplicateClaims"`
		PDBViolations          int     `json:"pdbViolations"`
		OldTemplateNodes       int     `json:"oldTemplateNodes"`
		MaxStrandedShareOfPaid float64 `json:"maxStrandedShareOfPaid"`
	} `json:"provisioner"`
	VersusNaive struct {
		MaxPaidRatio      float64  `json:"maxPaidRatio"`
		MaxInstancesRatio float64  `json:"maxInstancesRatio"`
		CostScenarios     []string `json:"costScenarios"`
	} `json:"versusNaive"`
	Expectations struct {
		Upgrade struct {
			MaxRolloutSeconds float64 `json:"maxRolloutSeconds"`
		} `json:"upgrade"`
	} `json:"expectations"`
	Required []string `json:"required"`
}

func main() {
	in := flag.String("in", "reports/drills.json", "drill results")
	cfg := flag.String("config", "config/gate.yaml", "gate thresholds")
	flag.Parse()
	var g gate
	b, err := os.ReadFile(*cfg)
	if err == nil {
		err = yaml.UnmarshalStrict(b, &g)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "::error::%s: %v\n", *cfg, err)
		os.Exit(2)
	}
	var results []*drill.Result
	b, err = os.ReadFile(*in)
	if err == nil {
		err = json.Unmarshal(b, &results)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "::error::%s: %v\n", *in, err)
		os.Exit(2)
	}
	fails, checks := 0, 0
	check := func(ok bool, format string, args ...any) {
		checks++
		if !ok {
			fails++
			fmt.Printf("::error::%s\n", fmt.Sprintf(format, args...))
		}
	}
	ours := map[string]*drill.Result{}
	naive := map[string]*drill.Result{}
	for _, r := range results {
		if r.Mode == drill.Naive {
			naive[r.Scenario] = r
		} else {
			ours[r.Scenario] = r
		}
	}
	for _, name := range g.Required {
		check(ours[name] != nil, "%s: no result for the provisioner", name)
	}
	names := make([]string, 0, len(ours))
	for n := range ours {
		names = append(names, n)
	}
	sort.Strings(names)
	p := g.Provisioner
	for _, name := range names {
		r := ours[name]
		check(r.TimedOut == p.TimedOut, "%s: timed out", name)
		check(r.Jobs-r.Completed <= p.IncompleteJobs, "%s: %d of %d jobs incomplete %v", name, r.Jobs-r.Completed, r.Jobs, r.Incomplete)
		check(r.SplitGangs <= p.SplitGangs, "%s: %d gangs split across fabrics", name, r.SplitGangs)
		check(r.GangRestarts <= p.GangRestarts, "%s: %d gang restarts", name, r.GangRestarts)
		check(r.LeakedInstances <= p.LeakedInstances, "%s: %d leaked instances", name, r.LeakedInstances)
		check(r.DuplicateClaims <= p.DuplicateClaims, "%s: %d claims got two instances", name, r.DuplicateClaims)
		check(r.PDBViolations <= p.PDBViolations, "%s: PDB violated in %d samples", name, r.PDBViolations)
		check(r.OldTemplateNodes <= p.OldTemplateNodes, "%s: %d nodes left on an old template", name, r.OldTemplateNodes)
		check(r.PaidGPUSeconds == 0 || r.StrandedGPUSeconds/r.PaidGPUSeconds <= p.MaxStrandedShareOfPaid,
			"%s: %.0f of %.0f paid GPU-seconds stranded in partly started gangs", name, r.StrandedGPUSeconds, r.PaidGPUSeconds)
		if n := naive[name]; n != nil {
			check(r.StrandedGPUSeconds < n.StrandedGPUSeconds || r.StrandedGPUSeconds == 0,
				"%s: stranded %.0f GPU-seconds, naive %.0f", name, r.StrandedGPUSeconds, n.StrandedGPUSeconds)
		}
		if n := naive[name]; n != nil && contains(g.VersusNaive.CostScenarios, name) {
			check(r.PaidGPUSeconds <= n.PaidGPUSeconds*g.VersusNaive.MaxPaidRatio,
				"%s: paid %.0f GPU-seconds, naive paid %.0f", name, r.PaidGPUSeconds, n.PaidGPUSeconds)
			check(float64(r.Instances) <= float64(n.Instances)*g.VersusNaive.MaxInstancesRatio,
				"%s: bought %d servers, naive bought %d", name, r.Instances, n.Instances)
		}
	}
	if r := ours["upgrade"]; r != nil {
		check(r.RolloutSeconds > 0 && r.RolloutSeconds <= g.Expectations.Upgrade.MaxRolloutSeconds,
			"upgrade: rollout took %.1f s", r.RolloutSeconds)
	}
	fmt.Printf("gate: %d checks, %d failed\n", checks, fails)
	if fails > 0 {
		os.Exit(1)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
