// Command drill runs the fault drills against a real API server (envtest) and writes the
// results as JSON and a Markdown report.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/drill"
)

func main() {
	only := flag.String("only", "", "comma-separated scenario names (default: all)")
	out := flag.String("out", "reports/drills.json", "results JSON")
	report := flag.String("report", "reports/DRILLS.md", "Markdown report ('' to skip)")
	naive := flag.Bool("naive", true, "also run the naive baseline where the scenario compares")
	flag.Parse()
	ctrllog.SetLogger(logr.Discard())

	want := map[string]bool{}
	for _, n := range strings.Split(*only, ",") {
		if n != "" {
			want[n] = true
		}
	}
	var results []*drill.Result
	ctx := context.Background()
	start := time.Now()
	for _, sc := range drill.Scenarios() {
		if len(want) > 0 && !want[sc.Name] {
			continue
		}
		modes := []drill.Mode{drill.Provisioner}
		if sc.CompareNaive && *naive {
			modes = append(modes, drill.Naive)
		}
		for _, mode := range modes {
			fmt.Fprintf(os.Stderr, "== %s / %s: %s\n", sc.Name, mode, sc.Summary)
			t := time.Now()
			r, err := drill.Run(ctx, sc, mode, os.Stderr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "::error::drill %s/%s: %v\n", sc.Name, mode, err)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "   %d/%d jobs, %d instances, paid %.0f GPU-s, useful %.0f%%, stranded %.0f GPU-s, split gangs %d, leaks %d, dupes %d (%.0fs)\n",
				r.Completed, r.Jobs, r.Instances, r.PaidGPUSeconds, 100*r.Utilization, r.StrandedGPUSeconds, r.SplitGangs, r.LeakedInstances, r.DuplicateClaims, time.Since(t).Seconds())
			results = append(results, r)
		}
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b, _ := json.MarshalIndent(results, "", "  ")
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *report != "" {
		if err := os.WriteFile(*report, []byte(drill.Markdown(results)), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d runs, %.0fs)\n", *out, len(results), time.Since(start).Seconds())
}
