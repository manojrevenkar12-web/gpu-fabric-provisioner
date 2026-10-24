package main

import (
	"os/exec"
	"strings"
	"testing"
)

// The production binary must not link the simulator, the drills or the naive baseline.
func TestManagerDoesNotImportTheSimulator(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	for _, dep := range strings.Fields(string(out)) {
		for _, banned := range []string{"/internal/sim", "/internal/simenv", "/internal/drill", "/internal/naive", "controller-runtime/pkg/envtest"} {
			if strings.HasSuffix(dep, banned) || strings.Contains(dep, banned+"/") {
				t.Errorf("cmd/manager depends on %s", dep)
			}
		}
	}
}
