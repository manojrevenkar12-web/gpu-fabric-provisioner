package controllers_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/go-logr/logr/funcr"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

func TestMain(m *testing.M) {
	verbosity := -1
	if os.Getenv("GFP_LOG") != "" {
		verbosity = 1
	}
	ctrllog.SetLogger(funcr.New(func(prefix, args string) {
		if verbosity >= 0 {
			fmt.Fprintln(os.Stderr, prefix, args)
		}
	}, funcr.Options{Verbosity: verbosity}))
	os.Exit(m.Run())
}
