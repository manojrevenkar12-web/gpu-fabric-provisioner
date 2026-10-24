// Package config holds the controller's operational settings. Business limits (pool sizes,
// disruption budgets) live on GPUNodePool objects; these are timings and client limits that an
// operator tunes per installation, read from config/controller.yaml.
package config

import (
	"fmt"
	"os"
	"time"

	"sigs.k8s.io/yaml"
)

// Duration is a time.Duration that reads "90s" or "5m" from YAML.
type Duration struct{ time.Duration }

// UnmarshalJSON implements json.Unmarshaler (sigs.k8s.io/yaml converts YAML to JSON first).
func (d *Duration) UnmarshalJSON(b []byte) error {
	s := string(b)
	if len(s) >= 2 && s[0] == '"' {
		s = s[1 : len(s)-1]
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration %q: %w", s, err)
	}
	d.Duration = v
	return nil
}

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) { return []byte(`"` + d.String() + `"`), nil }

// D builds a Duration.
func D(v time.Duration) Duration { return Duration{v} }

// Settings are the controller's timings and limits.
type Settings struct {
	// BatchIdle: the provisioner plans once no new pod arrived for this long...
	BatchIdle Duration `json:"batchIdle"`
	// BatchMax: ...or once this long has passed since the first pod of the batch.
	BatchMax Duration `json:"batchMax"`
	// Resync plans even without events, to retry what failed.
	Resync Duration `json:"resync"`
	// RegistrationTTL: a launched server whose Node has not appeared by then is replaced.
	RegistrationTTL Duration `json:"registrationTTL"`
	// InitializationTTL: a registered node that does not advertise all its GPUs by then is replaced.
	InitializationTTL Duration `json:"initializationTTL"`
	// RepairAfter: an initialized node that is NotReady this long is replaced.
	RepairAfter Duration `json:"repairAfter"`
	// LaunchRetries: transient create failures before a claim is given up.
	LaunchRetries int32 `json:"launchRetries"`
	// LaunchBackoff is the first retry delay; it doubles per attempt.
	LaunchBackoff Duration `json:"launchBackoff"`
	// UnavailableTTL: how long a fabric that answered InsufficientCapacity is skipped.
	UnavailableTTL Duration `json:"unavailableTTL"`
	// OfferingsRefresh: how often cloud offerings and quota are read.
	OfferingsRefresh Duration `json:"offeringsRefresh"`
	// DisruptionInterval: how often consolidation and drift run.
	DisruptionInterval Duration `json:"disruptionInterval"`
	// GCInterval: how often leaked instances and orphaned Nodes are collected.
	GCInterval Duration `json:"gcInterval"`
	// GCGrace: an instance must be at least this old before GC may delete it.
	GCGrace Duration `json:"gcGrace"`
	// CreateConcurrency bounds parallel NodeClaim creations per plan.
	CreateConcurrency int `json:"createConcurrency"`
	// MaxCreatesPerPlan bounds how many claims one plan may create (a safety valve).
	MaxCreatesPerPlan int `json:"maxCreatesPerPlan"`
	// ClaimWorkers is the NodeClaim controller's concurrency.
	ClaimWorkers int `json:"claimWorkers"`
	// PoolStatusInterval: a pool's status is written at most this often.
	PoolStatusInterval Duration `json:"poolStatusInterval"`
	// KubeQPS and KubeBurst limit the controller's API server client.
	KubeQPS   float32 `json:"kubeQPS"`
	KubeBurst int     `json:"kubeBurst"`
}

// Defaults are production settings.
func Defaults() Settings {
	return Settings{
		BatchIdle:          D(time.Second),
		BatchMax:           D(10 * time.Second),
		Resync:             D(30 * time.Second),
		RegistrationTTL:    D(15 * time.Minute),
		InitializationTTL:  D(15 * time.Minute),
		RepairAfter:        D(10 * time.Minute),
		LaunchRetries:      5,
		LaunchBackoff:      D(5 * time.Second),
		UnavailableTTL:     D(3 * time.Minute),
		OfferingsRefresh:   D(15 * time.Second),
		DisruptionInterval: D(15 * time.Second),
		GCInterval:         D(2 * time.Minute),
		GCGrace:            D(5 * time.Minute),
		CreateConcurrency:  50,
		MaxCreatesPerPlan:  1000,
		ClaimWorkers:       50,
		PoolStatusInterval: D(5 * time.Second),
		KubeQPS:            200,
		KubeBurst:          400,
	}
}

// Load reads settings from a YAML file over the defaults.
func Load(path string) (Settings, error) {
	s := Defaults()
	if path == "" {
		return s, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	if err := yaml.UnmarshalStrict(b, &s); err != nil {
		return s, fmt.Errorf("%s: %w", path, err)
	}
	return s, s.Validate()
}

// Validate rejects settings that cannot work.
func (s Settings) Validate() error {
	switch {
	case s.BatchIdle.Duration <= 0 || s.BatchMax.Duration < s.BatchIdle.Duration:
		return fmt.Errorf("batchIdle must be > 0 and <= batchMax")
	case s.CreateConcurrency < 1 || s.ClaimWorkers < 1:
		return fmt.Errorf("createConcurrency and claimWorkers must be >= 1")
	case s.MaxCreatesPerPlan < 1:
		return fmt.Errorf("maxCreatesPerPlan must be >= 1")
	case s.GCGrace.Duration <= 0:
		return fmt.Errorf("gcGrace must be > 0")
	}
	return nil
}
