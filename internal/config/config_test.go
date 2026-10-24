package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestShippedConfigLoads(t *testing.T) {
	s, err := Load(filepath.Join("..", "..", "config", "controller.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if s.RegistrationTTL.Duration != 15*time.Minute || s.KubeQPS != 200 {
		t.Fatalf("unexpected settings %+v", s)
	}
}

func TestUnknownKeysAndBadValuesAreRejected(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"typo.yaml":  "registrationTimeout: 5m\n",
		"batch.yaml": "batchIdle: 20s\nbatchMax: 10s\n",
		"dur.yaml":   "resync: soon\n",
	} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(p); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}
