// Command simcloud serves the simulated GPU cloud over the httpcloud API, for running the
// manager binary end to end without a real cloud (kind, CI smoke test).
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud/httpcloud"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/config"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/sim"
)

type file struct {
	Fabrics        []sim.FabricConfig   `json:"fabrics"`
	InstanceTypes  []cloud.InstanceType `json:"instanceTypes"`
	GPUQuota       int64                `json:"gpuQuota"`
	LaunchDelayMin config.Duration      `json:"launchDelayMin"`
	LaunchDelayMax config.Duration      `json:"launchDelayMax"`
	DeleteDelay    config.Duration      `json:"deleteDelay"`
	APILatency     config.Duration      `json:"apiLatency"`
	Seed           int64                `json:"seed"`
}

func main() {
	addr := flag.String("addr", ":8090", "listen address")
	cfgPath := flag.String("config", "config/simcloud.yaml", "simulated cloud: fabrics, instance types, quota, delays")
	flag.Parse()
	b, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	var f file
	if err := yaml.UnmarshalStrict(b, &f); err != nil {
		log.Fatalf("%s: %v", *cfgPath, err)
	}
	cfg := sim.CloudConfig{
		Fabrics: f.Fabrics, InstanceTypes: f.InstanceTypes, GPUQuota: f.GPUQuota, Seed: f.Seed,
		LaunchDelayMin: f.LaunchDelayMin.Duration, LaunchDelayMax: f.LaunchDelayMax.Duration,
		DeleteDelay: f.DeleteDelay.Duration, APILatency: f.APILatency.Duration,
	}
	srv := &http.Server{Addr: *addr, Handler: httpcloud.Handler(sim.NewCloud(cfg)), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("simulated cloud on %s (%d fabrics)", *addr, len(cfg.Fabrics))
	log.Fatal(srv.ListenAndServe())
}
