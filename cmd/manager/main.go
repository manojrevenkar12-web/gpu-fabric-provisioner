// Command manager runs the gpu-fabric-provisioner controllers.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-logr/logr/funcr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud/httpcloud"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/config"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/controllers"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/kube"
)

func main() {
	var (
		settingsPath = flag.String("config", "", "controller settings YAML (defaults if empty)")
		cloudURL     = flag.String("cloud-url", "", "base URL of the cloud compute API adapter (see internal/cloud/httpcloud)")
		metricsAddr  = flag.String("metrics-bind-address", ":8080", "metrics endpoint")
		probeAddr    = flag.String("health-probe-bind-address", ":8081", "health and readiness probes")
		leaderElect  = flag.Bool("leader-elect", true, "run with leader election (required with more than one replica)")
		leaderNS     = flag.String("leader-election-namespace", "", "namespace of the leader election Lease (defaults to the pod's)")
		webhookOn    = flag.Bool("enable-webhook", true, "serve the pod gating webhook on :9443")
		verbosity    = flag.Int("v", 0, "log verbosity")
	)
	flag.Parse()
	log := funcr.NewJSON(func(obj string) { fmt.Fprintln(os.Stderr, obj) }, funcr.Options{Verbosity: *verbosity, LogTimestamp: true})
	ctrl.SetLogger(log)
	if err := run(*settingsPath, *cloudURL, *metricsAddr, *probeAddr, *leaderElect, *leaderNS, *webhookOn); err != nil {
		log.Error(err, "manager exited")
		os.Exit(1)
	}
}

func run(settingsPath, cloudURL, metricsAddr, probeAddr string, leaderElect bool, leaderNS string, webhookOn bool) error {
	settings, err := config.Load(settingsPath)
	if err != nil {
		return err
	}
	if cloudURL == "" {
		return fmt.Errorf("--cloud-url is required")
	}
	var provider cloud.Provider = httpcloud.New(cloudURL, 30*time.Second)
	cfg := ctrl.GetConfigOrDie()
	cfg.QPS, cfg.Burst = settings.KubeQPS, settings.KubeBurst
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                        kube.Scheme(),
		Metrics:                       metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:        probeAddr,
		LeaderElection:                leaderElect,
		LeaderElectionID:              "gpu-fabric-provisioner",
		LeaderElectionNamespace:       leaderNS,
		LeaderElectionReleaseOnCancel: true,
		LeaseDuration:                 ptr.To(15 * time.Second),
		RenewDeadline:                 ptr.To(10 * time.Second),
		RetryPeriod:                   ptr.To(2 * time.Second),
		// Managed fields are a large share of every Node and Pod object and the controllers never
		// read them; at 20,000 nodes dropping them from the cache matters.
		Cache:         cache.Options{DefaultTransform: cache.TransformStripManagedFields()},
		WebhookServer: webhook.NewServer(webhook.Options{Port: 9443}),
	})
	if err != nil {
		return err
	}
	ctx := ctrl.SetupSignalHandler()
	if _, err := controllers.Setup(ctx, mgr, controllers.Options{Settings: settings, Cloud: provider, Webhook: webhookOn}); err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("cache", func(_ *http.Request) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return fmt.Errorf("cache not synced")
		}
		return nil
	}); err != nil {
		return err
	}
	return mgr.Start(ctx)
}
