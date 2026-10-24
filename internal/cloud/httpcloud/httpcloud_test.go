package httpcloud_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud/httpcloud"
	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/sim"
)

func TestRoundTripKeepsErrorCodesAndIdempotency(t *testing.T) {
	it := cloud.InstanceType{Name: "h100x8", GPUs: 8, MilliCPU: 1000, MemoryBytes: 1 << 30}
	s := sim.NewCloud(sim.CloudConfig{
		Fabrics:       []sim.FabricConfig{{Name: "a", Hosts: map[string]int32{"h100x8": 1}}},
		InstanceTypes: []cloud.InstanceType{it}, GPUQuota: 100, LaunchDelayMin: time.Millisecond, LaunchDelayMax: time.Millisecond,
	})
	srv := httptest.NewServer(httpcloud.Handler(s))
	defer srv.Close()
	c := httpcloud.New(srv.URL, 5*time.Second)
	ctx := context.Background()

	types, err := c.InstanceTypes(ctx)
	if err != nil || len(types) != 1 || types[0] != it {
		t.Fatalf("types %v %v", types, err)
	}
	a, err := c.Create(ctx, cloud.CreateRequest{ClaimName: "c1", Fabric: "a", InstanceType: "h100x8", Image: "img"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Create(ctx, cloud.CreateRequest{ClaimName: "c1", Fabric: "a", InstanceType: "h100x8", Image: "img"})
	if err != nil || b.ID != a.ID {
		t.Fatalf("create is not idempotent: %v %v %v", a, b, err)
	}
	_, err = c.Create(ctx, cloud.CreateRequest{ClaimName: "c2", Fabric: "a", InstanceType: "h100x8", Image: "img"})
	if cloud.CodeOf(err) != cloud.InsufficientCapacity {
		t.Fatalf("want InsufficientCapacity, got %v", err)
	}
	if err := c.Delete(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "i-nope"); !cloud.IsNotFound(err) {
		t.Fatalf("want NotFound, got %v", err)
	}
	offs, err := c.Offerings(ctx)
	if err != nil || len(offs) != 1 {
		t.Fatalf("offerings %v %v", offs, err)
	}
}
