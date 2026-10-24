package sim

import (
	"context"
	"testing"
	"time"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
)

func testCloud() *Cloud {
	return NewCloud(CloudConfig{
		Fabrics:       []FabricConfig{{Name: "a", Hosts: map[string]int32{"t": 2}}},
		InstanceTypes: []cloud.InstanceType{{Name: "t", GPUs: 8}}, GPUQuota: 24,
		LaunchDelayMin: 10 * time.Millisecond, LaunchDelayMax: 20 * time.Millisecond, DeleteDelay: 10 * time.Millisecond,
	})
}

func TestCapacityQuotaAndRelease(t *testing.T) {
	c := testCloud()
	ctx := context.Background()
	a, _ := c.Create(ctx, cloud.CreateRequest{ClaimName: "1", Fabric: "a", InstanceType: "t"})
	if _, err := c.Create(ctx, cloud.CreateRequest{ClaimName: "2", Fabric: "a", InstanceType: "t"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create(ctx, cloud.CreateRequest{ClaimName: "3", Fabric: "a", InstanceType: "t"}); cloud.CodeOf(err) != cloud.InsufficientCapacity {
		t.Fatalf("want ICE, got %v", err)
	}
	_ = c.Delete(ctx, a.ID)
	time.Sleep(20 * time.Millisecond)
	if _, err := c.Create(ctx, cloud.CreateRequest{ClaimName: "3", Fabric: "a", InstanceType: "t"}); err != nil {
		t.Fatalf("host should be free again: %v", err)
	}
	c.SetQuota(16)
	c.fabrics["a"]["t"] = 5
	if _, err := c.Create(ctx, cloud.CreateRequest{ClaimName: "4", Fabric: "a", InstanceType: "t"}); cloud.CodeOf(err) != cloud.QuotaExceeded {
		t.Fatalf("want quota, got %v", err)
	}
	l := c.Ledger()
	if l.Instances != 3 || l.ICERejections != 1 || l.QuotaRejections != 1 || l.DuplicateClaims != 0 {
		t.Fatalf("ledger %+v", l)
	}
}

func TestInstancesBecomeRunningAfterTheLaunchDelay(t *testing.T) {
	c := testCloud()
	in, _ := c.Create(context.Background(), cloud.CreateRequest{ClaimName: "1", Fabric: "a", InstanceType: "t"})
	if in.State != cloud.StateCreating {
		t.Fatalf("state %s", in.State)
	}
	time.Sleep(30 * time.Millisecond)
	got, _ := c.Get(context.Background(), in.ID)
	if got.State != cloud.StateRunning {
		t.Fatalf("state %s", got.State)
	}
}
