package simenv

import "github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"

type cloudInstanceType = cloud.InstanceType

func cloudType() cloud.InstanceType {
	return cloud.InstanceType{Name: "gpu-h100-sxm.8gpu", GPUs: 8, GPUProduct: "NVIDIA-H100-80GB-HBM3", MilliCPU: 128000, MemoryBytes: 1600 << 30}
}
