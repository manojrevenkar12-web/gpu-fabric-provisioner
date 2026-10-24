// Package cloud is the boundary between the provisioner and a GPU cloud's compute API. The
// provisioner only ever talks to a Provider; internal/sim implements one for tests and drills, and
// a real cloud is a new implementation of the same interface.
package cloud

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// InstanceType is a server shape the cloud sells.
type InstanceType struct {
	Name        string `json:"name"`
	GPUs        int32  `json:"gpus"`
	GPUProduct  string `json:"gpuProduct"`
	MilliCPU    int64  `json:"milliCPU"`
	MemoryBytes int64  `json:"memoryBytes"`
}

// Offering says how many more hosts of an instance type a fabric can take. It is a hint: the
// number may be stale, and Create can still fail with InsufficientCapacity.
type Offering struct {
	Fabric       string `json:"fabric"`
	InstanceType string `json:"instanceType"`
	Available    int32  `json:"available"`
}

// Quota is the project's GPU quota.
type Quota struct {
	GPULimit int64 `json:"gpuLimit"`
	GPUUsed  int64 `json:"gpuUsed"`
}

// CreateRequest asks for one server. Create is idempotent on ClaimName: asking again for the same
// claim returns the instance that already exists.
type CreateRequest struct {
	ClaimName    string            `json:"claimName"`
	Fabric       string            `json:"fabric"`
	InstanceType string            `json:"instanceType"`
	Image        string            `json:"image"`
	Labels       map[string]string `json:"labels,omitempty"`
}

// InstanceState is where an instance is in its life.
type InstanceState string

const (
	StateCreating InstanceState = "Creating"
	StateRunning  InstanceState = "Running"
	StateDeleting InstanceState = "Deleting"
)

// Instance is a server in the cloud.
type Instance struct {
	ID           string        `json:"id"`
	ProviderID   string        `json:"providerID"`
	ClaimName    string        `json:"claimName"`
	Fabric       string        `json:"fabric"`
	InstanceType string        `json:"instanceType"`
	Image        string        `json:"image"`
	State        InstanceState `json:"state"`
	CreatedAt    time.Time     `json:"createdAt"`
}

// Provider is a GPU cloud's compute API.
type Provider interface {
	InstanceTypes(ctx context.Context) ([]InstanceType, error)
	Offerings(ctx context.Context) ([]Offering, error)
	Quota(ctx context.Context) (Quota, error)
	Create(ctx context.Context, req CreateRequest) (*Instance, error)
	Get(ctx context.Context, id string) (*Instance, error)
	List(ctx context.Context) ([]*Instance, error)
	Delete(ctx context.Context, id string) error
}

// Code classifies cloud errors by what the caller should do about them.
type Code string

const (
	// InsufficientCapacity: the fabric has no host of this type now. Try another fabric.
	InsufficientCapacity Code = "InsufficientCapacity"
	// QuotaExceeded: the project may not have more GPUs. Retrying does not help.
	QuotaExceeded Code = "QuotaExceeded"
	// NotFound: the instance does not exist (any more).
	NotFound Code = "NotFound"
	// Unavailable: a transient API failure. Retry with backoff.
	Unavailable Code = "Unavailable"
)

// Error is a classified cloud error.
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// Errorf builds a classified error.
func Errorf(code Code, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf returns the code of a cloud error, or "" for other errors.
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// IsNotFound reports whether err says the instance does not exist.
func IsNotFound(err error) bool { return CodeOf(err) == NotFound }
