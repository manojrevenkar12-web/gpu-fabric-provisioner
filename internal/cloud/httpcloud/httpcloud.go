// Package httpcloud speaks a small JSON API that a cloud adapter serves, so the manager binary
// is independent of any one cloud SDK. Handler serves any cloud.Provider over the same API;
// cmd/simcloud uses it to serve the simulated cloud.
//
//	GET    /v1/instance-types        -> []InstanceType
//	GET    /v1/offerings             -> []Offering
//	GET    /v1/quota                 -> Quota
//	POST   /v1/instances             CreateRequest -> Instance   (idempotent on claimName)
//	GET    /v1/instances             -> []Instance
//	GET    /v1/instances/{id}        -> Instance
//	DELETE /v1/instances/{id}
//
// Errors are {"code": "...", "message": "..."} with the cloud.Code of the failure.
package httpcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/manojrevenkar12-web/gpu-fabric-provisioner/internal/cloud"
)

// Client implements cloud.Provider over HTTP.
type Client struct {
	base string
	http *http.Client
}

// New builds a client for an adapter at base.
func New(base string, timeout time.Duration) *Client {
	return &Client{base: strings.TrimRight(base, "/"), http: &http.Client{Timeout: timeout}}
}

var _ cloud.Provider = (*Client)(nil)

type apiError struct {
	Code    cloud.Code `json:"code"`
	Message string     `json:"message"`
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return cloud.Errorf(cloud.Unavailable, "%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return cloud.Errorf(cloud.Unavailable, "read %s: %v", path, err)
	}
	if resp.StatusCode >= 300 {
		var e apiError
		if json.Unmarshal(b, &e) == nil && e.Code != "" {
			return &cloud.Error{Code: e.Code, Message: e.Message}
		}
		if resp.StatusCode >= 500 {
			return cloud.Errorf(cloud.Unavailable, "%s %s: HTTP %d", method, path, resp.StatusCode)
		}
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, b)
	}
	if out != nil && len(b) > 0 {
		return json.Unmarshal(b, out)
	}
	return nil
}

func (c *Client) InstanceTypes(ctx context.Context) ([]cloud.InstanceType, error) {
	var out []cloud.InstanceType
	return out, c.do(ctx, http.MethodGet, "/v1/instance-types", nil, &out)
}

func (c *Client) Offerings(ctx context.Context) ([]cloud.Offering, error) {
	var out []cloud.Offering
	return out, c.do(ctx, http.MethodGet, "/v1/offerings", nil, &out)
}

func (c *Client) Quota(ctx context.Context) (cloud.Quota, error) {
	var out cloud.Quota
	return out, c.do(ctx, http.MethodGet, "/v1/quota", nil, &out)
}

func (c *Client) Create(ctx context.Context, req cloud.CreateRequest) (*cloud.Instance, error) {
	out := &cloud.Instance{}
	if err := c.do(ctx, http.MethodPost, "/v1/instances", req, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) Get(ctx context.Context, id string) (*cloud.Instance, error) {
	out := &cloud.Instance{}
	if err := c.do(ctx, http.MethodGet, "/v1/instances/"+id, nil, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) List(ctx context.Context) ([]*cloud.Instance, error) {
	var out []*cloud.Instance
	return out, c.do(ctx, http.MethodGet, "/v1/instances", nil, &out)
}

func (c *Client) Delete(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/instances/"+id, nil, nil)
}

// Handler serves a Provider over the API.
func Handler(p cloud.Provider) http.Handler {
	mux := http.NewServeMux()
	reply := func(w http.ResponseWriter, v any, err error) {
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			code := cloud.CodeOf(err)
			status := http.StatusInternalServerError
			switch code {
			case cloud.NotFound:
				status = http.StatusNotFound
			case cloud.InsufficientCapacity, cloud.QuotaExceeded:
				status = http.StatusUnprocessableEntity
			case cloud.Unavailable:
				status = http.StatusServiceUnavailable
			}
			if code == "" {
				code = cloud.Unavailable
			}
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(apiError{Code: code, Message: err.Error()})
			return
		}
		if v == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("GET /v1/instance-types", func(w http.ResponseWriter, r *http.Request) {
		v, err := p.InstanceTypes(r.Context())
		reply(w, v, err)
	})
	mux.HandleFunc("GET /v1/offerings", func(w http.ResponseWriter, r *http.Request) {
		v, err := p.Offerings(r.Context())
		reply(w, v, err)
	})
	mux.HandleFunc("GET /v1/quota", func(w http.ResponseWriter, r *http.Request) {
		v, err := p.Quota(r.Context())
		reply(w, v, err)
	})
	mux.HandleFunc("POST /v1/instances", func(w http.ResponseWriter, r *http.Request) {
		var req cloud.CreateRequest
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil || req.ClaimName == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(apiError{Code: "BadRequest", Message: "claimName, fabric and instanceType are required"})
			return
		}
		v, err := p.Create(r.Context(), req)
		reply(w, v, err)
	})
	mux.HandleFunc("GET /v1/instances", func(w http.ResponseWriter, r *http.Request) {
		v, err := p.List(r.Context())
		reply(w, v, err)
	})
	mux.HandleFunc("GET /v1/instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		v, err := p.Get(r.Context(), r.PathValue("id"))
		reply(w, v, err)
	})
	mux.HandleFunc("DELETE /v1/instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		reply(w, nil, p.Delete(r.Context(), r.PathValue("id")))
	})
	return mux
}
