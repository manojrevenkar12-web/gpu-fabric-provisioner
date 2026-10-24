GO ?= go
CONTROLLER_GEN ?= controller-gen
ENVTEST_K8S_VERSION ?= 1.33.0

.PHONY: generate manifests test test-short lint drills scale gate build image

generate: ## deepcopy code and CRDs from api/
	$(CONTROLLER_GEN) object paths=./api/...
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd
	$(CONTROLLER_GEN) rbac:roleName=gpu-fabric-provisioner paths=./internal/... output:rbac:artifacts:config=deploy/rbac

build:
	CGO_ENABLED=0 $(GO) build -o bin/manager ./cmd/manager

test-short: ## unit tests, no API server
	$(GO) test -short ./...

test: ## unit and envtest integration tests (KUBEBUILDER_ASSETS must point at kube-apiserver and etcd)
	$(GO) test -race -count=1 ./...

lint:
	$(GO) vet ./...
	golangci-lint run ./...

drills: ## fault drills against a real API server, compared with the naive provisioner
	$(GO) run ./cmd/drill -out reports/drills.json && $(GO) run ./cmd/gate -in reports/drills.json

scale: ## control-plane scale run
	$(GO) run ./cmd/scale -nodes 2000,5000,10000,20000 -out reports/scale.json

image:
	docker build -t gpu-fabric-provisioner:dev .
