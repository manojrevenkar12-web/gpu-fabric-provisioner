# syntax=docker/dockerfile:1
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/manager ./cmd/manager && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/simcloud ./cmd/simcloud

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/manager /out/simcloud /usr/local/bin/
COPY config/controller.yaml /etc/gpu-fabric-provisioner/controller.yaml
COPY config/simcloud.yaml /etc/gpu-fabric-provisioner/simcloud.yaml
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/manager"]
