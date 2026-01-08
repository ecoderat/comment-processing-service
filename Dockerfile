# syntax=docker/dockerfile:1.7

# 1) Build Go binaries
FROM --platform=$BUILDPLATFORM golang:1.25.5-bookworm AS builder

WORKDIR /app

# Cache deps
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy source
COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT

# Build all app binaries
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    GOARM="${TARGETVARIANT#v}"; \
    export CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" GOARM="$GOARM"; \
    mkdir -p /out; \
    go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api; \
    go build -trimpath -ldflags="-s -w" -o /out/ingest ./cmd/ingest; \
    go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker; \
    go build -trimpath -ldflags="-s -w" -o /out/outbox-publisher ./cmd/outbox-publisher; \
    go build -trimpath -ldflags="-s -w" -o /out/producer ./cmd/producer; \
    go build -trimpath -ldflags="-s -w" -o /out/sentiment-grpc ./cmd/sentiment-grpc

# 2) Build sql-migrate CLI
FROM --platform=$BUILDPLATFORM golang:1.25.5-bookworm AS migrate-builder
WORKDIR /src

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go install github.com/rubenv/sql-migrate/...@latest

# 3) Minimal runtime base
FROM --platform=$TARGETPLATFORM debian:bookworm-slim AS runtime
WORKDIR /app
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# 4) App images (targets)

FROM runtime AS api
COPY --from=builder /out/api /app/api
ENTRYPOINT ["/app/api"]

FROM runtime AS ingest
COPY --from=builder /out/ingest /app/ingest
ENTRYPOINT ["/app/ingest"]

FROM runtime AS worker
COPY --from=builder /out/worker /app/worker
ENTRYPOINT ["/app/worker"]

FROM runtime AS outbox-publisher
COPY --from=builder /out/outbox-publisher /app/outbox-publisher
ENTRYPOINT ["/app/outbox-publisher"]

FROM runtime AS producer
COPY --from=builder /out/producer /app/producer
ENTRYPOINT ["/app/producer"]

FROM runtime AS sentiment-grpc
COPY --from=builder /out/sentiment-grpc /app/sentiment-grpc
ENTRYPOINT ["/app/sentiment-grpc"]

# 5) Migrate (one-shot job)
FROM runtime AS migrate
COPY --from=migrate-builder /go/bin/sql-migrate /app/sql-migrate
COPY migrations /app/migrations
COPY dbconfig.yml /app/dbconfig.yml
ENTRYPOINT ["/app/sql-migrate"]
