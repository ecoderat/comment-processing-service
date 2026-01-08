FROM --platform=$BUILDPLATFORM golang:1.25.5-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} go build -o /out/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} go build -o /out/ingest ./cmd/ingest
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} go build -o /out/worker ./cmd/worker
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} go build -o /out/outbox-publisher ./cmd/outbox-publisher
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} go build -o /out/producer ./cmd/producer
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} go build -o /out/sentiment-grpc ./cmd/sentiment-grpc

FROM --platform=$TARGETPLATFORM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

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
