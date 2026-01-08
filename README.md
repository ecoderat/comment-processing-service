# Comment Service Pipeline

Local pipeline for comment ingestion, sentiment processing, and querying.

## Requirements

- Go (module version in `go.mod`)
- Docker + Compose

## Environment

Copy `.env.example` to `.env` and edit as needed. The apps load `.env` automatically.

## Infra Setup

```bash
$ docker compose -f deploy/docker-compose.yml up -d
```

## Migrations

```bash
$ sql-migrate up
```

## Run Apps

Run the following commands in separate terminal windows.

Sentiment gRPC service:
```
go run ./cmd/sentiment-grpc
```

Outbox publisher:
```
go run ./cmd/outbox-publisher
```

Worker:
```
go run ./cmd/worker
```

Ingest consumer:
```
go run ./cmd/ingest
```

Producer:
```
go run ./cmd/producer
```
