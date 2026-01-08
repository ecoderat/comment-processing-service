# Comment Service Pipeline

Local pipeline for comment ingestion, sentiment processing, and querying.

## Requirements

- Go (module version in `go.mod`)
- Docker + Compose

## Environment

Copy `.env.example` to `.env` and edit as needed. The apps load `.env` automatically.

## Infra Setup

```bash
$ docker-compose up -d
```

## Migrations

```bash
$ sql-migrate up
```

## Run Apps

Run the following commands in separate terminal windows.

Ingest consumer:
```
go run ./cmd/ingest
```

Producer:
```
go run ./cmd/producer
```
