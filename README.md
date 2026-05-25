# Take-Home: Task Producer/Consumer System

A two-tier worker architecture with shared PostgreSQL persistence, REST communication, and integrated observability (Prometheus + Grafana + pprof).

## Architecture

```
┌──────────────┐   REST    ┌──────────────┐
│   Producer   │ ───────── │   Consumer   │
└──────┬───────┘           └──────┬───────┘
       │                          │
       └──────────┬───────────────┘
                  │
           ┌──────▼──────┐
           │  PostgreSQL  │
           └─────────────┘
```

## Tech Stack

- **Language**: Go 1.25
- **Communication**: REST (HTTP/1)
- **Database**: PostgreSQL 16
- **Persistence**: sqlc (code generation)
- **Migrations**: gomigrate (sql-migrate)
- **Observability**: Prometheus, Grafana, pprof
- **Logging**: Loki + Promtail (Grafana Explore)
- **Containerization**: Docker + Docker Compose

## Quick Start

```bash
# Build binaries
make build

# Run full stack via Docker
make docker-up

# Run tests
make test

# Generate an HTML coverage report
make test-cover

# Generate sqlc code
make sqlc

# Tear down
make docker-down
```

## Test Coverage

- HTML report (opens a browser when possible): `make test-cover`
- Terminal summary:
  - `go test -coverprofile=coverage.out ./...`
  - `go tool cover -func=coverage.out`
- Write HTML to a file (instead of launching a browser): `go tool cover -html=coverage.out -o coverage.html`

## Flame Graph (pprof)

Start the services first (e.g. `make docker-up`), then use pprof.

pprof is exposed on:

- Producer: `http://localhost:6060/debug/pprof/`
- Consumer: `http://localhost:6061/debug/pprof/`

Collect a CPU profile and open the pprof web UI (includes a Flame Graph view):

```bash
# Producer (30s CPU profile)
go tool pprof -http=:8082 http://localhost:6060/debug/pprof/profile?seconds=30

# Consumer (30s CPU profile)
go tool pprof -http=:8083 http://localhost:6061/debug/pprof/profile?seconds=30
```

Other useful profiles:

```bash
# Heap profile (memory)
go tool pprof -http=:8084 http://localhost:6060/debug/pprof/heap

# Execution trace (5s)
curl -o trace.out "http://localhost:6060/debug/pprof/trace?seconds=5"
go tool trace trace.out
```

## Logs (Loki)

- Grafana: `http://localhost:3000` (admin / admin)
- Explore → Loki datasource → query examples:
  - `{service="producer"}`
  - `{service="consumer"}`

## Project Structure

```
├── cmd/producer/          # Producer entry point
├── cmd/consumer/          # Consumer entry point
├── internal/config/       # Shared config (embed)
├── internal/persistence/  # sqlc-generated DB code
├── internal/producer/     # Producer business logic
├── internal/consumer/     # Consumer business logic
├── db/migrations/         # SQL migrations
├── db/queries/            # sqlc SQL queries
├── deployments/           # Docker, Prometheus, Grafana
└── Makefile               # Build, test, deploy commands
```

## Design Decisions

| Decision      | Choice     | Rationale                                      |
|---------------|------------|-------------------------------------------------|
| Protocol      | REST       | Simple, debuggable, stdlib `net/http`           |
| Database      | PostgreSQL | Production-grade, strong typing, good tooling   |
| ORM           | sqlc       | Type-safe, no runtime reflection, compile-time  |
| Config        | `embed`    | Zero external deps, baked into binary            |
| Build flags   | `-ldflags` | Version injection without config files           |
