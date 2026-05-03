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
- **Containerization**: Docker + Docker Compose

## Quick Start

```bash
# Build binaries
make build

# Run full stack via Docker
make docker-up

# Run tests
make test

# Generate sqlc code
make sqlc

# Tear down
make docker-down
```

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
