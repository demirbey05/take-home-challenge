.PHONY: build-producer build-consumer sqlc migrate test docker-up docker-down clean

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -ldflags="-s -w -X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)"

## Build
build-producer:
	go build $(LDFLAGS) -o bin/producer ./cmd/producer

build-consumer:
	go build $(LDFLAGS) -o bin/consumer ./cmd/consumer

build: build-producer build-consumer

## Code generation
sqlc:
	sqlc generate

## Database migrations
DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/takehome?sslmode=disable

migrate-up:
	migrate -path db/migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path db/migrations -database "$(DATABASE_URL)" down

migrate-create:
	migrate create -ext sql -dir db/migrations -seq $(name)

## Testing
test:
	go test -v -race -count=1 ./...

## Docker
docker-up:
	docker compose -f deployments/docker-compose.yml up --build -d

docker-down:
	docker compose -f deployments/docker-compose.yml down -v

## Cleanup
clean:
	rm -rf bin/
