# Project Progress & Handover Log

## Roadmap Checklist

- [x] **Stage 1: Infrastructure & Base Architecture**
  - Setup Docker Compose (PostgreSQL 16 + Redis 7 with healthchecks)
  - Establish Go standard project directory layout
  - Verify container persistence & network connectivity

- [x] **Stage 2: Core Module A - Atomic Distributed Lock**
  - Implement Redis + Lua atomic check-in locking logic
  - Wrap Go client for Redis `EVAL` command
  - Write unit tests for lock concurrency

- [x] **Stage 3: Core Module B - Protobuf & gRPC Pairing Engine**
  - Define `pairing.proto` protobuf contracts
  - Generate Go gRPC code via `protoc`
  - Implement Goroutine Worker Pool for Swiss-pairing calculations

- [x] **Stage 4: Core Module C - Redis Streams & Dead Letter Queue (DLQ)**
  - Implement Stream event producer for check-ins
  - Implement Consumer Group worker pool with Exponential Backoff retries
  - Build DLQ routing for failed messages

- [x] **Stage 5: Core Module D - API Gateway & Graceful Shutdown**
  - Build HTTP Gateway mapping REST to internal gRPC
  - Implement `os/signal` graceful shutdown handlers

- [ ] **Stage 6: Automated Integration Testing**
  - Integrate `testcontainers-go` for ephemeral Redis/Postgres testing
  - Execute `go test -race` for concurrency data race checks

- [ ] **Stage 7: Load Testing & Performance Benchmarking**
  - Write k6 load testing scripts (simulate 2,000 concurrent check-ins)
  - Measure QPS, p95/p99 latency metrics, and write benchmark reports

---

## Stage Handover Logs

### Stage 1: Infrastructure & Base Architecture — Completed

#### Completed Tasks

- Configured Docker Compose services for PostgreSQL 16 and Redis 7 with persistent storage, health checks, authentication, and the dedicated `chess-network` bridge network.
- Established the Go project layout under `cmd/`, `internal/`, `pkg/`, `proto/`, and `scripts/`.
- Added graceful-shutdown-aware gRPC service entrypoints:
  - Gateway: `cmd/gateway/main.go` on port `50051`.
  - Checkin: `cmd/checkin/main.go` on port `50052`.
  - Pairing: `cmd/pairing/main.go` on port `50053`.
- Added PostgreSQL GORM and native pgxpool initialization helpers in `pkg/database/postgres.go`.
- Added an authenticated go-redis client initialization helper in `pkg/database/redis.go`.

#### Primary Dependencies

- gRPC (`google.golang.org/grpc`)
- GORM and its PostgreSQL driver (`gorm.io/gorm`, `gorm.io/driver/postgres`)
- pgxpool (`github.com/jackc/pgx/v5/pgxpool`)
- go-redis (`github.com/redis/go-redis/v9`)

#### Verification

- `go build ./...` — passed.
- `go vet ./...` — passed.
- All three microservice entrypoints compile cleanly.

### Stage 2: Core Module A - Atomic Distributed Lock — Completed

#### Completed Tasks

- Added `internal/checkin/checkin.lua` to atomically check for an existing player check-in, create the check-in key, and apply its TTL.
- Added `internal/checkin.CheckinManager`, which embeds and executes the Lua script through go-redis script caching (`EVALSHA` with automatic `EVAL` fallback).
- Added Redis-backed tests for a successful first check-in, duplicate rejection, TTL application, and 64 simultaneous check-in attempts with exactly one successful caller.
- Kept test cleanup scoped to unique per-test Redis keys so the suite does not flush or disturb unrelated Redis data.

#### Verification

- `go test -v -race ./internal/checkin/...` — passed with no reported data races.

### Stage 3: Core Module B - Protobuf & gRPC Pairing Engine — Completed

#### Completed Tasks

- Added the versioned `pairing.v1` protobuf contract and generated Go message and gRPC bindings under `pkg/proto/pairing/v1/`.
- Extended player state with White/Black game counts, last-color history, and prior-bye tracking; pairing responses now identify the player awarded a one-point bye.
- Added `scripts/gen_proto.sh` for reproducible Linux Bash generation, including Arch Linux and Go plugin installation guidance when tools are unavailable.
- Added a bounded, context-aware Goroutine worker pool for off-transport pairing calculations, configured with six workers in the pairing service entrypoint.
- Upgraded the Swiss engine with deterministic score ranking, lowest-ranked eligible bye assignment for odd fields, color balancing based on White/Black history and last-color alternation, and higher-rank color tie-breaking.
- Replaced greedy opponent selection with deterministic backtracking, strictly preventing rematches while finding a complete legal round whenever the supplied opponent constraints allow one.
- Implemented and registered the `PairingService.GeneratePairings` gRPC endpoint on the existing port `50053` service.
- Added tests for eight-player score ordering, bye selection and response propagation, color balance and alternation, rematch backtracking, impossible-rematch rejection, and 64 concurrent requests across a fixed four-worker test pool.

#### Verification

- `./scripts/gen_proto.sh` — passed using `protoc 35.1`, `protoc-gen-go 1.36.11`, and `protoc-gen-go-grpc 1.6.2`.
- `go test -v -race ./internal/pairing/...` — passed with no reported data races.
- `go build ./...` — passed.
- `go vet ./...` — passed.

### Stage 4: Core Module C - Redis Streams & Dead Letter Queue — Completed

#### Completed Tasks

- Extended the atomic check-in Lua script so lock acquisition, TTL assignment, and publication to `stream:checkin_events` occur in one Redis execution with `tournament_id`, `player_id`, and an RFC3339Nano `timestamp`.
- Added configurable Redis Consumer Group processing with production defaults `checkin_processors` and `worker-1`, `XGROUP CREATE ... MKSTREAM`, pending-message recovery, and blocking reads for new messages.
- Added a bounded four-Goroutine consumer worker pool with context-aware startup, processing, cancellation, and shutdown behavior.
- Added durable per-message retry tracking in Redis with three retries and exponential delays of 100 ms, 200 ms, and 400 ms.
- Added transactional DLQ routing to `stream:checkin_dlq`, followed by acknowledgment of the original message and retry-state cleanup after the fourth failed processing attempt.
- Wired the stream consumer into the check-in service lifecycle with authenticated Redis connectivity and graceful consumer shutdown.
- Added isolated Redis integration tests for event publication, successful processing and acknowledgment, retry exhaustion, DLQ payloads, empty pending-entry state, and retry-state cleanup.
- Added the GORM-backed `checkin_records` PostgreSQL model with non-null fields, individual tournament/player indexes, and a composite unique constraint on `(tournament_id, player_id)`.
- Added startup migration and idempotent `ON CONFLICT DO NOTHING` persistence so at-least-once Redis delivery cannot create duplicate database rows.
- Moved PostgreSQL persistence inside the consumer processing attempt: successful commits are acknowledged, while database errors remain pending and follow the existing retry/DLQ policy.
- Added PostgreSQL-backed integration coverage that observes the inserted row before acknowledgment and verifies duplicate saves retain exactly one record.

#### Verification

- `go test -v -race ./internal/checkin/...` — passed with no reported data races.
- `go build ./...` — passed.
- `go vet ./...` — passed.
- `git diff --check` — passed.
- Direct `chess_db` schema inspection confirmed the primary key, both single-column indexes, and the composite unique index.

### Stage 5: Core Module D - API Gateway & Graceful Shutdown — Completed

#### Completed Tasks

- Replaced the gateway's placeholder gRPC listener with a production-configured `net/http` server on port `8080`, with bounded header, request, response, idle, and shutdown timeouts.
- Added `GET /health`, `POST /api/v1/tournaments/{id}/checkin`, and `POST /api/v1/tournaments/{id}/pairings` REST endpoints with strict JSON decoding and explicit validation, conflict, dependency, and domain-error HTTP statuses.
- Connected REST check-ins to the Redis-backed atomic `CheckinManager`, including configurable TTLs and duplicate-check-in conflict responses.
- Added an owned, typed gRPC client connection from the gateway to `pairing.v1.PairingService` and complete JSON/protobuf mapping for current Swiss-player state, matches, and byes.
- Added signal-driven HTTP shutdown that stops accepting requests, drains active handlers, and closes the pairing gRPC and Redis connections afterward.
- Hardened the pairing and check-in service exit paths with explicit gRPC server cleanup; pairing workers close after graceful RPC completion.
- Changed check-in consumer shutdown to stop new stream reads while draining already-dispatched persistence jobs before Redis and PostgreSQL pools close.
- Added HTTP tests using a real `httptest` server for health, successful and invalid check-ins, duplicate conflicts, and pairing gRPC request/response mapping.

#### Verification

- `go test -v -race ./...` — passed across the full workspace with no reported data races.
- `go build ./...` — passed.
- `go vet ./...` — passed.
- `git diff --check` — passed.
- Live local smoke test passed for gateway health and gateway-to-pairing gRPC flow; both gateway and pairing processes handled `SIGTERM` and exited cleanly.
