
# Architecture Specification: chess-tournament

## 1. System Vision & Context
`chess-tournament` is a high-throughput, distributed tournament state and pairing engine written in Go. Based on real-world chess tournament operations (ICEA Chess), it resolves database pessimist lock contention and race conditions during peak player check-in windows through an event-driven microservices architecture.

## 2. Target Environment & Runtime Constraints
- **Operating System**: WSL2 (Arch Linux)
- **Runtime Environment**: Go 1.22+, Docker Desktop (WSL2 Integration)
- **Primary IDE**: GoLand (Windows) attached to WSL2 workspace
- **Rule for AI Assistants**: Generate all CLI commands, scripts, and build artifacts **EXCLUSIVELY for Linux Bash syntax**. Do NOT output Windows PowerShell commands.

## 3. Tech Stack & Infrastructure
- **Core Language**: Go 1.22+ (Goroutines, Channels, Context, Worker Pools)
- **Transport & Communication**: gRPC + Protocol Buffers (Internal RPC), HTTP REST Gateway (Gin/net/http for client)
- **Concurrency & Locking**: Redis 7 + Lua Scripts (Atomic check-in locks)
- **Event Streaming**: Redis Streams (Consumer Groups, Exponential Backoff, Dead Letter Queue / DLQ)
- **Persistence**: PostgreSQL 16 (Final transactional state and match history)
- **Infrastructure**: Docker & Docker Compose
- **Testing & Quality**: `testcontainers-go` (Ephemeral integration tests), `go test -race` (Data race detector), `k6` (Load testing)

## 4. High-Level Architecture Topology
                     [ Frontend / Client ]
                               │
                          (HTTP / REST)
                               ▼
                   ┌──────────────────────┐
                   │   API Gateway (Go)   │
                   └──────────┬───────────┘
                              │ (gRPC)
      ┌───────────────────────┴───────────────────────┐
      ▼                                               ▼


┌─────────────────────────┐                     ┌─────────────────────────┐

│ Check-in & Lock Service │                     │ Pairing Calculation Svc │

│       (Go / gRPC)       │                     │   (Go / Worker Pool)    │

└────────┬──────────┬──────┘                     └─────────────────────────┘

│          │

▼          ▼

┌──────────┐ ┌──────────────┐

│ Redis    │ │ Redis Streams│ ───► [ Consumer Worker ] ───► [ DLQ ]

│ (Lock/   │ │ (Async Event)│              │

│ Caching) │ └──────────────┘              ▼

└──────────┘                        ┌──────────────┐

│  PostgreSQL  │

└──────────────┘

## 5. Core Subsystem Responsibilities

### Subsystem A: Distributed Check-in & Atomic Locking
- Uses Redis + Lua scripts (`checkin.lua`) to execute "check existence + mark check-in + set TTL" in a single atomic step.
- Eliminates database row-level locking under high concurrent check-in requests.

### Subsystem B: Swiss-System Pairing Engine
- Computes multi-round Swiss-pairing algorithm off the main thread using Goroutine Worker Pools.
- Exposes strongly-typed gRPC endpoints defined via Protocol Buffers.

### Subsystem C: Asynchronous Event Pipeline & DLQ
- Produces check-in events to `stream:checkin_events`.
- Workers process events asynchronously with Exponential Backoff retry policies (1s, 2s, 4s).
- Unrecoverable events are routed to `stream:checkin_dlq` for manual inspection.

### Subsystem D: Graceful Shutdown & Lifecycle Management
- Traps `SIGTERM` / `SIGINT` signals using Go's `os/signal`.
- Flushes active Goroutines and gracefully closes gRPC servers and DB connections before exiting.

