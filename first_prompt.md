Role: Go Infrastructure & DevOps Engineer

Context:
I am building "chess-tournament", a high-throughput distributed tournament state engine in Go.
The system handles high-concurrency player check-ins, async event distribution, and compute-heavy Swiss-system pairing calculations using Go (gRPC), Redis (Distributed Locks & Streams), and PostgreSQL.

Target Environment:
- OS / Shell: WSL2 Arch Linux (Linux Native Bash)
- Note: All commands must be strictly standard Linux Bash commands (NOT Windows PowerShell).

Task:
I need a docker-compose.yml file for my local development environment in WSL2 / Linux.

Requirements:
1. PostgreSQL 16 Service:
   - Container name: chess-postgres
   - Database: chess_db
   - User: root
   - Password: secret
   - Port mapping: 5432:5432
   - Volume persistence mounted to local directory `./.data/postgres`
   - Command override: postgres -c max_connections=200
   - Healthcheck using `pg_isready`

2. Redis 7 (Alpine) Service:
   - Container name: chess-redis
   - Port mapping: 6379:6379
   - Requirepass authentication: redis123
   - Volume persistence mounted to local directory `./.data/redis`
   - Command: enable appendonly mode (`redis-server --appendonly yes --requirepass redis123`)
   - Healthcheck using `redis-cli ping`

3. Networking:
   - Define a dedicated custom bridge network named `chess-network` for both services.

Please produce a clean, production-grade docker-compose.yml file and provide the exact Linux bash commands to start services in detached mode and verify logs.