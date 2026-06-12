# Temporis AI Agent Guidelines

This file provides context and instructions for AI agents working on the Temporis repository.

## Project Context
Temporis is a distributed microservice written in Go designed to manage timers across partitions without overlap.
- **Languages:** Go 1.26+
- **Core Technologies:** PostgreSQL (persistent config), Redis (timer firing logs), HashiCorp memberlist (gossip protocol).
- **Architecture:** Nodes use consistent hashing on a hash ring to divide partitions so no two nodes execute timers for the same partition.

## Development Commands
- **Build:** `make build`
- **Tidy/Dependencies:** `make tidy`
- **Run Locally:** `make run`
- **Docker Build:** `make docker`
- **Test:** `make test`

## Directory Structure
- `src/`: Go application code (`cmd`, `internal`), `go.mod`, and `Dockerfile`.
- `deploy/`: Kubernetes manifests (temporis, postgres, redis).
- `pkg/database/`: Database initialization scripts (`script.sql`).
- `scripts/`: Auxiliary scripts like `deploy.sh`.
- `Makefile`: Project tasks and build targets.

## Coding Guidelines
- Follow standard Go idioms and effective Go guidelines.
- Handle database and Redis connectivity robustly (retries, timeouts).
- The database name is `timers`, and configurations related to it (e.g., config maps) generally follow the `timer-*` naming, while the core service is named `temporis`.
- Ensure proper logging using standard `log` package (or upgraded logger if introduced) to trace partition assignments and timer executions.
