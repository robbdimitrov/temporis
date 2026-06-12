# Temporis AI Agent Guidelines

This file provides context and instructions for AI agents working on the Temporis repository.

## Project Context
Temporis is a distributed microservice written in Go designed to manage timers across partitions without overlap.
- **Languages:** Go 1.26+
- **Core Technologies:** PostgreSQL (persistent config), Redis (timer firing logs), HashiCorp memberlist (gossip protocol).
- **Architecture:** Nodes use consistent hashing on a hash ring to divide partitions so no two nodes execute timers for the same partition.

## Development Commands
- **Build:** `go build ./...`
- **Tidy/Dependencies:** `go mod tidy`
- **Run Locally:** `go run ./cmd/server`
- **Docker Build:** `docker build -t temporis:1.0.0 .`

## Directory Structure
- `cmd/server/`: Main application entry point.
- `internal/`: Core application logic (config, gossip, hash, model, partition, service, storage).
- `deployments/`: Kubernetes manifests (temporis, postgres, redis).
- `database/`: Database initialization scripts (`script.sql`).

## Coding Guidelines
- Follow standard Go idioms and effective Go guidelines.
- Handle database and Redis connectivity robustly (retries, timeouts).
- The database name is `timers`, and configurations related to it (e.g., config maps) generally follow the `timer-*` naming, while the core service is named `temporis`.
- Ensure proper logging using standard `log` package (or upgraded logger if introduced) to trace partition assignments and timer executions.
