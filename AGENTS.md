# Temporis AI Agent Guidelines

This file provides context and instructions for AI agents working on the Temporis repository.

## Project Context
Temporis is a distributed microservice written in Go designed to manage timers across partitions without overlap.
- **Languages:** Go 1.26+
- **Core Technologies:** PostgreSQL (persistent config), Valkey (timer firing logs), HashiCorp memberlist (gossip protocol).
- **Architecture:** Nodes use consistent hashing on a hash ring to divide partitions so no two nodes execute timers for the same partition.

## Development Commands
- **Build:** `make build`
- **Tidy/Dependencies:** `make tidy`
- **Run Locally:** `make run`
- **Docker Build:** `make docker`
- **Test:** `make test`

## Directory Structure
- `src/`: Go application code (`cmd`, `internal`), `go.mod`, and `Dockerfile`.
- `deploy/`: Kubernetes manifests (temporis, postgres, valkey).
- `pkg/database/`: Database initialization scripts (`schema.sql`).
- `scripts/`: Auxiliary scripts like `deploy.sh`.
- `Makefile`: Project tasks and build targets.

## Coding Guidelines
- Follow standard Go idioms and effective Go guidelines.
- Follow SOLID, KISS, DRY, YAGNI, and the Pareto principle. Keep changes focused; do not build for hypothetical requirements.
- Search for an existing helper, abstraction, or platform primitive before adding one. Add abstractions only when they remove concrete complexity or duplication.
- Match surrounding structure, naming, and idioms so the codebase reads as one system.
- Use precise names and standard initialisms. Prefer clarity over compressed code and named constants over repeated policy-significant literals.
- Keep related fixes together; do not expand a task into unrelated cleanup.
- Comments explain constraints, invariants, security decisions, or non-obvious intent. Do not narrate straightforward code or preserve implementation history.
- Handle database and Valkey connectivity robustly (retries, timeouts).
- The database name is `timers`, and configurations related to it (e.g., config maps) generally follow the `timer-*` naming, while the core service is named `temporis`.
- Ensure proper logging using standard `log` package (or upgraded logger if introduced) to trace partition assignments and timer executions.

## Secure Engineering

Security controls are design constraints, not review-time additions.

- Validate untrusted data where it enters the system.
- Use parameterized SQL exclusively. Make check-then-act operations atomic with a transaction, row lock, or database constraint.
- Keep secrets out of code, committed configuration, URLs, browser storage, generated artifacts, and logs.
- Log structured operational metadata without credentials, session values, request bodies, unnecessary personal data, or raw user-controlled text.
- Justify new dependencies by their maintenance, security, image-size, and runtime cost.
- Containers run non-root with a read-only root filesystem, all capabilities dropped, and `seccompProfile: RuntimeDefault`.
