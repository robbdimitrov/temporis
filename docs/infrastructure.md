# Infrastructure

## Kubernetes Resources

Temporis manifests live in `deploy/` and are applied to the `temporis`
namespace by `scripts/deploy.sh`.

| Workload   | Kind        | Replicas | Storage                  |
| ---------- | ----------- | -------- | ------------------------ |
| `temporis` | Deployment  | 3        | none                     |
| `database` | StatefulSet | 1        | PVC 10 Gi ReadWriteOnce  |
| `cache`    | StatefulSet | 1        | none                     |

## Services and Networking

| Service    | Type      | Port | Protocol | Purpose                        |
| ---------- | --------- | ---- | -------- | ------------------------------ |
| `temporis` | Headless  | 7946 | TCP      | memberlist seed DNS and gossip |
| `database` | ClusterIP | 5432 | TCP      | PostgreSQL                     |
| `cache`    | ClusterIP | 6379 | TCP      | Valkey                         |

`deploy/networkpolicy.yaml` applies default-deny egress for all pods, allows
DNS egress to kube-dns, and then allows `temporis` pods to connect to:

- `database` pods on TCP 5432
- `cache` pods on TCP 6379
- other `temporis` pods on TCP and UDP 7946

Ingress is explicitly allowed from `temporis` pods to `database` on TCP 5432
and to `cache` on TCP 6379. No ingress NetworkPolicy selects `temporis` pods,
so the manifest does not restrict inbound gossip to those pods; egress from
`temporis` pods to other `temporis` pods is explicitly allowed on TCP and UDP
7946.

## Secrets

`scripts/deploy.sh` creates or repairs a single Secret named `timer-secret`.

| Key                 | Consumer / env var                    | Purpose                                      |
| ------------------- | ------------------------------------- | -------------------------------------------- |
| `database-password` | `database` / `POSTGRES_PASSWORD`      | PostgreSQL superuser password                |
| `database-url`      | `temporis` / `DATABASE_URL`           | Backend PostgreSQL connection string         |
| `cache-password`    | `cache` / `CACHE_PASSWORD`            | Valkey `--requirepass` value                 |
| `cache-url`         | `temporis` / `CACHE_URL`              | Backend Valkey connection string             |

The deploy script generates `database-url` as
`postgres://postgres:${database_password}@database:5432/temporis?sslmode=disable`
and `cache-url` as `redis://:${cache_password}@cache:6379`. On reruns it
preserves existing values and refuses to overwrite mismatched URL/password
pairs.

## Environment Variables

The backend configuration is loaded by `src/internal/config/config.go`; the
server in `src/cmd/server/main.go` uses those values to connect to PostgreSQL,
Valkey, and gossip.

| Variable       | Source in Kubernetes                         | Purpose |
| -------------- | -------------------------------------------- | ------- |
| `SERVICE_NAME` | downward API `metadata.name`                 | memberlist node name and local owner ID for partition assignment |
| `GOSSIP_PORT`  | unset in manifest; code default `7946`       | memberlist bind port |
| `DATABASE_URL` | `timer-secret` key `database-url`            | PostgreSQL connection used to load partitions, timers, and LISTEN for config changes |
| `CACHE_URL`    | `timer-secret` key `cache-url`               | Valkey connection used for timer firing claims and firing history |
| `SEED_NODE`    | literal `temporis.temporis.svc.cluster.local:7946` | seed peer passed to memberlist join |

The probe HTTP server listens on `:8080` in `src/cmd/server/main.go`; it is not
configured by an environment variable.

## Health Probes

The `temporis` container exposes `http-probes` on container port 8080. The
server implements both `/healthz` and `/readyz`, but the manifest configures all
probes against `/healthz`.

| Probe     | Type | Path / port     | Delay | Period | Timeout | Failure threshold |
| --------- | ---- | --------------- | ----- | ------ | ------- | ----------------- |
| Startup   | HTTP | `/healthz`:8080 | 0 s   | 2 s    | 1 s     | 30                |
| Readiness | HTTP | `/healthz`:8080 | 5 s   | 5 s    | 3 s     | 3                 |
| Liveness  | HTTP | `/healthz`:8080 | 15 s  | 10 s   | 3 s     | 3                 |

`/healthz` returns 503 after the service goroutine fails. `/readyz` returns 503
until the service marks itself ready after a successful sync, but it is not used
by the current Kubernetes readiness probe.

## Schema / Migration Strategy

`scripts/deploy.sh` creates a ConfigMap named `database-init-script` from
`pkg/database/schema.sql` and applies it before the manifests. The `database`
StatefulSet mounts that ConfigMap at `/docker-entrypoint-initdb.d/`, so the
PostgreSQL image runs `schema.sql` during initial database directory creation.

The schema script creates the `temporis` database, `partitions` and `timers`
tables, `config_changed` notification triggers, six seed partitions, and twenty
sample timers.

There is no versioned migration runner in the current deployment. Because
PostgreSQL entrypoint scripts run only for a fresh data directory, changes to
`pkg/database/schema.sql` affect new PVCs only. Existing clusters with a
populated `database-data` PVC need an explicit additive/corrective migration;
do not rely on editing the bootstrap schema to update an already-initialized
database.

## Gossip Topology

Temporis uses HashiCorp memberlist with `memberlist.DefaultLANConfig()`.
`GOSSIP_PORT` controls `BindPort` and defaults to 7946; the deployment exposes
container port 7946 as `gossip`. `SERVICE_NAME` becomes the memberlist node
name, and in Kubernetes it is the pod name from `metadata.name`.

At service startup, `SEED_NODE` is wrapped as the only seed peer and passed to
`GossipManager.Join`. The manifest sets it to
`temporis.temporis.svc.cluster.local:7946`, which resolves through the headless
`temporis` service to the `temporis` pods. The join loop tries three times with
a 2-second delay between attempts; if it still cannot join, the service logs a
warning and continues as a single node.

Each sync cycle reads `Members()` from memberlist, adds those node names to the
consistent hash ring, removes departed nodes, loads partitions from PostgreSQL,
and starts only the partitions assigned to the local `SERVICE_NAME`.
