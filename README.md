# Temporis

A distributed microservice written in Go, designed to manage timers across partitions with no overlap, deployable on Kubernetes. The service uses consistent hashing for partition distribution, a gossip protocol for service discovery, PostgreSQL for configuration storage, and Redis for logging timer firings.

## Features
- **Distributed Partition Management**: Each service instance manages a subset of partitions, ensuring no overlap using consistent hashing.
- **Timer Execution**: Supports one-time and recurring timers per partition, with configurable intervals.
- **Dynamic Service Discovery**: Uses a gossip protocol (via HashiCorp’s `memberlist`) to discover and monitor service instances (pods).
- **Data Storage**:
  - **PostgreSQL**: Stores partition and timer configurations.
  - **Redis**: Records timer firing events with timestamps.
- **Kubernetes Deployment**: Scalable deployment with Kubernetes manifests for pods, services, and configuration.
- **Resilience**: Handles node failures and reassigns partitions dynamically using gossip and consistent hashing.

## Architecture
The service is built as a Go microservice with the following components:
- **Gossip Protocol**: Manages cluster membership, detecting node joins/leaves using `memberlist`.
- **Consistent Hashing**: Distributes partitions across nodes to ensure balanced and non-overlapping ownership.
- **Partition Manager**: Executes timers (one-time or recurring) for owned partitions.
- **Storage**:
  - **PostgreSQL**: Persists partition and timer configurations.
  - **Redis**: Logs timer firings for auditing or downstream processing.
- **Service Logic**: Orchestrates partition distribution, timer execution, and cluster synchronization.

### Directory Structure
```
temporis/
├── cmd/
│   └── server/
│       └── main.go            # Entry point for the service
├── database/
│   ├── schema.sql             # Postgres init script
├── internal/
│   ├── config/                # Configuration loading
│   ├── gossip/                # Gossip protocol implementation
│   ├── hash/                  # Consistent hashing implementation
│   ├── model/                 # Data models (Partition, Timer)
│   ├── partition/             # Partition and timer execution logic
│   ├── storage/               # PostgreSQL and Redis clients
│   └── service/               # Core service logic
├── deployments/
│   ├── postgres.yaml          # Postgres manifests
│   ├── redis.yaml             # Redis manifests 
│   └── temporis.yaml             # Timer service manifests
├── Dockerfile                 # Docker build instructions
├── go.mod                     # Go module dependencies
└── README.md                  # Project documentation
```

## Prerequisites
- Go 1.26 or later
- Docker
- Kubernetes cluster (e.g., Minikube, EKS, GKE)
- PostgreSQL (accessible from the cluster)
- Redis (accessible from the cluster)

## Dependencies
Defined in `go.mod`:
```go
module github.com/yourname/temporis

go 1.26.0

require (
    github.com/go-redis/redis/v9 v9.0.0
    github.com/jackc/pgx/v5 v5.10.0
    github.com/hashicorp/memberlist v0.5.0
    github.com/spaolacci/murmur3 v1.1.0
)
```

## Setup
### 1. Clone the Repository
```bash
git clone https://github.com/yourname/temporis.git
cd temporis
```

### 2. Install Dependencies
```bash
go mod tidy
```

### 3. Configure Environment
Create a `.env` file or set environment variables for local development:
```bash
SERVICE_NAME=temporis-$(hostname)
GOSSIP_PORT=7946
POSTGRES_URL=postgres://user:pass@localhost:5432/timers?sslmode=disable
REDIS_URL=redis://localhost:6379
```

For Kubernetes, these are set via `deployments/configmap.yaml` and `fieldRef` for `SERVICE_NAME`.

### 4. Initialize PostgreSQL
Apply the schema to your PostgreSQL database:
```sql
psql -h <postgres-host> -U <user> -d timers < ./database/schema.sql
```

Schema (`./database/schema.sql`):
```sql
CREATE TABLE partitions (
    id VARCHAR(255) PRIMARY KEY
);

CREATE TABLE timers (
    id SERIAL PRIMARY KEY,
    partition_id VARCHAR(255) REFERENCES partitions(id),
    name VARCHAR(100),
    interval_ms BIGINT NOT NULL,
    once BOOLEAN NOT NULL
);
```

Insert sample data:
```sql
INSERT INTO partitions (id) VALUES ('part1'), ('part2');
INSERT INTO timers (partition_id, name, interval_ms, once) VALUES
    ('part1', 'timer1', 1000, false),  -- Recurring every 1s
    ('part2', 'timer2', 5000, true);   -- One-time after 5s
```

### 5. Build the Docker Image
```bash
docker build -t temporis:1.0.0 .
```

Push to a registry (if deploying to a remote cluster):
```bash
docker tag temporis:1.0.0 <your-registry>/temporis:1.0.0
docker push <your-registry>/temporis:1.0.0
```

## Deployment
### Local Development
Run the service locally:
```bash
go run ./cmd/server
```

### Kubernetes Deployment
1. Update `deployments/deployment.yaml` with your Docker image (e.g., `<your-registry>/temporis:1.0.0`).
2. Ensure PostgreSQL and Redis are accessible from the cluster.
3. Apply manifests:
   ```bash
   kubectl apply -f deployments/
   ```
4. Scale the deployment for multiple instances:
   ```bash
   kubectl scale deployment temporis --replicas=3
   ```

### Verify Deployment
- Check pod status:
  ```bash
  kubectl get pods -l app=temporis
  ```
- View logs to confirm partition assignment and timer firings:
  ```bash
  kubectl logs -l app=temporis
  ```
- Check Redis for timer firing records:
  ```bash
  redis-cli -h <redis-host> KEYS "firing:*"
  ```

## Usage
The service automatically:
1. Joins the gossip cluster to discover other pods.
2. Loads partitions and timers from PostgreSQL.
3. Assigns partitions using consistent hashing based on the gossip member list.
4. Executes timers (one-time or recurring) for owned partitions.
5. Logs timer firings to Redis.

### Adding Partitions and Timers
Insert new partitions or timers into PostgreSQL:
```sql
INSERT INTO partitions (id) VALUES ('part3');
INSERT INTO timers (partition_id, name, interval_ms, once) VALUES ('part3', 'timer3', 2000, false);
```

The service will detect changes during its next sync cycle (every 5 seconds).

### Monitoring
- **Logs**: Monitor pod logs for node joins/leaves, partition assignments, and errors.
- **Redis**: Inspect `firing:<timer-id>` keys for timer execution history.

## How It Works
1. **Service Startup**:
   - Loads configuration (e.g., database URLs, gossip port).
   - Initializes PostgreSQL, Redis, and gossip protocol.
   - Starts periodic synchronization (`syncWithCluster`).

2. **Gossip Protocol**:
   - Pods discover each other via a headless Kubernetes service (`temporis`).
   - The `memberlist` library maintains an up-to-date list of active pods, detecting failures or scaling events.

3. **Consistent Hashing**:
   - Maps partitions to pods using a hash ring.
   - Updates the ring when pods join or leave (via `AddNode` and `RemoveNode`).
   - Ensures no partition overlap by assigning each partition to exactly one pod.

4. **Partition and Timer Management**:
   - Each pod manages its assigned partitions, loaded from PostgreSQL.
   - Timers (one-time or recurring) are executed via goroutines, with firings logged to Redis.
   - Partitions are reassigned dynamically when the cluster changes.

5. **Node Removal**:
   - When a pod leaves (e.g., due to failure or scaling), it’s removed from the gossip member list.
   - The hash ring is updated to exclude the node, and its partitions are reassigned to other pods.
   - Timers for unowned partitions are stopped gracefully using context cancellation.

## Development

### Debugging
- Enable verbose logging in `memberlist` by setting `LogOutput` in `gossip.NewGossipManager`.
- Add debug logs in `service.syncWithCluster` to track node and partition changes.

### Enhancements
- **Metrics**: Integrate Prometheus for monitoring node count, partition assignments, and timer firings.
- **Health Checks**: Add HTTP endpoints for readiness and liveness probes.
- **Retry Logic**: Implement retries for database connections and Redis writes.

## Troubleshooting
- **Pods Not Discovering Each Other**:
  - Verify the headless service (`kubectl get svc temporis`).
  - Check gossip port (7946) is open and not blocked by network policies.
- **Partitions Not Assigned**:
  - Ensure partitions exist in PostgreSQL.
  - Check logs for hash ring updates and errors.
- **Timer Firings Missing**:
  - Verify Redis connectivity and inspect `firing:*` keys.
  - Confirm timer intervals are reasonable (e.g., not too short).

## Contributing
1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/xyz`).
3. Commit changes (`git commit -m "Add feature xyz"`).
4. Push to the branch (`git push origin feature/xyz`).
5. Open a pull request.

## License
MIT License. See [LICENSE](LICENSE) for details.
