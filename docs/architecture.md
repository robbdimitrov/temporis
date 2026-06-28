# Architecture

Temporis is a distributed timer service. Nodes self-organise into a cluster,
divide work without coordination overhead, and execute timers exactly once per
interval with no overlap between nodes.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Kubernetes Cluster                                                       │
│                                                                         │
│  ┌──────────────┐   gossip    ┌──────────────┐   gossip   ┌──────────┐ │
│  │  temporis-0  │◄───────────►│  temporis-1  │◄──────────►│temporis-2│ │
│  │  (Go)        │             │  (Go)        │            │ (Go)     │ │
│  └──────┬───────┘             └──────┬───────┘            └────┬─────┘ │
│         │  LISTEN/NOTIFY + queries   │                         │       │
│         ▼                            ▼                         ▼       │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  PostgreSQL (StatefulSet)                                        │  │
│  │  partitions table · timers table · config_changed trigger        │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  Valkey (StatefulSet)                                            │  │
│  │  firing claims (SET NX) · firing history lists                   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

Each pod runs four loosely-coupled subsystems: the **gossip manager**, the
**consistent hash ring**, the **service orchestrator**, and one or more
**partition managers**. PostgreSQL and Valkey are the only shared state; the
pods never coordinate with each other directly beyond gossip membership.

---

## Gossip Protocol

Temporis uses [HashiCorp memberlist](https://github.com/hashicorp/memberlist)
to maintain a live view of cluster membership. Memberlist implements a
[SWIM-based](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
gossip protocol: nodes periodically probe each other with UDP pings, propagate
membership deltas via piggybacking, and mark unreachable nodes as suspect
before declaring them dead.

### Seed and join

At startup each pod is given a single seed address via the `SEED_NODE`
environment variable. In Kubernetes this resolves to the headless `temporis`
service, which returns the IP addresses of all ready pods. The join sequence
retries up to three times with a 2-second backoff. If all attempts fail the
node logs a warning and continues as a single-node cluster — it will merge with
the rest of the cluster the next time a peer probes it.

```
pod start
  │
  ├─► gossip.NewGossipManager(port, serviceName)   ← creates memberlist, binds UDP/TCP 7946
  │
  └─► gossipMgr.Join([]string{SEED_NODE})
        retry up to 3 times, 2s apart
        on success → integrated into the cluster
        on failure → continue as single node (cluster will re-merge)
```

### Membership view

`gossipMgr.Members()` returns the name of every node memberlist currently
considers alive. Node names are the Kubernetes pod names (sourced from
`SERVICE_NAME` / downward-API `metadata.name`). The same name is used as the
key in the consistent hash ring, so membership and ownership always refer to
the same identifier.

### Node departure

When a pod shuts down gracefully it calls `list.Leave(0)` followed by
`list.Shutdown()`, which broadcasts a leave message so peers can immediately
remove it from their membership tables. Ungraceful departure (crash, OOM kill,
network partition) is detected by the SWIM failure detector: the node is first
marked suspect, then dead after the suspicion timeout. Any running sync cycle
on the remaining nodes will pick up the change at its next membership read.

---

## Consistent Hash Ring

Partitions are assigned to nodes via a consistent hash ring built on
[MurmurHash3](https://github.com/spaolacci/murmur3). Consistent hashing
minimises churn when the cluster changes: only the partitions that were owned
by the affected node need to be redistributed.

### Ring construction

Each node is represented by 100 **virtual nodes** (replicas) placed on the
ring. A virtual node is keyed by `"<node-name>:<replica-index>"` hashed with
MurmurHash3 to a `uint64`. The ring is kept sorted by hash value. More virtual
nodes smooth the distribution so no single node receives a disproportionate
share of partitions.

```
ring (sorted by hash) ────────────────────────────────────────►
 ...  [temporis-0:3]  [temporis-2:7]  [temporis-1:1]  ...
```

### Partition lookup

To find the owner of a partition, MurmurHash3 hashes the partition ID and
binary-searches the ring for the first virtual node whose hash is ≥ the
partition hash. If the search wraps past the end of the ring the first virtual
node is used (ring wrap-around). This makes ownership deterministic: every
node independently computes the same answer given the same member list.

### Ring updates

`AddNode` and `RemoveNode` are called during each sync cycle (see below). Both
are idempotent — adding an already-present node or removing an absent node is a
no-op. After every change the ring slice is re-sorted.

---

## Sync Cycle

The service orchestrator runs a control loop that keeps the local partition set
consistent with the current cluster state.

```
triggers
  ├─ startup (immediate)
  ├─ PostgreSQL LISTEN/NOTIFY  ← config_changed fires on partitions/timers INSERT/UPDATE/DELETE
  └─ 30-second periodic ticker ← safety net for missed notifications

  │
  ▼
syncWithCluster(ctx)
  │
  ├─ 1. gossipMgr.Members()             ← who is alive right now?
  ├─ 2. hashRing.AddNode / RemoveNode   ← reconcile ring with live members
  ├─ 3. database.GetPartitions()        ← load all partitions + their timers
  ├─ 4. hashRing.GetNode(partition.ID)  ← compute owner for each partition
  ├─ 5. stop runners for unowned        ← cancel context → goroutines drain
  └─ 6. start runners for newly owned   ← skip if partition+timers unchanged
```

The sync is serialised under a mutex. A buffered channel of capacity 1 acts as
a coalescing queue: rapid back-to-back notifications (e.g., a batch SQL
insert) collapse into a single sync execution.

### Partition runner lifecycle

Each owned partition is represented by a `partitionRunner` struct holding the
`partition.Manager`, a `context.CancelFunc`, and a `done` channel. When a
partition is stopped, `cancel()` is called and the caller blocks on `<-done`
before removing the runner from the map — ensuring clean shutdown before the
next sync can start a replacement.

Partition equality is checked before restarting: if the partition ID and all
timer IDs, intervals, and once-flags are identical to what is already running,
the runner is left in place. This prevents unnecessary churn on syncs triggered
by unrelated table changes.

---

## Timer Execution

Each owned partition is managed by a `partition.Manager` that starts one
goroutine per timer. Goroutines run until context cancellation.

### One-time timers

```
startTimer (once=true)
  │
  ├─ tracker.HasFired?  → yes → return (already done)
  │
  ├─ scheduler.ScheduleOnce()   ← DB: INSERT … ON CONFLICT DO NOTHING, SELECT
  │   returns the first pick-up timestamp, stable across rebalances
  │
  ├─ nextFire = scheduledAt + interval
  ├─ wait (with jitter)
  │
  ├─ tracker.ClaimFiring (SET NX in Valkey)
  │   → claimed → fire callback, return
  │   → not claimed → another node won the race, return
```

`ScheduleOnce` writes a row the first time any node picks up the timer. If a
rebalance moves the timer to a different node mid-countdown, the new node reads
the same timestamp and resumes from where the countdown left off rather than
restarting from zero.

### Recurring timers

```
startTimer (once=false)
  │
  ├─ tracker.GetLastFirings()   ← most recent scheduled time from Valkey list
  │   → found: nextFire = lastFiring + interval
  │   → not found: nextFire = now + interval
  │
  loop:
    ├─ wait until nextFire + jitter (or jitter-only if past due)
    │
    ├─ tracker.ClaimRecurringFiring (Valkey Lua script)
    │   ├─ RecurringClaimed      → fire callback, release fence, advance nextFire
    │   ├─ RecurringBusy         → another node is still executing; retry after backoff
    │   └─ RecurringAlreadyClaimed → this slot was already fired; advance nextFire
    │
    └─ nextFire = first future multiple of interval after now
```

The Valkey Lua script for `ClaimRecurringFiring` is a single atomic operation:
it checks for an existing fence key and, if absent, sets it with a TTL of
`max(2×interval, 1 minute)`. This prevents two nodes from ever executing the
same scheduled slot concurrently.

### Thundering-herd jitter

When a node restarts after downtime, all of its timers may be past-due
simultaneously. To prevent them from hammering the database at the same instant,
each timer computes a deterministic jitter from its ID:

1. Hash the timer ID with FNV-32a.
2. Quantise the hash into 1-minute buckets up to `maxJitter`.
3. `maxJitter` is the larger of 10 % of the interval or `numTimers × 2 ms`,
   capped at 1 hour.

The same timer always lands in the same bucket, so restarts do not produce
random double-penalty delays.

---

## Storage Roles

| Store      | What is stored                         | Access pattern                        |
| ---------- | -------------------------------------- | ------------------------------------- |
| PostgreSQL | Partition and timer configuration      | Read on every sync; LISTEN for change |
|            | Once-timer pick-up timestamps          | INSERT … ON CONFLICT, SELECT          |
| Valkey     | Recurring timer execution fence        | Lua SET NX with TTL                   |
|            | Once-timer claimed flag                | SET NX (permanent)                    |
|            | Firing history list (last 10 per timer)| LPUSH + LTRIM                         |

PostgreSQL is the source of truth for configuration. Valkey is the source of
truth for execution state. No execution state is kept in memory alone: a node
can be replaced at any time and the replacement will reconstruct the correct
state from these two stores.

---

## Delivery Guarantees

| Timer type | Guarantee      | Mechanism                                              |
| ---------- | -------------- | ------------------------------------------------------ |
| One-time   | At-most-once   | `ClaimFiring` (SET NX): only one node wins             |
| Recurring  | At-most-once per scheduled slot | `ClaimRecurringFiring` (Lua SET NX) |

Neither type offers at-least-once delivery. If the winning node crashes after
claiming but before executing the callback, the slot is lost. This is an
explicit design choice to keep coordination simple and avoid retrying side
effects that may already have been applied.

---

## Failure Modes

| Scenario                        | Behaviour                                                                   |
| ------------------------------- | --------------------------------------------------------------------------- |
| Pod crashes mid-timer           | Remaining nodes detect departure via gossip; next sync reassigns partitions |
| Partition rebalanced during countdown | One-time timer resumes from original pick-up time; recurring timer uses last Valkey history entry |
| PostgreSQL unreachable at sync  | Sync returns false (ready=false); retried on next ticker tick (30 s)        |
| Valkey unreachable at claim     | Claim returns false/error; node skips execution; recurring timer retries next interval |
| Split-brain (network partition) | Both sides compute ownership independently; if both sides own a partition, Valkey SET NX prevents double-execution per slot |
