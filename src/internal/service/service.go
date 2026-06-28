package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"temporis/internal/config"
	"temporis/internal/gossip"
	"temporis/internal/hash"
	"temporis/internal/partition"
	"temporis/internal/store"
)

type Service struct {
	cfg         *config.Config
	database    *store.DatabaseStore
	cache       *store.CacheStore
	gossipMgr   *gossip.GossipManager
	hashRing    *hash.ConsistentHash
	partitions  map[string]*partition.Manager
	cancelFuncs map[string]context.CancelFunc
	mu          sync.Mutex
	wg          sync.WaitGroup
}

func NewService(cfg *config.Config, database *store.DatabaseStore, cache *store.CacheStore, gossipMgr *gossip.GossipManager) (*Service, error) {
	hashRing := hash.NewConsistentHash(100)
	return &Service{
		cfg:         cfg,
		database:    database,
		cache:       cache,
		gossipMgr:   gossipMgr,
		hashRing:    hashRing,
		partitions:  make(map[string]*partition.Manager),
		cancelFuncs: make(map[string]context.CancelFunc),
	}, nil
}

func (s *Service) Run(ctx context.Context) error {
	// Join gossip cluster with seed nodes
	seedNodes := []string{s.cfg.SeedNode}
	if err := s.gossipMgr.Join(seedNodes); err != nil {
		slog.Error("Failed to join gossip", "error", err)
	}

	syncChan := make(chan struct{}, 1)
	triggerSync := func() {
		select {
		case syncChan <- struct{}{}:
		default:
		}
	}

	// Listen for database notifications.
	go s.database.ListenForChanges(ctx, func() {
		slog.Info("Database changed, executing instant sync...")
		triggerSync()
	})

	// Initial sync
	triggerSync()

	// Periodic fallback sync (low frequency)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Service context cancelled, waiting for partitions to drain...")
			s.wg.Wait()
			slog.Info("All partitions drained cleanly.")
			return nil
		case <-ticker.C:
			triggerSync()
		case <-syncChan:
			s.syncWithCluster(ctx)
		}
	}
}

func (s *Service) syncWithCluster(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Step 1: Get the current list of active nodes from the gossip protocol
	currentNodes := s.gossipMgr.Members()
	slog.Info("Sync cycle starting", "current_nodes", currentNodes)

	// Step 2: Track nodes currently in the hash ring
	existingNodes := s.hashRing.Nodes()
	// Step 3: Update the hash ring
	for _, node := range currentNodes {
		s.hashRing.AddNode(node)
		delete(existingNodes, node)
	}
	// Remove nodes that are no longer in the member list
	for node := range existingNodes {
		s.hashRing.RemoveNode(node)
	}

	// Step 4: Load partitions from the database.
	partitions, err := s.database.GetPartitions(ctx)
	if err != nil {
		slog.Error("Failed to load partitions", "error", err)
		return
	}
	slog.Info("Partitions loaded from DB", "count", len(partitions))

	// Step 5: Redistribute partitions
	newPartitions := make(map[string]*partition.Manager)
	for _, p := range partitions {
		owner := s.hashRing.GetNode(p.ID)
		slog.Info("Partition assigned", "partition_id", p.ID, "node", owner, "timer_count", len(p.Timers))
		if owner == s.cfg.ServiceName {
			slog.Info("This pod owns partition", "partition_id", p.ID)
			newPartitions[p.ID] = partition.NewManager(p, s.cache, s.database)
		}
	}

	// Step 6: Stop partitions this pod no longer owns
	for id, cancel := range s.cancelFuncs {
		if _, exists := newPartitions[id]; !exists {
			slog.Info("Stopping partition", "partition_id", id)
			cancel() // Cancel the context to stop timers
			delete(s.partitions, id)
			delete(s.cancelFuncs, id)
		}
	}

	slog.Info("Starting new partitions", "count", len(newPartitions))
	// Step 7: Start new partitions assigned to this pod
	for id, mgr := range newPartitions {
		if _, exists := s.partitions[id]; !exists {
			// Create a new context for the partition
			partitionCtx, cancel := context.WithCancel(ctx)
			s.partitions[id] = mgr
			s.cancelFuncs[id] = cancel
			slog.Info("Launching StartTimers for partition", "partition_id", id, "timer_count", len(mgr.Partition.Timers))
			s.wg.Add(1)
			go func(m *partition.Manager, pCtx context.Context) {
				defer s.wg.Done()
				m.StartTimers(pCtx)
			}(mgr, partitionCtx)
		} else {
			slog.Info("Partition already running", "partition_id", id, "timer_count", len(mgr.Partition.Timers))
		}
	}
}
