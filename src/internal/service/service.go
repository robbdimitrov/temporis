package service

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"temporis/internal/config"
	"temporis/internal/gossip"
	"temporis/internal/hash"
	"temporis/internal/model"
	"temporis/internal/partition"
	"temporis/internal/store"
)

type Service struct {
	cfg        *config.Config
	database   *store.DatabaseStore
	cache      *store.CacheStore
	gossipMgr  *gossip.GossipManager
	hashRing   *hash.ConsistentHash
	partitions map[string]*partitionRunner
	ready      atomic.Bool
	mu         sync.Mutex
	wg         sync.WaitGroup
}

type partitionRunner struct {
	manager *partition.Manager
	cancel  context.CancelFunc
	done    chan struct{}
}

func NewService(cfg *config.Config, database *store.DatabaseStore, cache *store.CacheStore, gossipMgr *gossip.GossipManager) (*Service, error) {
	hashRing := hash.NewConsistentHash(100)
	return &Service{
		cfg:        cfg,
		database:   database,
		cache:      cache,
		gossipMgr:  gossipMgr,
		hashRing:   hashRing,
		partitions: make(map[string]*partitionRunner),
	}, nil
}

func (s *Service) Ready() bool {
	return s.ready.Load()
}

func (s *Service) Run(ctx context.Context) error {
	// Join gossip cluster with seed nodes
	seedNodes := []string{s.cfg.SeedNode}
	if err := s.gossipMgr.Join(seedNodes); err != nil {
		slog.Warn("Failed to join gossip, continuing as a single node", "error", err)
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
			s.ready.Store(false)
			s.stopAllPartitions()
			s.wg.Wait()
			slog.Info("All partitions drained cleanly.")
			return nil
		case <-ticker.C:
			triggerSync()
		case <-syncChan:
			s.ready.Store(s.syncWithCluster(ctx))
		}
	}
}

func (s *Service) syncWithCluster(ctx context.Context) bool {
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
		return false
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
	for id, runner := range s.partitions {
		if _, exists := newPartitions[id]; !exists {
			slog.Info("Stopping partition", "partition_id", id)
			s.stopPartition(id, runner)
		}
	}

	slog.Info("Starting new partitions", "count", len(newPartitions))
	// Step 7: Start new partitions assigned to this pod
	for id, mgr := range newPartitions {
		if runner, exists := s.partitions[id]; exists {
			if partitionsEqual(runner.manager.Partition, mgr.Partition) {
				slog.Info("Partition already running", "partition_id", id, "timer_count", len(mgr.Partition.Timers))
				continue
			}
			slog.Info("Restarting partition with updated timer configuration", "partition_id", id)
			s.stopPartition(id, runner)
		}
		s.startPartition(ctx, id, mgr)
	}

	return true
}

func (s *Service) startPartition(ctx context.Context, id string, mgr *partition.Manager) {
	partitionCtx, cancel := context.WithCancel(ctx)
	runner := &partitionRunner{
		manager: mgr,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	s.partitions[id] = runner
	slog.Info("Launching StartTimers for partition", "partition_id", id, "timer_count", len(mgr.Partition.Timers))
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(runner.done)
		mgr.StartTimers(partitionCtx)
	}()
}

func (s *Service) stopPartition(id string, runner *partitionRunner) {
	runner.cancel()
	<-runner.done
	delete(s.partitions, id)
}

func (s *Service) stopAllPartitions() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, runner := range s.partitions {
		slog.Info("Stopping partition", "partition_id", id)
		s.stopPartition(id, runner)
	}
}

func partitionsEqual(a, b *model.Partition) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.ID != b.ID || len(a.Timers) != len(b.Timers) {
		return false
	}

	aTimers := sortedTimers(a.Timers)
	bTimers := sortedTimers(b.Timers)
	for i := range aTimers {
		if !timersEqual(aTimers[i], bTimers[i]) {
			return false
		}
	}
	return true
}

func sortedTimers(timers []*model.Timer) []*model.Timer {
	sorted := append([]*model.Timer(nil), timers...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i] == nil || sorted[j] == nil {
			return sorted[j] != nil
		}
		return sorted[i].ID < sorted[j].ID
	})
	return sorted
}

func timersEqual(a, b *model.Timer) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.ID == b.ID &&
		a.Partition == b.Partition &&
		a.Interval == b.Interval &&
		a.Once == b.Once
}
