package service

import (
	"context"
	"log"
	"sync"
	"time"

	"temporis/internal/config"
	"temporis/internal/gossip"
	"temporis/internal/hash"
	"temporis/internal/partition"
	"temporis/internal/storage"
)

type Service struct {
	cfg         *config.Config
	pgStore     *storage.PostgresStore
	redisStore  *storage.RedisStore
	gossipMgr   *gossip.GossipManager
	hashRing    *hash.ConsistentHash
	partitions  map[string]*partition.Manager
	cancelFuncs map[string]context.CancelFunc
	mu          sync.Mutex
}

func NewService(cfg *config.Config, pgStore *storage.PostgresStore, redisStore *storage.RedisStore, gossipMgr *gossip.GossipManager) (*Service, error) {
	hashRing := hash.NewConsistentHash(100)
	return &Service{
		cfg:         cfg,
		pgStore:     pgStore,
		redisStore:  redisStore,
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
		log.Printf("Failed to join gossip: %v", err)
	}

	syncChan := make(chan struct{}, 1)
	triggerSync := func() {
		select {
		case syncChan <- struct{}{}:
		default:
		}
	}

	// Listen for Postgres notifications
	go s.pgStore.ListenForChanges(ctx, func() {
		log.Println("Database changed, executing instant sync...")
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
	log.Printf("Sync cycle: current nodes = %v", currentNodes)

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

	// Step 4: Load partitions from PostgreSQL
	partitions, err := s.pgStore.GetPartitions()
	if err != nil {
		log.Printf("Failed to load partitions: %v", err)
		return
	}
	log.Printf("Loaded %d partitions", len(partitions))

	// Step 5: Redistribute partitions
	newPartitions := make(map[string]*partition.Manager)
	for _, p := range partitions {
		owner := s.hashRing.GetNode(p.ID)
		log.Printf("Partition %s assigned to node %s (%d timers)", p.ID, owner, len(p.Timers))
		if owner == s.cfg.ServiceName {
			log.Printf("This pod (%s) owns partition %s", s.cfg.ServiceName, p.ID)
			newPartitions[p.ID] = partition.NewManager(p, s.redisStore.HasFired)
		}
	}

	// Step 6: Stop partitions this pod no longer owns
	for id, cancel := range s.cancelFuncs {
		if _, exists := newPartitions[id]; !exists {
			log.Printf("Stopping partition %s", id)
			cancel() // Cancel the context to stop timers
			delete(s.partitions, id)
			delete(s.cancelFuncs, id)
		}
	}

	log.Printf("Starting %d new partitions", len(newPartitions))
	// Step 7: Start new partitions assigned to this pod
	for id, mgr := range newPartitions {
		if _, exists := s.partitions[id]; !exists {
			// Create a new context for the partition
			partitionCtx, cancel := context.WithCancel(ctx)
			s.partitions[id] = mgr
			s.cancelFuncs[id] = cancel
			log.Printf("Launching StartTimers for partition %s with %d timers", id, len(mgr.Partition.Timers))
			go mgr.StartTimers(partitionCtx, s.redisStore.RecordFiring)
		} else {
			log.Printf("Partition %s already running with %d timers", id, len(mgr.Partition.Timers))
		}
	}
}
