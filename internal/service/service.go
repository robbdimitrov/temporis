package service

import (
	"context"
	"log"
	"sync"
	"time"

	"timer-service/internal/config"
	"timer-service/internal/gossip"
	"timer-service/internal/hash"
	"timer-service/internal/partition"
	"timer-service/internal/storage"
)

type Service struct {
	cfg         *config.Config
	pgStore     *storage.PostgresStore
	redisStore  *storage.RedisStore
	gossipMgr   *gossip.GossipManager
	hashRing    *hash.ConsistentHash
	partitions  map[string]*partition.Manager
	cancelFuncs map[string]context.CancelFunc // Store cancellation functions for each partition
	mu          sync.Mutex
}

func NewService(cfg *config.Config, pgStore *storage.PostgresStore, redisStore *storage.RedisStore, gossipMgr *gossip.GossipManager) (*Service, error) {
	hashRing := hash.NewConsistentHash(100) // 100 virtual nodes
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
	// Join gossip cluster
	if err := s.gossipMgr.Join([]string{}); err != nil {
		log.Printf("Failed to join gossip: %v", err)
	}

	// Periodic sync
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.syncWithCluster(ctx)
		}
	}
}

func (s *Service) syncWithCluster(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Print("Syncing with cluster...")

	// Step 1: Get the current list of active nodes from the gossip protocol
	currentNodes := s.gossipMgr.Members()

	// Step 2: Track nodes currently in the hash ring
	existingNodes := make(map[string]bool)
	for node := range s.hashRing.Nodes() {
		existingNodes[node] = true
	}

	// Step 3: Update the hash ring
	// Add new nodes
	for _, node := range currentNodes {
		s.hashRing.AddNode(node)
		delete(existingNodes, node) // Remove from existingNodes to track nodes that left
	}

	// Remove nodes that are no longer in the member list
	for node := range existingNodes {
		s.hashRing.RemoveNode(node)
		log.Printf("Removed node %s from hash ring", node)
	}

	// Step 4: Load partitions from PostgreSQL
	partitions, err := s.pgStore.GetPartitions()
	if err != nil {
		log.Printf("Failed to load partitions: %v", err)
		return
	}

	// Step 5: Redistribute partitions
	newPartitions := make(map[string]*partition.Manager)
	for _, p := range partitions {
		owner := s.hashRing.GetNode(p.ID)
		if owner == s.cfg.ServiceName {
			newPartitions[p.ID] = partition.NewManager(p)
		}
	}

	// Step 6: Stop partitions this pod no longer owns
	for id, cancel := range s.cancelFuncs {
		if _, exists := newPartitions[id]; !exists {
			cancel() // Cancel the context to stop timers
			delete(s.partitions, id)
			delete(s.cancelFuncs, id)
			log.Printf("Stopped managing partition %s", id)
		}
	}

	// Step 7: Start new partitions assigned to this pod
	for id, mgr := range newPartitions {
		if _, exists := s.partitions[id]; !exists {
			// Create a new context for the partition
			partitionCtx, cancel := context.WithCancel(ctx)
			s.partitions[id] = mgr
			s.cancelFuncs[id] = cancel
			go mgr.StartTimers(partitionCtx, s.redisStore.RecordFiring)
			log.Printf("Started managing partition %s", id)
		}
	}
}
