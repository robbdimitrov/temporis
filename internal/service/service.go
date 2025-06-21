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
	cfg        *config.Config
	pgStore    *storage.PostgresStore
	redisStore *storage.RedisStore
	gossipMgr  *gossip.GossipManager
	hashRing   *hash.ConsistentHash
	partitions map[string]*partition.Manager
	mu         sync.Mutex
}

func NewService(cfg *config.Config, pgStore *storage.PostgresStore, redisStore *storage.RedisStore, gossipMgr *gossip.GossipManager) (*Service, error) {
	hashRing := hash.NewConsistentHash(100) // 100 virtual nodes
	return &Service{
		cfg:        cfg,
		pgStore:    pgStore,
		redisStore: redisStore,
		gossipMgr:  gossipMgr,
		hashRing:   hashRing,
		partitions: make(map[string]*partition.Manager),
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

	// Update hash ring with current members
	currentNodes := s.gossipMgr.Members()
	for _, node := range currentNodes {
		s.hashRing.AddNode(node)
	}

	// Load partitions from postgres
	partitions, err := s.pgStore.GetPartitions()
	if err != nil {
		log.Printf("Failed to load partitions: %v", err)
		return
	}

	// Redistribute partitions
	newPartitions := make(map[string]*partition.Manager)
	for _, p := range partitions {
		owner := s.hashRing.GetNode(p.ID)
		if owner == s.cfg.ServiceName {
			newPartitions[p.ID] = partition.NewManager(p)
		}
	}

	// Stop old partitions we no longer own
	for id := range s.partitions {
		if _, exists := newPartitions[id]; !exists {
			delete(s.partitions, id)
		}
	}

	// Start new partitions
	for id, mgr := range newPartitions {
		if _, exists := s.partitions[id]; !exists {
			s.partitions[id] = mgr
			go mgr.StartTimers(ctx, s.redisStore.RecordFiring)
		}
	}
}
