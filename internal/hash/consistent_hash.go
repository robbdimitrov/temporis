package hash

import (
	"sort"
	"strconv"
	"sync"

	"github.com/spaolacci/murmur3"
)

type ConsistentHash struct {
	replicas int
	ring     []hashKey
	nodes    map[string]bool
	mu       sync.RWMutex
}

type hashKey struct {
	hash uint64
	node string
}

func NewConsistentHash(replicas int) *ConsistentHash {
	return &ConsistentHash{
		replicas: replicas,
		nodes:    make(map[string]bool),
	}
}

func (ch *ConsistentHash) AddNode(node string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.nodes[node] {
		return
	}
	ch.nodes[node] = true

	for i := 0; i < ch.replicas; i++ {
		hash := murmur3.Sum64([]byte(node + ":" + strconv.Itoa(i)))
		ch.ring = append(ch.ring, hashKey{hash, node})
	}
	sort.Slice(ch.ring, func(i, j int) bool {
		return ch.ring[i].hash < ch.ring[j].hash
	})
}

func (ch *ConsistentHash) RemoveNode(node string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.nodes[node] {
		return
	}
	delete(ch.nodes, node)

	newRing := make([]hashKey, 0, len(ch.ring))
	for _, hk := range ch.ring {
		if hk.node != node {
			newRing = append(newRing, hk)
		}
	}
	ch.ring = newRing
}

func (ch *ConsistentHash) GetNode(key string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return ""
	}

	hash := murmur3.Sum64([]byte(key))
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i].hash >= hash
	})
	if idx == len(ch.ring) {
		idx = 0
	}
	return ch.ring[idx].node
}
