package gossip

import (
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
)

type GossipManager struct {
	list *memberlist.Memberlist
}

func NewGossipManager(port int64, serviceName string) (*GossipManager, error) {
	config := memberlist.DefaultLANConfig()
	config.Name = fmt.Sprintf("%s-%d", serviceName, port)
	config.BindPort = int(port)
	config.LogOutput = log.Writer() // Enable verbose logging for debugging
	list, err := memberlist.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %v", err)
	}
	return &GossipManager{list: list}, nil
}

func (gm *GossipManager) Join(peers []string) error {
	if len(peers) == 0 {
		log.Println("No seed peers provided, starting as first node")
		return nil
	}

	// Retry joining the cluster
	for i := 0; i < 3; i++ {
		joined, err := gm.list.Join(peers)
		if err != nil {
			log.Printf("Failed to join gossip cluster (attempt %d): %v", i+1, err)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Printf("Joined gossip cluster, connected to %d peers", joined)
		return nil
	}
	return fmt.Errorf("failed to join gossip cluster after retries")
}

func (gm *GossipManager) Members() []string {
	members := gm.list.Members()
	nodes := make([]string, len(members))
	for i, m := range members {
		nodes[i] = m.Name
	}
	return nodes
}

func (gm *GossipManager) Shutdown() {
	if err := gm.list.Leave(0); err != nil {
		log.Printf("Failed to leave gossip: %v", err)
	}
	if err := gm.list.Shutdown(); err != nil {
		log.Printf("Failed to shutdown gossip: %v", err)
	}
}
