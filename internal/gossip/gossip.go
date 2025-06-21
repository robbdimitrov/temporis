package gossip

import (
	"fmt"
	"log"

	"github.com/hashicorp/memberlist"
)

type GossipManager struct {
	list *memberlist.Memberlist
}

func NewGossipManager(port int64, serviceName string) (*GossipManager, error) {
	config := memberlist.DefaultLANConfig()
	config.Name = fmt.Sprintf("%s-%d", serviceName, port)
	config.BindPort = int(port)
	list, err := memberlist.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %v", err)
	}
	return &GossipManager{list: list}, nil
}

func (gm *GossipManager) Join(peers []string) error {
	if len(peers) > 0 {
		_, err := gm.list.Join(peers)
		return err
	}
	return nil
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
