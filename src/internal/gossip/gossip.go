package gossip

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/hashicorp/memberlist"
)

type GossipManager struct {
	list *memberlist.Memberlist
}

func NewGossipManager(port int64, serviceName string) (*GossipManager, error) {
	config := memberlist.DefaultLANConfig()
	config.Name = serviceName
	config.BindPort = int(port)
	// Note: memberlist only supports standard io.Writer, so we don't route it to slog.
	// In a real app we might bridge it, but for now we let it use standard log or discard.
	// config.LogOutput = log.Writer() // Enable verbose logging for debugging
	list, err := memberlist.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %v", err)
	}
	slog.Info("Gossip manager initialized", "node_name", config.Name)
	return &GossipManager{list: list}, nil
}

func (gm *GossipManager) Join(peers []string) error {
	if len(peers) == 0 {
		slog.Info("No seed peers provided, starting as first node")
		return nil
	}

	// Retry joining the cluster
	for i := 0; i < 3; i++ {
		joined, err := gm.list.Join(peers)
		if err != nil {
			slog.Warn("Failed to join gossip cluster", "attempt", i+1, "error", err)
			time.Sleep(2 * time.Second)
			continue
		}
		slog.Info("Joined gossip cluster", "peers_connected", joined)
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
		slog.Error("Failed to leave gossip", "error", err)
	}
	if err := gm.list.Shutdown(); err != nil {
		slog.Error("Failed to shutdown gossip", "error", err)
	}
}
