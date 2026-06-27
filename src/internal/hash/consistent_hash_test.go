package hash

import (
	"strconv"
	"testing"
)

func TestConsistentHash_AddNode(t *testing.T) {
	ch := NewConsistentHash(3)
	ch.AddNode("node1")

	if !ch.Nodes()["node1"] {
		t.Errorf("Expected node1 to be in nodes map")
	}

	if len(ch.ring) != 3 {
		t.Errorf("Expected ring to have 3 elements, got %d", len(ch.ring))
	}
}

func TestConsistentHash_RemoveNode(t *testing.T) {
	ch := NewConsistentHash(3)
	ch.AddNode("node1")
	ch.RemoveNode("node1")

	if ch.Nodes()["node1"] {
		t.Errorf("Expected node1 to be removed from nodes map")
	}

	if len(ch.ring) != 0 {
		t.Errorf("Expected ring to be empty, got %d", len(ch.ring))
	}
}

func TestConsistentHash_GetNode(t *testing.T) {
	ch := NewConsistentHash(3)

	// Should return empty if no nodes
	if node := ch.GetNode("test_key"); node != "" {
		t.Errorf("Expected empty node, got %s", node)
	}

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	nodeCount := make(map[string]int)
	for i := 0; i < 100; i++ {
		node := ch.GetNode("key" + strconv.Itoa(i))
		nodeCount[node]++
	}

	if len(nodeCount) == 0 {
		t.Errorf("Expected keys to be distributed")
	}
}
