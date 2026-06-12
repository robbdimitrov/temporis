package model

// Partition represents a partition with associated timers.
type Partition struct {
	ID     string
	Timers []*Timer
}

// NewPartition creates a new Partition instance.
func NewPartition(id string, timers []*Timer) *Partition {
	return &Partition{
		ID:     id,
		Timers: timers,
	}
}
