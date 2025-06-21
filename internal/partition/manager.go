package partition

import (
	"context"
	"log"
	"sync"
	"time"

	"timer-service/internal/model"
)

// Manager handles the execution of timers within a partition.
type Manager struct {
	Partition *model.Partition
	mu        sync.Mutex
}

// NewManager creates a new Manager for a partition.
func NewManager(partition *model.Partition) *Manager {
	return &Manager{
		Partition: partition,
	}
}

// StartTimers starts all timers in the partition, using the provided recordFiring function to log firings.
func (m *Manager) StartTimers(ctx context.Context, recordFiring func(timerID string, t time.Time) error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, timer := range m.Partition.Timers {
		go m.runTimer(ctx, timer, recordFiring)
	}
}

// runTimer executes a single timer's logic.
func (m *Manager) runTimer(ctx context.Context, timer *model.Timer, recordFiring func(timerID string, t time.Time) error) {
	if timer.Once {
		select {
		case <-time.After(timer.Interval):
			timer.Callback()
			if err := recordFiring(timer.ID, time.Now()); err != nil {
				log.Printf("failed to record firing for timer %s: %v", timer.ID, err)
			}
		case <-ctx.Done():
			return
		}
		return
	}

	// Recurring timer
	ticker := time.NewTicker(timer.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			timer.Callback()
			if err := recordFiring(timer.ID, time.Now()); err != nil {
				log.Printf("failed to record firing for timer %s: %v", timer.ID, err)
			}
		case <-ctx.Done():
			return
		}
	}
}
