package partition

import (
	"context"
	"log"
	"sync"
	"time"

	"temporis/internal/model"
)

// Manager handles the execution of timers within a partition.
type Manager struct {
	Partition *model.Partition
	mu        sync.Mutex
	hasFired  func(timerID string) bool
}

// NewManager creates a new Manager for a partition.
func NewManager(partition *model.Partition, hasFired func(timerID string) bool) *Manager {
	return &Manager{
		Partition: partition,
		hasFired:  hasFired,
	}
}

// StartTimers starts all timers in the partition, using the provided recordFiring function to log firings.
func (m *Manager) StartTimers(ctx context.Context, recordFiring func(timerID string, t time.Time) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("StartTimers called for partition %s with %d timers", m.Partition.ID, len(m.Partition.Timers))
	if len(m.Partition.Timers) == 0 {
		log.Printf("No timers to start for partition %s", m.Partition.ID)
		return
	}

	for i, timer := range m.Partition.Timers {
		if timer.ID == "" {
			log.Printf("Invalid timer at index %d for partition %s: empty ID, skipping", i, m.Partition.ID)
			continue
		}
		if timer.Interval <= 0 {
			log.Printf("Invalid timer %s for partition %s: interval=%v, skipping", timer.ID, m.Partition.ID, timer.Interval)
			continue
		}
		go m.startTimer(ctx, timer, recordFiring)
	}
}

// startTimer executes a single timer's logic.
func (m *Manager) startTimer(ctx context.Context, timer *model.Timer, recordFiring func(timerID string, t time.Time) bool) {
	if timer.Once {
		// Check if timer has already fired
		if m.hasFired(timer.ID) {
			log.Printf("One-time timer %s has already fired, skipping", timer.ID)
			return
		}
		timerObj := time.NewTimer(timer.Interval)
		defer timerObj.Stop()

		select {
		case <-timerObj.C:
			log.Printf("Firing one-time timer %s at %v", timer.ID, time.Now())
			timer.Callback()
			recordFiring(timer.ID, time.Now())
		case <-ctx.Done():
			log.Printf("Timer %s cancelled by context: %v", timer.ID, ctx.Err())
			return
		}
		return
	}

	// Recurring timer
	ticker := time.NewTicker(timer.Interval)
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			log.Printf("Firing recurring timer %s at %v", timer.ID, t)
			timer.Callback()
			recordFiring(timer.ID, time.Now())
		case <-ctx.Done():
			log.Printf("Timer %s cancelled by context: %v", timer.ID, ctx.Err())
			return
		}
	}
}
