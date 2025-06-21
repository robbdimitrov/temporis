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

	log.Printf("Starting %d timers for partition %s", len(m.Partition.Timers), m.Partition.ID)
	if len(m.Partition.Timers) == 0 {
		log.Printf("No timers to start for partition %s", m.Partition.ID)
		return
	}

	for _, timer := range m.Partition.Timers {
		if timer.ID == "" || timer.Interval <= 0 {
			log.Printf("Invalid timer for partition %s: ID=%s, Interval=%v, skipping", m.Partition.ID, timer.ID, timer.Interval)
			continue
		}
		log.Printf("Starting timer %s (partition: %s, interval: %v, once: %v)", timer.ID, timer.Partition, timer.Interval, timer.Once)
		go m.startTimer(ctx, timer, recordFiring)
	}
}

// startTimer executes a single timer's logic.
func (m *Manager) startTimer(ctx context.Context, timer *model.Timer, recordFiring func(timerID string, t time.Time) error) {
	log.Printf("Timer %s goroutine started", timer.ID)
	if timer.Once {
		select {
		case <-time.After(timer.Interval):
			log.Printf("Firing one-time timer %s at %v", timer.ID, time.Now())
			timer.Callback()
			if err := recordFiring(timer.ID, time.Now()); err != nil {
				log.Printf("Failed to record firing for timer %s: %v", timer.ID, err)
			}
		case <-ctx.Done():
			log.Printf("Timer %s cancelled by context: %v", timer.ID, ctx.Err())
			return
		}
		return
	}

	// Recurring timer
	ticker := time.NewTicker(timer.Interval)
	defer ticker.Stop()

	log.Printf("Recurring timer %s ticker started with interval %v", timer.ID, timer.Interval)
	for {
		select {
		case t := <-ticker.C:
			log.Printf("Firing recurring timer %s at %v", timer.ID, t)
			timer.Callback()
			if err := recordFiring(timer.ID, time.Now()); err != nil {
				log.Printf("Failed to record firing for timer %s: %v", timer.ID, err)
			}
		case <-ctx.Done():
			log.Printf("Timer %s cancelled by context: %v", timer.ID, ctx.Err())
			return
		}
	}
}
