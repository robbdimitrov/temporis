package partition

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"temporis/internal/model"
)

// ExecutionTracker records and checks timer firings.
type ExecutionTracker interface {
	// HasFired is a non-atomic pre-check; not the dedup gate.
	HasFired(ctx context.Context, timerID string) bool
	// ClaimFiring atomically claims the firing slot (SET NX).
	// Returns true if this caller won the claim.
	ClaimFiring(ctx context.Context, timerID string, t time.Time) bool
	// RecordFiring appends a timestamp to a recurring timer's history.
	RecordFiring(ctx context.Context, timerID string, t time.Time) bool
}

// Manager handles the execution of timers within a partition.
type Manager struct {
	Partition *model.Partition
	mu        sync.Mutex
	tracker   ExecutionTracker
}

// NewManager creates a new Manager for a partition.
func NewManager(partition *model.Partition, tracker ExecutionTracker) *Manager {
	return &Manager{
		Partition: partition,
		tracker:   tracker,
	}
}

// StartTimers starts all timers in the partition.
func (m *Manager) StartTimers(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger := slog.With("partition_id", m.Partition.ID)

	logger.Info("StartTimers called", "timer_count", len(m.Partition.Timers))
	if len(m.Partition.Timers) == 0 {
		logger.Info("No timers to start")
		return
	}

	for i, timer := range m.Partition.Timers {
		if timer.ID == "" {
			logger.Warn("Invalid timer: empty ID, skipping", "index", i)
			continue
		}
		if timer.Interval <= 0 {
			logger.Warn("Invalid timer: interval <= 0, skipping", "timer_id", timer.ID, "interval", timer.Interval)
			continue
		}
		go m.startTimer(ctx, timer, logger.With("timer_id", timer.ID))
	}
}

// startTimer executes a single timer's logic.
func (m *Manager) startTimer(ctx context.Context, timer *model.Timer, logger *slog.Logger) {
	if timer.Once {
		if m.tracker.HasFired(ctx, timer.ID) {
			logger.Info("One-time timer already claimed, skipping")
			return
		}
		timerObj := time.NewTimer(timer.Interval)
		defer timerObj.Stop()

		select {
		case <-timerObj.C:
			if !m.tracker.ClaimFiring(ctx, timer.ID, time.Now()) {
				logger.Info("One-time timer already claimed by another node, skipping")
				return
			}
			logger.Info("Firing one-time timer")
			timer.Callback()
		case <-ctx.Done():
			logger.Info("Timer cancelled by context", "error", ctx.Err())
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
			logger.Info("Firing recurring timer", "time", t)
			timer.Callback()
			m.tracker.RecordFiring(ctx, timer.ID, time.Now())
		case <-ctx.Done():
			logger.Info("Timer cancelled by context", "error", ctx.Err())
			return
		}
	}
}
