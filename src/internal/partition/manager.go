package partition

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"temporis/internal/model"
)

// ExecutionTracker defines how timer firings are recorded and checked.
type ExecutionTracker interface {
	HasFired(ctx context.Context, timerID string) bool
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
		// Check if timer has already fired
		if m.tracker.HasFired(ctx, timer.ID) {
			logger.Info("One-time timer has already fired, skipping")
			return
		}
		timerObj := time.NewTimer(timer.Interval)
		defer timerObj.Stop()

		select {
		case <-timerObj.C:
			logger.Info("Firing one-time timer")
			timer.Callback()
			m.tracker.RecordFiring(ctx, timer.ID, time.Now())
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
