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

// ScheduleTracker persists the first pick-up timestamp for once-timers so that
// the target fire time is stable across partition rebalances.
type ScheduleTracker interface {
	ScheduleOnce(ctx context.Context, timerID string) (time.Time, error)
}

// Manager handles the execution of timers within a partition.
type Manager struct {
	Partition *model.Partition
	mu        sync.Mutex
	tracker   ExecutionTracker
	scheduler ScheduleTracker
}

// NewManager creates a new Manager for a partition.
func NewManager(partition *model.Partition, tracker ExecutionTracker, scheduler ScheduleTracker) *Manager {
	return &Manager{
		Partition: partition,
		tracker:   tracker,
		scheduler: scheduler,
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

		// Compute remaining wait from the stable first-schedule time so that
		// rebalancing nodes resume mid-interval rather than restart it.
		remaining := timer.Interval
		scheduledAt, err := m.scheduler.ScheduleOnce(ctx, timer.ID)
		if err != nil {
			logger.Warn("Failed to get schedule time, using full interval", "error", err)
		} else {
			remaining = time.Until(scheduledAt.Add(timer.Interval))
		}

		// claimAndFire uses a non-cancellable context so a concurrent partition
		// handoff cannot prevent the SET NX from completing.
		claimAndFire := func() {
			claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if !m.tracker.ClaimFiring(claimCtx, timer.ID, time.Now()) {
				logger.Info("One-time timer already claimed by another node, skipping")
				return
			}
			logger.Info("Firing one-time timer")
			timer.Callback()
		}

		if remaining <= 0 {
			claimAndFire()
			return
		}

		timerObj := time.NewTimer(remaining)
		defer timerObj.Stop()

		select {
		case <-timerObj.C:
			claimAndFire()
		case <-ctx.Done():
			logger.Info("Timer cancelled by context", "error", ctx.Err())
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
