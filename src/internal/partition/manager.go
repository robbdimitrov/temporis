package partition

import (
	"context"
	"hash/fnv"
	"log/slog"
	"math"
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
	// GetLastFirings returns the recent firing history for a recurring timer.
	GetLastFirings(ctx context.Context, timerID string) ([]time.Time, error)
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

// StartTimers begins execution of all timers in the partition.
// It blocks until all timer goroutines have exited (e.g., after context cancellation).
func (m *Manager) StartTimers(ctx context.Context) {
	logger := slog.With("partition_id", m.Partition.ID)

	logger.Info("StartTimers called", "timer_count", len(m.Partition.Timers))
	if len(m.Partition.Timers) == 0 {
		logger.Info("No timers to start")
		return
	}

	var wg sync.WaitGroup
	for i, timer := range m.Partition.Timers {
		if timer.ID == "" {
			logger.Warn("Invalid timer: empty ID, skipping", "index", i)
			continue
		}
		if timer.Interval <= 0 {
			logger.Warn("Invalid timer: interval <= 0, skipping", "timer_id", timer.ID, "interval", timer.Interval)
			continue
		}
		wg.Add(1)
		go func(t *model.Timer) {
			defer wg.Done()
			m.startTimer(ctx, t, logger.With("timer_id", t.ID))
		}(timer)
	}
	wg.Wait()
}

// startTimer executes a single timer's logic.
func (m *Manager) startTimer(ctx context.Context, timer *model.Timer, logger *slog.Logger) {
	var nextFire time.Time

	// 1. Determine the target time for the next firing
	if timer.Once {
		if m.tracker.HasFired(ctx, timer.ID) {
			logger.Info("One-time timer already claimed, skipping")
			return
		}
		scheduledAt, err := m.scheduler.ScheduleOnce(ctx, timer.ID)
		if err != nil {
			logger.Warn("Failed to get schedule time, using full interval", "error", err)
			nextFire = time.Now().Add(timer.Interval)
		} else {
			nextFire = scheduledAt.Add(timer.Interval)
		}
	} else {
		if firings, err := m.tracker.GetLastFirings(ctx, timer.ID); err == nil && len(firings) > 0 {
			nextFire = firings[0].Add(timer.Interval)
		} else {
			if err != nil {
				logger.Warn("Failed to get last firings, using full interval", "error", err)
			}
			nextFire = time.Now().Add(timer.Interval)
		}
	}

	// Calculate maximum jitter once.
	// Base jitter is 10% of the interval.
	maxJitter := timer.Interval / 10

	// Dynamically expand the jitter window for extremely heavy partitions
	// Cap the total spread at 1 hour to maintain an acceptable recovery SLA.
	volumeJitter := time.Duration(len(m.Partition.Timers)) * 2 * time.Millisecond
	if volumeJitter > time.Hour {
		volumeJitter = time.Hour
	}
	if volumeJitter > maxJitter {
		maxJitter = volumeJitter
	}

	// Never jitter longer than the timer's own interval to preserve logical ordering
	if maxJitter > timer.Interval {
		maxJitter = timer.Interval
	}

	// Pre-allocate a stopped timer to avoid GC pressure in the recurring loop
	timerObj := time.NewTimer(math.MaxInt64)
	timerObj.Stop()

	// 2. Execution Loop
	for {
		delay := time.Until(nextFire)

		var jitter time.Duration
		if maxJitter > 0 {
			// Deterministic Hashing: consistently assign the same timer to the same
			// bucket across restarts, avoiding random double-penalty delays.
			h := fnv.New32a()
			h.Write([]byte(timer.ID))
			hashVal := int64(h.Sum32())

			// Randomize into discrete 1-minute buckets.
			// Matches standard cron granularity and maximizes database pool reuse.
			bucketSize := int64(time.Minute)
			if numBuckets := int64(maxJitter) / bucketSize; numBuckets > 1 {
				jitter = time.Duration((hashVal % numBuckets) * bucketSize)
			} else {
				// For intervals smaller than the bucket size, fallback to continuous jitter
				jitter = time.Duration(hashVal % int64(maxJitter))
			}
		}

		if delay <= 0 {
			// Catch-up mode: ignore the negative delay but still wait the jitter duration
			// to spread out the thundering herd of past-due timers.
			logger.Info("Timer is past due, applying jitter for catch-up", "missed_by", -delay, "jitter", jitter)
			delay = jitter
		} else {
			// Normal mode: wait until nextFire plus some jitter
			delay += jitter
		}

		timerObj.Reset(delay)
		select {
		case <-timerObj.C:
		case <-ctx.Done():
			timerObj.Stop()
			logger.Info("Timer cancelled by context", "error", ctx.Err())
			return
		}

		if timer.Once {
			claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			claimed := m.tracker.ClaimFiring(claimCtx, timer.ID, time.Now())
			cancel()
			if !claimed {
				logger.Info("One-time timer already claimed by another node, skipping")
				return
			}
			logger.Info("Firing one-time timer")
			timer.Callback()
			return
		}

		// Recurring timer
		if ctx.Err() != nil {
			logger.Info("Timer cancelled by context right before execution", "error", ctx.Err())
			return
		}

		logger.Info("Firing recurring timer")
		timer.Callback()
		m.tracker.RecordFiring(ctx, timer.ID, time.Now())

		// Align to the next future multiple of the interval to prevent rapid-fire cascades
		now := time.Now()
		for !nextFire.After(now) {
			nextFire = nextFire.Add(timer.Interval)
		}
	}
}
