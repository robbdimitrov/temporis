package store

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type CacheStore struct {
	client *redis.Client
}

func NewCacheStore(url string) (*CacheStore, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		slog.Error("Failed to ping cache", "error", err)
		return nil, err
	}
	slog.Info("Connected to cache", "addr", opts.Addr)
	return &CacheStore{client}, nil
}

func (s *CacheStore) Close() error {
	return s.client.Close()
}

// HasFired checks whether a one-time timer has already been claimed.
// Non-atomic: used only as a pre-check to skip the interval wait early.
func (s *CacheStore) HasFired(ctx context.Context, timerID string) bool {
	for i := 0; i < 3; i++ {
		opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		exists, err := s.client.Exists(opCtx, "fired:"+timerID).Result()
		cancel()
		if err == nil {
			return exists == 1
		}
		slog.Warn("Failed to check fired status", "timer_id", timerID, "attempt", i+1, "error", err)
		time.Sleep(100 * time.Millisecond)
	}
	slog.Error("Failed to check fired status after retries, assuming not fired", "timer_id", timerID)
	return false
}

// ClaimFiring atomically claims the firing slot for a one-time timer (SET NX).
// Returns true if this caller won the claim. Returns false on failure to avoid
// firing without a confirmed claim.
func (s *CacheStore) ClaimFiring(ctx context.Context, timerID string, t time.Time) bool {
	for i := 0; i < 3; i++ {
		opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		claimed, err := s.client.SetNX(opCtx, "fired:"+timerID, t.UnixNano(), 0).Result()
		cancel()
		if err == nil {
			return claimed
		}
		slog.Warn("Failed to claim firing", "timer_id", timerID, "attempt", i+1, "error", err)
		time.Sleep(100 * time.Millisecond)
	}
	slog.Error("Failed to claim firing after retries, skipping execution", "timer_id", timerID)
	return false
}

// ClaimRecurringFiring fences a recurring timer callback and records the
// scheduled fire time before execution. Return values match partition's
// Recurring* constants without importing that package.
func (s *CacheStore) ClaimRecurringFiring(ctx context.Context, timerID string, scheduledAt time.Time, lockTTL time.Duration) int {
	const (
		recurringClaimed = 1
		recurringBusy    = 2
	)

	if lockTTL <= 0 {
		lockTTL = time.Minute
	}

	runningKey := "recurring:running:" + timerID
	claimKey := recurringClaimKey(timerID, scheduledAt)
	firingsKey := "firings:" + timerID
	lockValue := strconv.FormatInt(scheduledAt.UnixNano(), 10)
	scheduledNano := strconv.FormatInt(scheduledAt.UnixNano(), 10)

	script := `
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 2
end
if redis.call("EXISTS", KEYS[2]) == 1 then
	return 0
end
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
redis.call("SET", KEYS[2], ARGV[1])
redis.call("LPUSH", KEYS[3], ARGV[3])
redis.call("LTRIM", KEYS[3], 0, 9)
return 1
`

	for i := 0; i < 3; i++ {
		opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		result, err := s.client.Eval(opCtx, script, []string{runningKey, claimKey, firingsKey}, lockValue, lockTTL.Milliseconds(), scheduledNano).Int()
		cancel()
		if err == nil {
			if result == recurringClaimed {
				slog.Info("Claimed recurring firing", "timer_id", timerID, "scheduled_at", scheduledAt)
			}
			return result
		}
		slog.Warn("Failed to claim recurring firing", "timer_id", timerID, "attempt", i+1, "error", err)
		time.Sleep(100 * time.Millisecond)
	}
	slog.Error("Failed to claim recurring firing after retries", "timer_id", timerID)
	return recurringBusy
}

func (s *CacheStore) ReleaseRecurringFiring(ctx context.Context, timerID string, scheduledAt time.Time) bool {
	runningKey := "recurring:running:" + timerID
	lockValue := strconv.FormatInt(scheduledAt.UnixNano(), 10)
	script := `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`

	for i := 0; i < 3; i++ {
		opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		released, err := s.client.Eval(opCtx, script, []string{runningKey}, lockValue).Int()
		cancel()
		if err == nil {
			return released == 1
		}
		slog.Warn("Failed to release recurring firing", "timer_id", timerID, "attempt", i+1, "error", err)
		time.Sleep(100 * time.Millisecond)
	}
	slog.Error("Failed to release recurring firing after retries", "timer_id", timerID)
	return false
}

// GetLastFirings returns the last 10 firing times for a recurring timer.
func (s *CacheStore) GetLastFirings(ctx context.Context, timerID string) ([]time.Time, error) {
	opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	timestamps, err := s.client.LRange(opCtx, "firings:"+timerID, 0, 9).Result()
	if err != nil {
		slog.Error("Failed to get last firings", "timer_id", timerID, "error", err)
		return nil, err
	}
	firings := make([]time.Time, 0, len(timestamps))
	for _, ts := range timestamps {
		nano, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			slog.Warn("Invalid timestamp", "timer_id", timerID, "timestamp", ts, "error", err)
			continue
		}
		firings = append(firings, time.Unix(0, nano))
	}
	slog.Info("Retrieved firings", "timer_id", timerID, "count", len(firings))
	return firings, nil
}

func recurringClaimKey(timerID string, scheduledAt time.Time) string {
	return "recurring:claimed:" + timerID + ":" + strconv.FormatInt(scheduledAt.UnixNano(), 10)
}
