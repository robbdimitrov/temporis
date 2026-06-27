package storage

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type ValkeyStore struct {
	client *redis.Client
}

func NewValkeyStore(url string) (*ValkeyStore, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		slog.Error("Failed to ping Valkey", "error", err)
		return nil, err
	}
	slog.Info("Connected to Valkey", "addr", opts.Addr)
	return &ValkeyStore{client}, nil
}

func (s *ValkeyStore) Close() error {
	return s.client.Close()
}

func (s *ValkeyStore) HasFired(ctx context.Context, timerID string) bool {
	for i := 0; i < 3; i++ {
		opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		exists, err := s.client.Exists(opCtx, "firings:"+timerID).Result()
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

func (s *ValkeyStore) RecordFiring(ctx context.Context, timerID string, t time.Time) bool {
	for i := 0; i < 3; i++ {
		opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		pipe := s.client.Pipeline()
		// Add timestamp to firings list
		pipe.LPush(opCtx, "firings:"+timerID, t.UnixNano())
		// Keep only the last 10 entries
		pipe.LTrim(opCtx, "firings:"+timerID, 0, 9)
		
		_, err := pipe.Exec(opCtx)
		cancel()
		if err == nil {
			slog.Info("Recorded firing", "timer_id", timerID, "time", t)
			return true
		}
		slog.Warn("Failed to record firing", "timer_id", timerID, "attempt", i+1, "error", err)
		time.Sleep(100 * time.Millisecond)
	}
	slog.Error("Failed to record firing after retries", "timer_id", timerID)
	return false
}

func (s *ValkeyStore) GetLastFirings(ctx context.Context, timerID string) ([]time.Time, error) {
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
