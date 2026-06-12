package storage

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(url string) (*RedisStore, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("Failed to ping Redis: %v", err)
		return nil, err
	}
	log.Printf("Connected to Redis at %s", opts.Addr)
	return &RedisStore{client}, nil
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}

func (s *RedisStore) HasFired(timerID string) bool {
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		exists, err := s.client.Exists(ctx, "firings:"+timerID).Result()
		if err == nil {
			return exists == 1
		}
		log.Printf("Failed to check fired status for timer %s (attempt %d): %v", timerID, i+1, err)
		time.Sleep(100 * time.Millisecond)
	}
	log.Printf("Failed to check fired status for timer %s after retries, assuming not fired", timerID)
	return false
}

func (s *RedisStore) RecordFiring(timerID string, t time.Time) bool {
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		pipe := s.client.Pipeline()
		// Add timestamp to firings list
		pipe.LPush(ctx, "firings:"+timerID, t.UnixNano())
		// Keep only the last 10 entries
		pipe.LTrim(ctx, "firings:"+timerID, 0, 9)
		
		_, err := pipe.Exec(ctx)
		if err == nil {
			log.Printf("Recorded firing for timer %s at %v", timerID, t)
			return true
		}
		log.Printf("Failed to record firing for timer %s (attempt %d): %v", timerID, i+1, err)
		time.Sleep(100 * time.Millisecond)
	}
	log.Printf("Failed to record firing for timer %s after retries", timerID)
	return false
}

func (s *RedisStore) GetLastFirings(timerID string) ([]time.Time, error) {
	ctx := context.Background()
	timestamps, err := s.client.LRange(ctx, "firings:"+timerID, 0, 9).Result()
	if err != nil {
		log.Printf("Failed to get last firings for timer %s: %v", timerID, err)
		return nil, err
	}
	firings := make([]time.Time, 0, len(timestamps))
	for _, ts := range timestamps {
		nano, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			log.Printf("Invalid timestamp for timer %s: %v", timerID, ts)
			continue
		}
		firings = append(firings, time.Unix(0, nano))
	}
	log.Printf("Retrieved %d firings for timer %s", len(firings), timerID)
	return firings, nil
}
