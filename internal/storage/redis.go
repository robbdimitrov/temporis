package storage

import (
	"context"
	"log"
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
	return &RedisStore{client}, nil
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}

func (s *RedisStore) RecordFiring(timerID string, t time.Time) error {
	err := s.client.Set(context.Background(), "firing:"+timerID, t.UnixNano(), 0).Err()
	if err != nil {
		log.Printf("Failed to record firing for timer %s in Redis: %v", timerID, err)
		return err
	}
	log.Printf("Recorded firing for timer %s in Redis at %v", timerID, t)
	return nil
}
