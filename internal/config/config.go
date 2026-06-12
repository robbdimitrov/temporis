package config

import (
	"os"
	"strconv"
)

type Config struct {
	ServiceName string
	GossipPort  int64
	PostgresURL string
	RedisURL    string
	SeedNode    string
}

func Load() (*Config, error) {
	return &Config{
		ServiceName: getEnv("SERVICE_NAME", "temporis"),
		GossipPort:  parseInt(getEnv("GOSSIP_PORT", "7946")),
		PostgresURL: getEnv("POSTGRES_URL", "postgres://postgres:password@localhost:5432/timers?sslmode=disable"),
		RedisURL:    getEnv("REDIS_URL", "redis://localhost:6379"),
		SeedNode:    getEnv("SEED_NODE", "localhost:7946"),
	}, nil
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func parseInt(s string) int64 {
	res, _ := strconv.ParseInt(s, 10, 64)
	return res
}
