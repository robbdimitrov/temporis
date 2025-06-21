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
}

func Load() (*Config, error) {
	return &Config{
		ServiceName: getEnv("SERVICE_NAME", "timer-service"),
		GossipPort:  parseInt(getEnv("GOSSIP_PORT", "7946")),
		PostgresURL: getEnv("POSTGRES_URL", "postgres://user:pass@localhost:5432/timers?sslmode=disable"),
		RedisURL:    getEnv("REDIS_URL", "redis://localhost:6379"),
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
