package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ServiceName string
	GossipPort  int64
	DatabaseURL string
	CacheURL    string
	SeedNode    string
}

func Load() (*Config, error) {
	gossipPort, err := parseInt(getEnv("GOSSIP_PORT", "7946"))
	if err != nil {
		return nil, fmt.Errorf("invalid GOSSIP_PORT: %w", err)
	}

	return &Config{
		ServiceName: getEnv("SERVICE_NAME", "temporis"),
		GossipPort:  gossipPort,
		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:password@localhost:5432/temporis?sslmode=disable"),
		CacheURL:    getEnv("CACHE_URL", "redis://localhost:6379"),
		SeedNode:    getEnv("SEED_NODE", "localhost:7946"),
	}, nil
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func parseInt(s string) (int64, error) {
	res, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return res, nil
}
