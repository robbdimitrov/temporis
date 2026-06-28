package config

import (
	"os"
	"testing"
)

func TestLoad(t *testing.T) {
	// Setup test environment variables
	os.Setenv("SERVICE_NAME", "test-service")
	os.Setenv("GOSSIP_PORT", "1234")
	os.Setenv("CACHE_URL", "redis://test:6379")
	defer os.Clearenv()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ServiceName != "test-service" {
		t.Errorf("expected test-service, got %v", cfg.ServiceName)
	}
	if cfg.GossipPort != 1234 {
		t.Errorf("expected 1234, got %v", cfg.GossipPort)
	}
	if cfg.CacheURL != "redis://test:6379" {
		t.Errorf("expected redis://test:6379, got %v", cfg.CacheURL)
	}
	// Fallback test
	if cfg.DatabaseURL != "postgres://postgres:password@localhost:5432/temporis?sslmode=disable" {
		t.Errorf("expected default database url, got %v", cfg.DatabaseURL)
	}
}

func TestParseInt_Invalid(t *testing.T) {
	res := parseInt("invalid")
	if res != 0 {
		t.Errorf("expected 0 for invalid int, got %v", res)
	}
}
