package config

import (
	"os"
	"testing"
)

func TestLoad(t *testing.T) {
	// Setup test environment variables
	os.Setenv("SERVICE_NAME", "test-service")
	os.Setenv("GOSSIP_PORT", "1234")
	os.Setenv("VALKEY_URL", "redis://test:6379")
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
	if cfg.ValkeyURL != "redis://test:6379" {
		t.Errorf("expected redis://test:6379, got %v", cfg.ValkeyURL)
	}
	// Fallback test
	if cfg.PostgresURL != "postgres://postgres:password@localhost:5432/temporis?sslmode=disable" {
		t.Errorf("expected default postgres url, got %v", cfg.PostgresURL)
	}
}

func TestParseInt_Invalid(t *testing.T) {
	res := parseInt("invalid")
	if res != 0 {
		t.Errorf("expected 0 for invalid int, got %v", res)
	}
}
