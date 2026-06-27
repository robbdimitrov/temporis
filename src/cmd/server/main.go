package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"temporis/internal/config"
	"temporis/internal/gossip"
	"temporis/internal/service"
	"temporis/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	pgStore, err := storage.NewPostgresStore(cfg.PostgresURL)
	if err != nil {
		slog.Error("Failed to init postgres", "error", err)
		os.Exit(1)
	}
	defer pgStore.Close()

	valkeyStore, err := storage.NewValkeyStore(cfg.ValkeyURL)
	if err != nil {
		slog.Error("Failed to init valkey", "error", err)
		os.Exit(1)
	}
	defer valkeyStore.Close()

	gossipMgr, err := gossip.NewGossipManager(cfg.GossipPort, cfg.ServiceName)
	if err != nil {
		slog.Error("Failed to init gossip", "error", err)
		os.Exit(1)
	}
	defer gossipMgr.Shutdown()

	svc, err := service.NewService(cfg, pgStore, valkeyStore, gossipMgr)
	if err != nil {
		slog.Error("Failed to init service", "error", err)
		os.Exit(1)
	}

	go func() {
		slog.Info("Starting service...")
		if err := svc.Run(ctx); err != nil {
			slog.Error("Service error", "error", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	slog.Info("Shutting down...")
}
