package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"temporis/internal/config"
	"temporis/internal/gossip"
	"temporis/internal/service"
	"temporis/internal/storage"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	pgStore, err := storage.NewPostgresStore(cfg.PostgresURL)
	if err != nil {
		log.Fatalf("Failed to init postgres: %v", err)
	}
	defer pgStore.Close()

	redisStore, err := storage.NewRedisStore(cfg.RedisURL)
	if err != nil {
		log.Fatalf("Failed to init redis: %v", err)
	}
	defer redisStore.Close()

	gossipMgr, err := gossip.NewGossipManager(cfg.GossipPort, cfg.ServiceName)
	if err != nil {
		log.Fatalf("Failed to init gossip: %v", err)
	}
	defer gossipMgr.Shutdown()

	svc, err := service.NewService(cfg, pgStore, redisStore, gossipMgr)
	if err != nil {
		log.Fatalf("Failed to init service: %v", err)
	}

	go func() {
		log.Println("Starting service...")
		if err := svc.Run(ctx); err != nil {
			log.Printf("Service error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down...")
}
