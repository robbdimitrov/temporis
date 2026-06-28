package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"temporis/internal/config"
	"temporis/internal/gossip"
	"temporis/internal/service"
	"temporis/internal/store"
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

	database, err := store.NewDatabaseStore(cfg.DatabaseURL)
	if err != nil {
		slog.Error("Failed to init database", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	cache, err := store.NewCacheStore(cfg.CacheURL)
	if err != nil {
		slog.Error("Failed to init cache", "error", err)
		os.Exit(1)
	}
	defer cache.Close()

	gossipMgr, err := gossip.NewGossipManager(cfg.GossipPort, cfg.ServiceName)
	if err != nil {
		slog.Error("Failed to init gossip", "error", err)
		os.Exit(1)
	}
	defer gossipMgr.Shutdown()

	svc, err := service.NewService(cfg, database, cache, gossipMgr)
	if err != nil {
		slog.Error("Failed to init service", "error", err)
		os.Exit(1)
	}

	done := make(chan struct{})
	serviceErrCh := make(chan error, 1)
	var serviceFailed atomic.Bool
	go func() {
		defer close(done)
		slog.Info("Starting service...")
		if err := svc.Run(ctx); err != nil {
			serviceFailed.Store(true)
			slog.Error("Service error", "error", err)
			serviceErrCh <- err
		}
	}()

	// Start HTTP server for probes
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if serviceFailed.Load() {
			http.Error(w, "service failed", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !svc.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})
	probeSrv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}
	go func() {
		slog.Info("Starting probe server on :8080")
		if err := probeSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Probe server failed", "error", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigCh:
		slog.Info("Shutting down... sending cancel to service")
	case <-serviceErrCh:
		slog.Info("Service failed, shutting down...")
	}
	cancel()

	// Shut down probe server
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer probeCancel()
	_ = probeSrv.Shutdown(probeCtx)

	// Wait for the service to drain all running partition timers
	<-done
	slog.Info("Service stopped gracefully, closing database connections...")
}
