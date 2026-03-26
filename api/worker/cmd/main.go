package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/worker"
)

func main() {
	slog.Info("starting page-cache worker")

	_ = godotenv.Load()
	_ = godotenv.Load("api/.env")

	if err := config.Load(); err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	defer config.Close()

	if err := config.LoadPostgres(); err != nil {
		slog.Error("failed to load PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer config.ClosePostgres()

	// Load Neo4j (optional — needed for metro path latency)
	if err := config.LoadNeo4j(); err != nil {
		slog.Warn("Neo4j not available", "error", err)
	} else {
		defer func() { _ = config.CloseNeo4j() }()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-shutdown
		slog.Info("received shutdown signal")
		cancel()
	}()

	cfg := worker.Config{
		Log: slog.Default(),
	}

	if err := worker.Start(ctx, cfg); err != nil {
		slog.Error("worker error", "error", err)
		os.Exit(1)
	}
}
