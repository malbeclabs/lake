package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/malbeclabs/lake/dev/controlcenter/internal/logs"
	"github.com/malbeclabs/lake/dev/controlcenter/internal/process"
	"github.com/malbeclabs/lake/dev/controlcenter/internal/scheduler"
	"github.com/malbeclabs/lake/dev/controlcenter/internal/server"
	"github.com/malbeclabs/lake/utils/pkg/logger"
)

func main() {
	port := flag.Int("port", 5174, "HTTP server port")
	bind := flag.String("bind", "127.0.0.1", "Bind address (127.0.0.1 or 0.0.0.0)")
	dataDir := flag.String("data-dir", "./controlcenter-data", "Data directory for logs and state")
	verbose := flag.Bool("verbose", false, "Enable debug logging")
	flag.Parse()

	bindExplicit := isFlagSet("bind")

	log := logger.New(*verbose)
	log.Info("starting control center", "version", "0.1.0")

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Error("failed to create data directory", "error", err)
		os.Exit(1)
	}

	logAgg, err := logs.NewAggregator(*dataDir, 10000, log)
	if err != nil {
		log.Error("failed to create log aggregator", "error", err)
		os.Exit(1)
	}
	defer logAgg.Close()

	manager := process.NewManager(*dataDir, logAgg, log)

	// Determine the lake root (parent of the controlcenter directory)
	cwd, err := os.Getwd()
	if err != nil {
		log.Error("failed to get working directory", "error", err)
		os.Exit(1)
	}
	lakeRoot := filepath.Dir(filepath.Dir(cwd))
	if filepath.Base(cwd) != "controlcenter" {
		lakeRoot = cwd
	}
	log.Info("lake root directory", "path", lakeRoot)

	config := loadConfig(*dataDir, log)

	// Use config's bind host if --bind was not explicitly passed
	if !bindExplicit && config.BindHost != "" {
		*bind = config.BindHost
	}

	// Web service always mirrors the control center bind address
	webEnv := []string{}
	if *bind == "0.0.0.0" {
		webEnv = []string{"VITE_HTTPS=1"}
	}

	manager.Register(process.ServiceConfig{
		Name:       "indexer",
		Command:    "go",
		Args:       []string{"run", "./indexer/cmd/indexer/", "--verbose", "--migrations-enable", "--mock-device-usage"},
		WorkingDir: lakeRoot,
		Env:        []string{},
	})

	manager.Register(process.ServiceConfig{
		Name:       "api",
		Command:    "go",
		Args:       []string{"run", "./api/main.go"},
		WorkingDir: lakeRoot,
		Env:        []string{},
	})

	manager.Register(process.ServiceConfig{
		Name:       "web",
		Command:    "bun",
		Args:       []string{"dev", "--host", *bind},
		WorkingDir: filepath.Join(lakeRoot, "web"),
		Env:        webEnv,
	})

	if err := manager.LoadState(); err != nil {
		log.Error("failed to load state", "error", err)
	}

	sched := scheduler.NewScheduler(manager, &config.Schedule, log)
	sched.Start()

	srv := server.NewServer(*bind, *port, lakeRoot, *dataDir, manager, logAgg, sched, config, log)

	go func() {
		log.Info("control center ready", "url", fmt.Sprintf("http://%s:%d", *bind, *port))
		if err := srv.Start(); err != nil {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info("received signal, shutting down", "signal", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	sched.Stop()

	log.Info("stopping all services")
	if err := manager.StopAll(); err != nil {
		log.Error("failed to stop all services", "error", err)
	}

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("failed to shutdown server", "error", err)
	}

	log.Info("control center stopped")
}

func isFlagSet(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func loadConfig(dataDir string, log *slog.Logger) *server.Config {
	configPath := filepath.Join(dataDir, "config.json")

	data, err := os.ReadFile(configPath)
	if err == nil {
		var config server.Config
		if err := json.Unmarshal(data, &config); err == nil {
			log.Info("loaded config from file", "path", configPath)
			return &config
		}
		log.Warn("failed to parse config file, using defaults", "error", err)
	}

	config := &server.Config{
		Port: 5174,
		Schedule: scheduler.Config{
			Enabled:   false,
			StartTime: "08:00",
			StopTime:  "23:00",
		},
		Services: map[string]string{},
	}

	data, err = json.MarshalIndent(config, "", "  ")
	if err == nil {
		if err := os.WriteFile(configPath, data, 0644); err != nil {
			log.Error("failed to write config file", "error", err)
		} else {
			log.Info("created default config", "path", configPath)
		}
	}

	return config
}
