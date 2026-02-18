package scheduler

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/malbeclabs/lake/dev/controlcenter/internal/process"
)

// Config holds the scheduler configuration
type Config struct {
	Enabled   bool   `json:"enabled"`
	StartTime string `json:"startTime"` // Format: "HH:MM"
	StopTime  string `json:"stopTime"`  // Format: "HH:MM"
}

// Scheduler handles automatic start/stop of services
type Scheduler struct {
	manager *process.Manager
	config  *Config
	stopCh  chan struct{}
	logger  *slog.Logger
	mu      sync.RWMutex
}

// NewScheduler creates a new scheduler
func NewScheduler(manager *process.Manager, config *Config, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		manager: manager,
		config:  config,
		stopCh:  make(chan struct{}),
		logger:  logger,
	}
}

// Start begins the scheduler loop
func (s *Scheduler) Start() {
	go s.run()
	s.mu.RLock()
	enabled := s.config.Enabled
	s.mu.RUnlock()
	s.logger.Info("scheduler started", "enabled", enabled)
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	close(s.stopCh)
	s.logger.Info("scheduler stopped")
}

// UpdateConfig updates the scheduler configuration
func (s *Scheduler) UpdateConfig(config *Config) {
	s.mu.Lock()
	s.config = config
	s.mu.Unlock()
	s.logger.Info("scheduler config updated", "enabled", config.Enabled, "start", config.StartTime, "stop", config.StopTime)
}

// run is the main scheduler loop
func (s *Scheduler) run() {
	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	// Track the last times we triggered start/stop to avoid re-firing within the same cycle.
	// Using the zero time means we'll fire immediately if we're already past the scheduled time.
	var lastStarted, lastStopped time.Time

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.mu.RLock()
			cfg := *s.config
			s.mu.RUnlock()

			if !cfg.Enabled {
				continue
			}

			now := time.Now()
			nextStart, err := s.parseTime(cfg.StartTime)
			if err != nil {
				s.logger.Error("invalid start time", "value", cfg.StartTime, "error", err)
				continue
			}

			nextStop, err := s.parseTime(cfg.StopTime)
			if err != nil {
				s.logger.Error("invalid stop time", "value", cfg.StopTime, "error", err)
				continue
			}

			// Fire start if we're past the scheduled time and haven't already fired for this cycle.
			if !now.Before(nextStart) && lastStarted.Before(nextStart) {
				lastStarted = now
				if s.shouldStart() {
					s.logger.Info("scheduled start triggered", "at", now.Format("15:04"))
					if err := s.manager.StartAll(); err != nil {
						s.logger.Error("failed to start services", "error", err)
					}
				}
			}

			// Fire stop if we're past the scheduled time and haven't already fired for this cycle.
			if !now.Before(nextStop) && lastStopped.Before(nextStop) {
				lastStopped = now
				if s.shouldStop() {
					s.logger.Info("scheduled stop triggered", "at", now.Format("15:04"))
					if err := s.manager.StopAll(); err != nil {
						s.logger.Error("failed to stop services", "error", err)
					}
				}
			}
		}
	}
}

// shouldStart returns true if any service needs to be started
func (s *Scheduler) shouldStart() bool {
	services := s.manager.GetAllServices()
	for _, svc := range services {
		if svc.Status == process.StatusStopped || svc.Status == process.StatusFailed {
			return true
		}
	}
	return false
}

// shouldStop returns true if any service needs to be stopped
func (s *Scheduler) shouldStop() bool {
	services := s.manager.GetAllServices()
	for _, svc := range services {
		if svc.Status == process.StatusRunning {
			return true
		}
	}
	return false
}

// parseTime parses a time string in HH:MM format and returns today's time at that hour
func (s *Scheduler) parseTime(timeStr string) (time.Time, error) {
	parts := strings.Split(timeStr, ":")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("invalid time format: %s (expected HH:MM)", timeStr)
	}

	hour, err := strconv.Atoi(parts[0])
	if err != nil || hour < 0 || hour > 23 {
		return time.Time{}, fmt.Errorf("invalid hour: %s", parts[0])
	}

	minute, err := strconv.Atoi(parts[1])
	if err != nil || minute < 0 || minute > 59 {
		return time.Time{}, fmt.Errorf("invalid minute: %s", parts[1])
	}

	now := time.Now()
	scheduled := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, now.Location())

	return scheduled, nil
}

// GetNextAction returns a description of the next scheduled action and the time it will occur.
func (s *Scheduler) GetNextAction() (string, time.Time, error) {
	s.mu.RLock()
	cfg := *s.config
	s.mu.RUnlock()

	if !cfg.Enabled {
		return "Scheduling disabled", time.Time{}, nil
	}

	now := time.Now()
	startTime, err := s.parseTime(cfg.StartTime)
	if err != nil {
		return "", time.Time{}, err
	}

	stopTime, err := s.parseTime(cfg.StopTime)
	if err != nil {
		return "", time.Time{}, err
	}

	// If start time is in the past today, move it to tomorrow
	if startTime.Before(now) {
		startTime = startTime.Add(24 * time.Hour)
	}

	// If stop time is in the past today, move it to tomorrow
	if stopTime.Before(now) {
		stopTime = stopTime.Add(24 * time.Hour)
	}

	// Determine which comes first
	if startTime.Before(stopTime) {
		duration := startTime.Sub(now)
		return fmt.Sprintf("Start services in %s", formatDuration(duration)), startTime, nil
	}

	duration := stopTime.Sub(now)
	return fmt.Sprintf("Stop services in %s", formatDuration(duration)), stopTime, nil
}

// formatDuration formats a duration in a human-readable way
func formatDuration(d time.Duration) string {
	if d < 0 {
		return "now"
	}

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}
