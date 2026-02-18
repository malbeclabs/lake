package process

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/malbeclabs/lake/controlcenter/internal/logs"
)

// ServiceStatus represents the current state of a service
type ServiceStatus string

const (
	StatusStopped  ServiceStatus = "stopped"
	StatusStarting ServiceStatus = "starting"
	StatusRunning  ServiceStatus = "running"
	StatusFailed   ServiceStatus = "failed"
)

// ServiceConfig defines how to run a service
type ServiceConfig struct {
	Name       string   `json:"name"`
	Command    string   `json:"command"`
	Args       []string `json:"args"`
	WorkingDir string   `json:"workingDir"`
	Env        []string `json:"env"`
}

// Service represents a managed service process
type Service struct {
	Config    ServiceConfig `json:"config"`
	PID       int           `json:"pid"`
	Status    ServiceStatus `json:"status"`
	StartedAt *time.Time    `json:"startedAt,omitempty"`
	StoppedAt *time.Time    `json:"stoppedAt,omitempty"`
	Error     string        `json:"error,omitempty"`

	cmd    *exec.Cmd
	ctx    context.Context
	cancel context.CancelFunc
	exitCh chan struct{} // closed when the process exits
	mu     sync.RWMutex
}

// Manager manages multiple service processes
type Manager struct {
	services map[string]*Service
	logAgg   *logs.Aggregator
	dataDir  string
	logger   *slog.Logger
	mu       sync.RWMutex
}

// NewManager creates a new process manager
func NewManager(dataDir string, logAgg *logs.Aggregator, logger *slog.Logger) *Manager {
	return &Manager{
		services: make(map[string]*Service),
		logAgg:   logAgg,
		dataDir:  dataDir,
		logger:   logger,
	}
}

// UpdateConfig updates the ServiceConfig for an existing service without affecting its running state.
// The new config takes effect on the next Start.
func (m *Manager) UpdateConfig(config ServiceConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	svc, exists := m.services[config.Name]
	if !exists {
		return
	}
	svc.mu.Lock()
	svc.Config = config
	svc.mu.Unlock()
}

// Register adds a service configuration to the manager
func (m *Manager) Register(config ServiceConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.services[config.Name] = &Service{
		Config: config,
		Status: StatusStopped,
	}
	m.logger.Info("registered service", "name", config.Name, "total_services", len(m.services))
}

// Start starts a service by name
func (m *Manager) Start(name string) error {
	m.mu.Lock()
	svc, exists := m.services[name]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("service %q not found", name)
	}
	m.mu.Unlock()

	svc.mu.Lock()

	if svc.Status == StatusRunning {
		svc.mu.Unlock()
		return fmt.Errorf("service %q is already running", name)
	}

	m.logger.Info("starting service", "name", name, "command", svc.Config.Command, "args", svc.Config.Args)

	ctx, cancel := context.WithCancel(context.Background())
	svc.ctx = ctx
	svc.cancel = cancel

	cmd := exec.CommandContext(ctx, svc.Config.Command, svc.Config.Args...)
	cmd.Dir = svc.Config.WorkingDir
	cmd.Env = append(os.Environ(), svc.Config.Env...)

	// Create pipes for stdout and stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		svc.mu.Unlock()
		cancel()
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		svc.mu.Unlock()
		cancel()
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		stdout.Close()
		stderr.Close()
		cancel()
		svc.Status = StatusFailed
		svc.Error = err.Error()
		svc.mu.Unlock()
		return fmt.Errorf("failed to start command: %w", err)
	}

	svc.cmd = cmd
	svc.PID = cmd.Process.Pid
	svc.Status = StatusRunning
	now := time.Now()
	svc.StartedAt = &now
	svc.Error = ""
	svc.exitCh = make(chan struct{})

	m.logger.Info("service started", "name", name, "pid", svc.PID)

	// Release the lock before launching goroutines
	svc.mu.Unlock()

	// Stream output to log aggregator
	go m.streamOutput(name, "stdout", stdout)
	go m.streamOutput(name, "stderr", stderr)

	// Wait for process to exit
	go func() {
		err := cmd.Wait()

		svc.mu.Lock()
		now := time.Now()
		svc.StoppedAt = &now

		if err != nil {
			if ctx.Err() == context.Canceled {
				// Process was stopped intentionally
				svc.Status = StatusStopped
				m.logger.Info("service stopped", "name", name)
			} else {
				// Process crashed or exited with error
				svc.Status = StatusFailed
				svc.Error = err.Error()
				m.logger.Error("service failed", "name", name, "error", err)
			}
		} else {
			// Process exited cleanly
			svc.Status = StatusStopped
			m.logger.Info("service exited", "name", name)
		}

		svc.PID = 0
		svc.mu.Unlock()

		// Signal exit after releasing lock so Stop() can proceed
		close(svc.exitCh)
		m.SaveState()
	}()

	m.SaveState()
	return nil
}

// streamOutput reads from a pipe and sends each line to the log aggregator
func (m *Manager) streamOutput(serviceName, stream string, reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		entry := logs.LogEntry{
			Timestamp: time.Now(),
			Service:   serviceName,
			Stream:    stream,
			Line:      line,
		}
		m.logAgg.Append(entry)
	}
	if err := scanner.Err(); err != nil {
		m.logger.Error("error reading output", "service", serviceName, "stream", stream, "error", err)
	}
}

// Stop stops a service by name
func (m *Manager) Stop(name string) error {
	m.mu.RLock()
	svc, exists := m.services[name]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("service %q not found", name)
	}

	svc.mu.Lock()

	if svc.Status != StatusRunning {
		svc.mu.Unlock()
		return fmt.Errorf("service %q is not running", name)
	}

	m.logger.Info("stopping service", "name", name, "pid", svc.PID)

	exitCh := svc.exitCh

	// Cancel context to signal termination
	if svc.cancel != nil {
		svc.cancel()
	}

	// Send SIGTERM for graceful shutdown
	if svc.cmd != nil && svc.cmd.Process != nil {
		if err := svc.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			m.logger.Error("failed to send SIGTERM", "name", name, "error", err)
		}
	}

	// Release lock before waiting - the Start goroutine needs it to update status
	svc.mu.Unlock()

	// Wait up to 10 seconds for graceful shutdown
	select {
	case <-exitCh:
		m.logger.Info("service stopped gracefully", "name", name)
	case <-time.After(10 * time.Second):
		// Force kill after timeout
		m.logger.Warn("service did not stop gracefully, sending SIGKILL", "name", name)
		svc.mu.Lock()
		if svc.cmd != nil && svc.cmd.Process != nil {
			if err := svc.cmd.Process.Kill(); err != nil {
				m.logger.Error("failed to kill process", "name", name, "error", err)
			}
		}
		svc.mu.Unlock()
		<-exitCh
	}

	return nil
}

// Status returns the status of a service
func (m *Manager) Status(name string) (ServiceStatus, error) {
	m.mu.RLock()
	svc, exists := m.services[name]
	m.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("service %q not found", name)
	}

	svc.mu.RLock()
	defer svc.mu.RUnlock()
	return svc.Status, nil
}

// GetService returns a copy of a service's state
func (m *Manager) GetService(name string) (*Service, error) {
	m.mu.RLock()
	svc, exists := m.services[name]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("service %q not found", name)
	}

	svc.mu.RLock()
	defer svc.mu.RUnlock()

	// Return a copy to avoid race conditions
	copy := *svc
	copy.cmd = nil
	copy.ctx = nil
	copy.cancel = nil
	return &copy, nil
}

// GetAllServices returns a map of all service states
func (m *Manager) GetAllServices() map[string]*Service {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*Service)
	for name, svc := range m.services {
		svc.mu.RLock()
		copy := *svc
		copy.cmd = nil
		copy.ctx = nil
		copy.cancel = nil
		svc.mu.RUnlock()
		result[name] = &copy
	}
	return result
}

// StartAll starts all registered services
func (m *Manager) StartAll() error {
	m.mu.RLock()
	names := make([]string, 0, len(m.services))
	for name := range m.services {
		names = append(names, name)
	}
	m.mu.RUnlock()

	var errors []error
	for _, name := range names {
		if err := m.Start(name); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to start some services: %v", errors)
	}
	return nil
}

// StopAll stops all running services
func (m *Manager) StopAll() error {
	m.mu.RLock()
	names := make([]string, 0, len(m.services))
	for name := range m.services {
		names = append(names, name)
	}
	m.mu.RUnlock()

	var errors []error
	for _, name := range names {
		if err := m.Stop(name); err != nil {
			// Ignore "not running" errors
			if svc, _ := m.GetService(name); svc != nil && svc.Status != StatusRunning {
				continue
			}
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to stop some services: %v", errors)
	}
	return nil
}

// SaveState persists the current process state to disk
func (m *Manager) SaveState() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	statePath := filepath.Join(m.dataDir, "processes.json")

	// Collect service states
	states := make(map[string]*Service)
	for name, svc := range m.services {
		svc.mu.RLock()
		copy := *svc
		copy.cmd = nil
		copy.ctx = nil
		copy.cancel = nil
		svc.mu.RUnlock()
		states[name] = &copy
	}

	data, err := json.MarshalIndent(states, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(statePath, data, 0600); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}

// LoadState restores process state from disk
func (m *Manager) LoadState() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	statePath := filepath.Join(m.dataDir, "processes.json")

	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			// No state file yet, not an error
			return nil
		}
		return fmt.Errorf("failed to read state file: %w", err)
	}

	var states map[string]*Service
	if err := json.Unmarshal(data, &states); err != nil {
		return fmt.Errorf("failed to unmarshal state: %w", err)
	}

	// Update service states (but don't try to reconnect to old PIDs)
	for name, savedState := range states {
		if svc, exists := m.services[name]; exists {
			svc.mu.Lock()
			// Only restore if saved state was running (PIDs are stale though)
			if savedState.Status == StatusRunning {
				svc.Status = StatusStopped // Mark as stopped since we can't reconnect
				m.logger.Info("found previously running service", "name", name, "note", "marked as stopped")
			}
			svc.mu.Unlock()
		}
	}

	return nil
}
