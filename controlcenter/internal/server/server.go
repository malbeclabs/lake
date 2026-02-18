package server

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/malbeclabs/lake/controlcenter/internal/logs"
	"github.com/malbeclabs/lake/controlcenter/internal/process"
	"github.com/malbeclabs/lake/controlcenter/internal/scheduler"
)

//go:embed all:ui/dist
var uiFiles embed.FS

// Server is the HTTP server for the control center
type Server struct {
	router    *chi.Mux
	manager   *process.Manager
	logAgg    *logs.Aggregator
	scheduler *scheduler.Scheduler
	config    *Config
	lakeRoot  string
	dataDir   string
	logger    *slog.Logger
	srv       *http.Server
	mu        sync.RWMutex
}

// Config holds the server configuration
type Config struct {
	Port     int               `json:"port"`
	Schedule scheduler.Config  `json:"schedule"`
	Services map[string]string `json:"services"`
	BindHost string            `json:"bindHost,omitempty"` // control center bind host
}

// NewServer creates a new HTTP server
func NewServer(
	bind string,
	port int,
	lakeRoot string,
	dataDir string,
	manager *process.Manager,
	logAgg *logs.Aggregator,
	sched *scheduler.Scheduler,
	config *Config,
	logger *slog.Logger,
) *Server {
	s := &Server{
		router:    chi.NewRouter(),
		manager:   manager,
		logAgg:    logAgg,
		scheduler: sched,
		config:    config,
		lakeRoot:  lakeRoot,
		dataDir:   dataDir,
		logger:    logger,
	}

	s.setupRoutes()

	s.srv = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", bind, port),
		Handler:      s.router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	return s
}

// setupRoutes configures all HTTP routes
func (s *Server) setupRoutes() {
	s.router.Use(middleware.Logger)
	s.router.Use(middleware.Recoverer)
	s.router.Use(s.corsMiddleware)

	// API routes
	s.router.Route("/api", func(r chi.Router) {
		r.Get("/status", s.handleStatus)
		r.Post("/services/{name}/start", s.handleStartService)
		r.Post("/services/{name}/stop", s.handleStopService)
		r.Post("/services/start-all", s.handleStartAll)
		r.Post("/services/stop-all", s.handleStopAll)
		r.Get("/logs/stream", s.handleLogStream)
		r.Get("/logs/histogram", s.handleGetLogHistogram)
		r.Get("/logs", s.handleGetLogs)
		r.Get("/config", s.handleGetConfig)
		r.Put("/config", s.handleUpdateConfig)
		r.Get("/schedule/next", s.handleGetNextAction)
	})

	// Serve static UI files
	s.serveUI()
}

// corsMiddleware adds CORS headers, allowing only localhost origins.
// This prevents cross-site attacks when the server is bound to 0.0.0.0.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && isLocalhostOrigin(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		}

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// isLocalhostOrigin reports whether the given Origin header value is a localhost origin.
func isLocalhostOrigin(origin string) bool {
	return strings.HasPrefix(origin, "http://localhost") ||
		strings.HasPrefix(origin, "https://localhost") ||
		strings.HasPrefix(origin, "http://127.0.0.1") ||
		strings.HasPrefix(origin, "https://127.0.0.1")
}

// serveUI serves the static UI files with SPA fallback to index.html
func (s *Server) serveUI() {
	uiFS, err := fs.Sub(uiFiles, "ui/dist")
	if err != nil {
		s.logger.Error("failed to load UI files", "error", err)
		s.router.Get("/*", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Control Center - UI not built. Run 'cd ui && bun run build' to build the UI."))
		})
		return
	}

	fileServer := http.FileServer(http.FS(uiFS))
	s.router.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		// Check whether the requested path exists as an actual file in the embedded FS.
		// If not, serve index.html so the React SPA can handle client-side routing
		// (e.g. a hard refresh on /logs shouldn't return 404).
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "."
		}
		if _, err := uiFS.(fs.FS).Open(path); err != nil {
			r2 := r.Clone(r.Context())
			r2.URL = &url.URL{Path: "/"}
			fileServer.ServeHTTP(w, r2)
			return
		}
		fileServer.ServeHTTP(w, r)
	})
}

// handleStatus returns the status of all services
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	services := s.manager.GetAllServices()
	s.logger.Info("handleStatus called", "service_count", len(services))

	type serviceStatus struct {
		Name      string                `json:"name"`
		Status    process.ServiceStatus `json:"status"`
		PID       int                   `json:"pid"`
		StartedAt *time.Time            `json:"startedAt,omitempty"`
		StoppedAt *time.Time            `json:"stoppedAt,omitempty"`
		Error     string                `json:"error,omitempty"`
		Uptime    string                `json:"uptime,omitempty"`
	}

	response := make([]serviceStatus, 0, len(services))
	for _, svc := range services {
		status := serviceStatus{
			Name:      svc.Config.Name,
			Status:    svc.Status,
			PID:       svc.PID,
			StartedAt: svc.StartedAt,
			StoppedAt: svc.StoppedAt,
			Error:     svc.Error,
		}

		if svc.StartedAt != nil && svc.Status == process.StatusRunning {
			uptime := time.Since(*svc.StartedAt)
			status.Uptime = formatUptime(uptime)
		}

		response = append(response, status)
	}

	s.writeJSON(w, http.StatusOK, response)
}

// handleStartService starts a specific service
func (s *Server) handleStartService(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		s.writeError(w, http.StatusBadRequest, "service name is required")
		return
	}

	if err := s.manager.Start(name); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "started", "service": name})
}

// handleStopService stops a specific service
func (s *Server) handleStopService(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		s.writeError(w, http.StatusBadRequest, "service name is required")
		return
	}

	if err := s.manager.Stop(name); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "stopped", "service": name})
}

// handleStartAll starts all services
func (s *Server) handleStartAll(w http.ResponseWriter, r *http.Request) {
	if err := s.manager.StartAll(); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "all services started"})
}

// handleStopAll stops all services
func (s *Server) handleStopAll(w http.ResponseWriter, r *http.Request) {
	if err := s.manager.StopAll(); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "all services stopped"})
}

// handleLogStream streams logs via Server-Sent Events
func (s *Server) handleLogStream(w http.ResponseWriter, r *http.Request) {
	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	// Get filters from query params
	serviceFilter := r.URL.Query().Get("service")
	levelFilter := r.URL.Query().Get("level")

	// Subscribe to log stream
	ch := s.logAgg.Subscribe()
	defer s.logAgg.Unsubscribe(ch)

	// Stream logs
	for {
		select {
		case <-r.Context().Done():
			return
		case entry, ok := <-ch:
			if !ok {
				return
			}

			// Apply filters
			if serviceFilter != "" && serviceFilter != "all" && entry.Service != serviceFilter {
				continue
			}

			if levelFilter != "" && levelFilter != "all" && string(entry.Level) != levelFilter {
				continue
			}

			// Send log entry as SSE
			data, err := json.Marshal(entry)
			if err != nil {
				s.logger.Error("failed to marshal log entry", "error", err)
				continue
			}

			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// handleGetLogs returns recent logs with optional time range filtering
func (s *Server) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	if service == "" {
		service = "all"
	}

	limitStr := r.URL.Query().Get("limit")
	limit := 1000
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}
	if limit > 10000 {
		limit = 10000
	}

	levelFilter := r.URL.Query().Get("level")

	var from, to time.Time
	if fromStr := r.URL.Query().Get("from"); fromStr != "" {
		if t, err := time.Parse(time.RFC3339, fromStr); err == nil {
			from = t
		}
	}
	if toStr := r.URL.Query().Get("to"); toStr != "" {
		if t, err := time.Parse(time.RFC3339, toStr); err == nil {
			to = t
		}
	}

	logEntries := s.logAgg.GetFiltered(service, levelFilter, from, to, limit)

	s.writeJSON(w, http.StatusOK, logEntries)
}

// handleGetLogHistogram returns log counts per time bucket
func (s *Server) handleGetLogHistogram(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	if service == "" {
		service = "all"
	}

	level := r.URL.Query().Get("level")

	var from, to time.Time
	if fromStr := r.URL.Query().Get("from"); fromStr != "" {
		if t, err := time.Parse(time.RFC3339, fromStr); err == nil {
			from = t
		}
	}
	if toStr := r.URL.Query().Get("to"); toStr != "" {
		if t, err := time.Parse(time.RFC3339, toStr); err == nil {
			to = t
		}
	}

	intervalSeconds := 300 // default 5 minutes
	if ivStr := r.URL.Query().Get("interval"); ivStr != "" {
		if iv, err := strconv.Atoi(ivStr); err == nil {
			intervalSeconds = iv
		}
	}
	if intervalSeconds < 1 {
		intervalSeconds = 1
	}
	if intervalSeconds > 86400 {
		intervalSeconds = 86400
	}

	bucketDuration := time.Duration(intervalSeconds) * time.Second
	buckets := s.logAgg.GetHistogram(service, level, from, to, bucketDuration)

	s.writeJSON(w, http.StatusOK, buckets)
}

// handleGetConfig returns the current configuration
func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	cfg := *s.config
	s.mu.RUnlock()
	s.writeJSON(w, http.StatusOK, &cfg)
}

// saveConfig persists the current config to disk
func (s *Server) saveConfig() {
	configPath := filepath.Join(s.dataDir, "config.json")
	data, err := json.MarshalIndent(s.config, "", "  ")
	if err != nil {
		s.logger.Error("failed to marshal config", "error", err)
		return
	}
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		s.logger.Error("failed to write config", "error", err)
	}
}

// handleUpdateConfig updates the configuration
func (s *Server) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	var newConfig Config
	if err := json.NewDecoder(r.Body).Decode(&newConfig); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	// Validate BindHost
	if newConfig.BindHost != "" && newConfig.BindHost != "127.0.0.1" && newConfig.BindHost != "0.0.0.0" {
		s.writeError(w, http.StatusBadRequest, "bindHost must be '127.0.0.1' or '0.0.0.0'")
		return
	}

	s.mu.Lock()
	// Update scheduler config
	s.scheduler.UpdateConfig(&newConfig.Schedule)
	s.config.Schedule = newConfig.Schedule

	// Update bind host if provided
	if newConfig.BindHost != "" {
		s.config.BindHost = newConfig.BindHost
	}
	s.mu.Unlock()

	s.saveConfig()
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "config updated"})
}

// handleGetNextAction returns the next scheduled action
func (s *Server) handleGetNextAction(w http.ResponseWriter, r *http.Request) {
	message, nextTime, err := s.scheduler.GetNextAction()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := map[string]any{"message": message}
	if !nextTime.IsZero() {
		resp["nextTime"] = nextTime.UTC().Format(time.RFC3339)
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// Start starts the HTTP server
func (s *Server) Start() error {
	s.logger.Info("starting HTTP server", "addr", s.srv.Addr)
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down HTTP server")
	return s.srv.Shutdown(ctx)
}

// writeJSON writes a JSON response
func (s *Server) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("failed to encode JSON response", "error", err)
	}
}

// writeError writes an error response
func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]string{"error": message})
}

// formatUptime formats a duration as uptime
func formatUptime(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}
