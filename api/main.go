package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/getsentry/sentry-go"
	sentryhttp "github.com/getsentry/sentry-go/http"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/joho/godotenv"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	"github.com/malbeclabs/lake/api/metrics"
	slackbot "github.com/malbeclabs/lake/slack/bot"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/slack-go/slack/socketmode"
)

var (
	// Set by LDFLAGS
	version = "dev"
	commit  = "none"
	date    = "unknown"

	// shuttingDown is set to true when shutdown signal is received.
	// Readiness probe checks this to immediately return 503.
	shuttingDown atomic.Bool

	// dbHealthy tracks ClickHouse connectivity in the background so the
	// readiness probe never blocks on a synchronous ping.
	dbHealthy   atomic.Bool
	dbHealthErr atomic.Value // stores string
)

const (
	defaultMetricsAddr = "0.0.0.0:0"
)

// spaHandler serves static files and falls back to index.html for SPA routing.
// If assetBucketURL is set, missing assets are fetched from the bucket and cached locally.
func spaHandler(staticDir, assetBucketURL string) http.HandlerFunc {
	fileServer := http.FileServer(http.Dir(staticDir))

	// Static asset extensions that should 404 if missing (not fallback to index.html)
	staticExtensions := map[string]bool{
		".js": true, ".mjs": true, ".css": true, ".map": true,
		".woff": true, ".woff2": true, ".ttf": true, ".eot": true,
		".png": true, ".jpg": true, ".jpeg": true, ".gif": true, ".svg": true, ".ico": true, ".webp": true,
		".json": true, ".wasm": true,
	}

	// setNoCacheHeaders prevents browsers from caching the response
	setNoCacheHeaders := func(w http.ResponseWriter) {
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
	}

	// setLongCacheHeaders allows browsers to cache content-hashed assets indefinitely
	setLongCacheHeaders := func(w http.ResponseWriter) {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}

	// Asset cache directory for assets fetched from S3
	cacheDir := filepath.Join(os.TempDir(), "lake-asset-cache")
	if assetBucketURL != "" {
		if err := os.MkdirAll(cacheDir, 0755); err != nil {
			slog.Warn("failed to create asset cache dir", "error", err)
		}
	}

	// Track in-flight fetches to avoid duplicate requests for the same asset
	var fetchMu sync.Mutex
	fetching := make(map[string]chan struct{})

	// fetchFromBucket fetches an asset from S3 and caches it locally.
	// Returns the local cache path on success, empty string on failure.
	fetchFromBucket := func(assetName string) string {
		if assetBucketURL == "" {
			return ""
		}

		cachePath := filepath.Join(cacheDir, assetName)

		// Check if already cached
		if _, err := os.Stat(cachePath); err == nil {
			return cachePath
		}

		// Coordinate concurrent fetches for the same asset
		fetchMu.Lock()
		if ch, ok := fetching[assetName]; ok {
			fetchMu.Unlock()
			<-ch // Wait for in-flight fetch
			if _, err := os.Stat(cachePath); err == nil {
				return cachePath
			}
			return ""
		}
		ch := make(chan struct{})
		fetching[assetName] = ch
		fetchMu.Unlock()

		defer func() {
			fetchMu.Lock()
			delete(fetching, assetName)
			close(ch)
			fetchMu.Unlock()
		}()

		// Fetch from S3
		url := strings.TrimSuffix(assetBucketURL, "/") + "/" + assetName
		resp, err := http.Get(url)
		if err != nil {
			slog.Error("failed to fetch asset from bucket", "error", err)
			return ""
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return ""
		}

		// Write to cache
		if err := os.MkdirAll(filepath.Dir(cachePath), 0755); err != nil {
			slog.Error("failed to create cache subdir", "error", err)
			return ""
		}

		f, err := os.Create(cachePath)
		if err != nil {
			slog.Error("failed to create cache file", "error", err)
			return ""
		}
		defer f.Close()

		if _, err := io.Copy(f, resp.Body); err != nil {
			slog.Error("failed to write cache file", "error", err)
			os.Remove(cachePath)
			return ""
		}

		slog.Info("cached asset from bucket", "asset", assetName)
		return cachePath
	}

	return func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Join(staticDir, strings.TrimPrefix(r.URL.Path, "/"))

		// Check if file exists locally
		_, err := os.Stat(path)
		if os.IsNotExist(err) || err != nil {
			// Check if it's a directory (and serve index.html from it or fallback)
			if fi, statErr := os.Stat(path); statErr == nil && fi.IsDir() {
				indexPath := filepath.Join(path, "index.html")
				if _, indexErr := os.Stat(indexPath); indexErr == nil {
					setNoCacheHeaders(w)
					http.ServeFile(w, r, indexPath)
					return
				}
			}

			// For static assets, try fetching from S3 bucket if configured
			ext := strings.ToLower(filepath.Ext(r.URL.Path))
			if staticExtensions[ext] {
				// Extract asset name (e.g., "assets/index-abc123.js" from "/assets/index-abc123.js")
				assetName := strings.TrimPrefix(r.URL.Path, "/assets/")
				if cachePath := fetchFromBucket(assetName); cachePath != "" {
					// Serve from cache with appropriate content type and long cache headers
					setLongCacheHeaders(w)
					contentType := mime.TypeByExtension(ext)
					if contentType != "" {
						w.Header().Set("Content-Type", contentType)
					}
					http.ServeFile(w, r, cachePath)
					return
				}

				// Not in bucket either, return 404
				setNoCacheHeaders(w)
				http.NotFound(w, r)
				return
			}

			// Fallback to root index.html for SPA routing
			setNoCacheHeaders(w)
			http.ServeFile(w, r, filepath.Join(staticDir, "index.html"))
			return
		}

		// Direct request to index.html - never cache
		if strings.HasSuffix(r.URL.Path, "index.html") {
			setNoCacheHeaders(w)
		}

		fileServer.ServeHTTP(w, r)
	}
}

func main() {
	metricsAddrFlag := flag.String("metrics-addr", defaultMetricsAddr, "Address to listen on for prometheus metrics")
	useRemoteFlag := flag.Bool("use-remote", false, "Use remote proxy database (e.g., lake_remote) instead of local data")
	flag.Parse()

	// Set env var so config.Load() picks it up (flag takes precedence over env)
	if *useRemoteFlag {
		os.Setenv("CLICKHOUSE_USE_REMOTE", "true")
	}

	slog.Info("starting lake-api", "version", version, "commit", commit, "date", date)
	handlers.SetBuildInfo(version, commit, date)

	// Load .env files if they exist
	// godotenv doesn't override existing env vars, so later files don't overwrite earlier ones
	_ = godotenv.Load()           // .env in current working directory
	_ = godotenv.Load("api/.env") // api/.env when running from repo root

	// Initialize Sentry for error tracking (optional - gracefully no-op if DSN not set)
	sentryDSN := os.Getenv("SENTRY_DSN")
	if sentryDSN != "" {
		sentryEnv := os.Getenv("SENTRY_ENVIRONMENT")
		if sentryEnv == "" {
			sentryEnv = "development"
		}
		release := version
		if commit != "none" {
			release = version + "-" + commit
		}
		// TracesSampleRate: 1.0 for development, 0.1 (10%) otherwise
		tracesSampleRate := 0.1
		if sentryEnv == "development" {
			tracesSampleRate = 1.0
		}
		err := sentry.Init(sentry.ClientOptions{
			Dsn:              sentryDSN,
			Environment:      sentryEnv,
			Release:          release,
			EnableTracing:    true,
			TracesSampleRate: tracesSampleRate,
		})
		if err != nil {
			slog.Warn("Sentry initialization failed", "error", err)
		} else {
			slog.Info("Sentry initialized", "env", sentryEnv, "release", release)
			defer sentry.Flush(2 * time.Second)
		}
	}

	// Load configuration
	if err := config.Load(); err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Load PostgreSQL
	if err := config.LoadPostgres(); err != nil {
		slog.Error("failed to load PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer config.ClosePostgres()
	defer config.Close() // Close ClickHouse connection

	// Start background DB health checker so the readiness probe never
	// blocks on a synchronous ClickHouse ping.
	dbHealthy.Store(true) // assume healthy until first check
	dbHealthCtx, dbHealthCancel := context.WithCancel(context.Background())
	defer dbHealthCancel()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-dbHealthCtx.Done():
				return
			case <-ticker.C:
				pingCtx, pingCancel := context.WithTimeout(dbHealthCtx, 3*time.Second)
				if err := config.HealthDB.Ping(pingCtx); err != nil {
					dbHealthy.Store(false)
					dbHealthErr.Store(err.Error())
				} else {
					dbHealthy.Store(true)
					dbHealthErr.Store("")
				}
				pingCancel()
			}
		}
	}()

	// Start background ClickHouse connection pool stats collector.
	go func() {
		pools := map[string]driver.Conn{
			"main":   config.DB,
			"health": config.HealthDB,
		}
		for env, conn := range config.EnvDBs {
			if env == "mainnet-beta" {
				continue // same as "main"
			}
			pools[env] = conn
		}

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-dbHealthCtx.Done():
				return
			case <-ticker.C:
				metrics.CollectClickHousePoolStats(pools)
			}
		}
	}()

	// Load Neo4j (optional - log warning if unavailable)
	if err := config.LoadNeo4j(); err != nil {
		slog.Warn("Neo4j not available", "error", err)
	} else {
		defer func() { _ = config.CloseNeo4j() }()
	}

	// Initialize page cache for fast page loads
	handlers.InitPageCache()
	// Note: StopPageCache() is called explicitly before server shutdown, not deferred

	// Start metrics server
	var metricsServer *http.Server
	if *metricsAddrFlag != "" {
		metrics.BuildInfo.WithLabelValues(version, commit, date).Set(1)
		listener, err := net.Listen("tcp", *metricsAddrFlag)
		if err != nil {
			slog.Error("failed to start Prometheus metrics server listener", "error", err)
		} else {
			slog.Info("Prometheus metrics server listening", "addr", listener.Addr().String())
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			metricsServer = &http.Server{Handler: mux}
			go func() {
				if err := metricsServer.Serve(listener); err != nil && err != http.ErrServerClosed {
					slog.Error("metrics server error", "error", err)
				}
			}()
		}
	}

	r := chi.NewRouter()

	r.Use(middleware.Logger)

	// Sentry middleware for error and performance monitoring (before Recoverer to capture panics)
	if sentryDSN != "" {
		sentryHandler := sentryhttp.New(sentryhttp.Options{
			Repanic: true, // Re-panic after capturing so Recoverer can handle it
		})
		r.Use(sentryHandler.Handle)

		// Set transaction name from Chi route pattern
		r.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if txn := sentry.TransactionFromContext(r.Context()); txn != nil {
					// Try to get route pattern - may or may not be available depending on timing
					if rctx := chi.RouteContext(r.Context()); rctx != nil {
						if pattern := rctx.RoutePattern(); pattern != "" {
							txn.Name = r.Method + " " + pattern
						} else {
							// Fallback to URL path if route pattern not yet available
							txn.Name = r.Method + " " + r.URL.Path
						}
					}
				}
				next.ServeHTTP(w, r)
			})
		})
	}

	r.Use(middleware.Recoverer)
	r.Use(metrics.Middleware)

	// CORS configuration - origins from env or allow all
	corsOrigins := []string{"*"}
	if origins := os.Getenv("CORS_ORIGINS"); origins != "" {
		corsOrigins = strings.Split(origins, ",")
	}
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   corsOrigins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization", "X-DZ-Env"},
		ExposedHeaders:   []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"},
		AllowCredentials: false,
		MaxAge:           300,
	}))

	// Security headers middleware
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Content Security Policy for Google Sign-In and app resources
			csp := strings.Join([]string{
				"default-src 'self'",
				"script-src 'self' 'unsafe-inline' https://accounts.google.com https://static.cloudflareinsights.com",
				"worker-src 'self' blob:",
				"frame-src https://accounts.google.com https://accounts.googleusercontent.com",
				"connect-src 'self' https://accounts.google.com https://cloudflareinsights.com https://*.basemaps.cartocdn.com https://*.ingest.us.sentry.io",
				"style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://accounts.google.com",
				"font-src 'self' https://fonts.gstatic.com",
				"img-src 'self' data: blob: https://lh3.googleusercontent.com https://*.basemaps.cartocdn.com",
			}, "; ")
			w.Header().Set("Content-Security-Policy", csp)

			// Additional security headers
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("X-Frame-Options", "DENY")
			w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")

			next.ServeHTTP(w, r)
		})
	})

	// Apply optional auth middleware globally to attach user context
	r.Use(handlers.OptionalAuth)

	// Apply env middleware to extract X-DZ-Env header
	r.Use(handlers.EnvMiddleware)

	// Health check endpoints
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if shuttingDown.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("shutting down"))
			return
		}

		if !dbHealthy.Load() {
			errMsg, _ := dbHealthErr.Load().(string)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("database connection failed: " + errMsg))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Lightweight endpoints (no rate limiting)
	r.Get("/api/config", handlers.GetConfig)
	r.Get("/api/version", handlers.GetVersion)

	// Database query endpoints (rate limited)
	r.Group(func(r chi.Router) {
		r.Use(handlers.QueryRateLimitMiddleware)

		r.Get("/api/catalog", handlers.GetCatalog)
		r.Get("/api/stats", handlers.GetStats)
		r.Get("/api/status", handlers.GetStatus)
		r.Get("/api/status/link-history", handlers.GetLinkHistory)
		r.Get("/api/status/device-history", handlers.GetDeviceHistory)
		r.Get("/api/status/interface-issues", handlers.GetInterfaceIssues)
		r.Get("/api/status/devices/{pk}/interface-history", handlers.GetDeviceInterfaceHistory)
		r.Get("/api/status/devices/{pk}/history", handlers.GetSingleDeviceHistory)
		r.Get("/api/status/links/{pk}/history", handlers.GetSingleLinkHistory)
		r.Get("/api/timeline", handlers.GetTimeline)
		r.Get("/api/timeline/bounds", handlers.GetTimelineBounds)

		// Incident routes
		r.Get("/api/incidents/links", handlers.GetLinkIncidents)
		r.Get("/api/incidents/links/csv", handlers.GetLinkIncidentsCSV)
		r.Get("/api/incidents/devices", handlers.GetDeviceIncidents)
		r.Get("/api/incidents/devices/csv", handlers.GetDeviceIncidentsCSV)

		// Search routes
		r.Get("/api/search", handlers.Search)
		r.Get("/api/search/autocomplete", handlers.SearchAutocomplete)

		// DZ entity routes
		r.Get("/api/dz/devices", handlers.GetDevices)
		r.Get("/api/dz/devices/{pk}", handlers.GetDevice)
		r.Get("/api/dz/links", handlers.GetLinks)
		r.Get("/api/dz/links/{pk}", handlers.GetLink)
		r.Get("/api/dz/links-health", handlers.GetLinkHealth)
		r.Get("/api/dz/metros", handlers.GetMetros)
		r.Get("/api/dz/metros/{pk}", handlers.GetMetro)
		r.Get("/api/dz/contributors", handlers.GetContributors)
		r.Get("/api/dz/contributors/{pk}", handlers.GetContributor)
		r.Get("/api/dz/users", handlers.GetUsers)
		r.Get("/api/dz/users/{pk}", handlers.GetUser)
		r.Get("/api/dz/users/{pk}/traffic", handlers.GetUserTraffic)
		r.Get("/api/dz/users/{pk}/multicast-groups", handlers.GetUserMulticastGroups)
		r.Get("/api/dz/multicast-groups", handlers.GetMulticastGroups)
		r.Get("/api/dz/multicast-groups/{pk}", handlers.GetMulticastGroup)
		r.Get("/api/dz/multicast-groups/{pk}/members", handlers.GetMulticastGroupMembers)
		r.Get("/api/dz/multicast-groups/{pk}/tree-paths", handlers.GetMulticastTreePaths)
		r.Get("/api/dz/multicast-groups/{pk}/tree-segments", handlers.GetMulticastTreeSegments)
		r.Get("/api/dz/multicast-groups/{pk}/traffic", handlers.GetMulticastGroupTraffic)
		r.Get("/api/dz/multicast-groups/{pk}/member-counts", handlers.GetMulticastGroupMemberCounts)
		r.Get("/api/dz/publisher-check", handlers.GetPublisherCheck)
		r.With(handlers.RequireInternalDomain).Get("/api/dz/edge/scoreboard", handlers.GetEdgeScoreboard)
		r.Get("/api/dz/tenants", handlers.GetTenants)
		r.Get("/api/dz/tenants/{pk}", handlers.GetTenant)
		r.Get("/api/dz/field-values", handlers.GetFieldValues)
		r.Get("/api/dz/ledger", handlers.GetDZLedger)

		// Solana entity routes
		r.Get("/api/solana/validators", handlers.GetValidators)
		r.Get("/api/solana/validators/{vote_pubkey}", handlers.GetValidator)
		r.Get("/api/solana/gossip-nodes", handlers.GetGossipNodes)
		r.Get("/api/solana/gossip-nodes/{pubkey}", handlers.GetGossipNode)
		r.Get("/api/solana/ledger", handlers.GetSolanaLedger)
		r.Get("/api/solana/validator-performance", handlers.GetValidatorPerformance)

		// Stake analytics routes
		r.Get("/api/stake/overview", handlers.GetStakeOverview)
		r.Get("/api/stake/history", handlers.GetStakeHistory)
		r.Get("/api/stake/changes", handlers.GetStakeChanges)
		r.Get("/api/stake/validators", handlers.GetStakeValidators)

		// Traffic analytics routes
		r.Get("/api/traffic/data", handlers.GetTrafficData)
		r.Get("/api/traffic/discards", handlers.GetDiscardsData)

		// Traffic dashboard routes
		r.Get("/api/traffic/dashboard/stress", handlers.GetTrafficDashboardStress)
		r.Get("/api/traffic/dashboard/top", handlers.GetTrafficDashboardTop)
		r.Get("/api/traffic/dashboard/drilldown", handlers.GetTrafficDashboardDrilldown)
		r.Get("/api/traffic/dashboard/burstiness", handlers.GetTrafficDashboardBurstiness)
		r.Get("/api/traffic/dashboard/health", handlers.GetTrafficDashboardHealth)

		// Topology endpoints (ClickHouse only)
		r.Get("/api/topology", handlers.GetTopology)
		r.Get("/api/traffic/entity", handlers.GetEntityTraffic)
		r.Get("/api/topology/link-latency", handlers.GetLinkLatencyHistory)
		r.Get("/api/topology/latency-comparison", handlers.GetLatencyComparison)
		r.Get("/api/topology/latency-history/{origin}/{target}", handlers.GetLatencyHistory)

		// Topology endpoints (require Neo4j — mainnet only)
		r.Group(func(r chi.Router) {
			r.Use(handlers.RequireNeo4jMiddleware)
			r.Get("/api/topology/isis", handlers.GetISISTopology)
			r.Get("/api/topology/path", handlers.GetISISPath)
			r.Get("/api/topology/paths", handlers.GetISISPaths)
			r.Get("/api/topology/compare", handlers.GetTopologyCompare)
			r.Get("/api/topology/impact/{pk}", handlers.GetFailureImpact)
			r.Get("/api/topology/critical-links", handlers.GetCriticalLinks)
			r.Get("/api/topology/redundancy-report", handlers.GetRedundancyReport)
			r.Get("/api/topology/simulate-link-removal", handlers.GetSimulateLinkRemoval)
			r.Get("/api/topology/simulate-link-addition", handlers.GetSimulateLinkAddition)
			r.Get("/api/topology/metro-connectivity", handlers.GetMetroConnectivity)
			r.Get("/api/topology/metro-path-latency", handlers.GetMetroPathLatency)
			r.Get("/api/topology/metro-path-detail", handlers.GetMetroPathDetail)
			r.Get("/api/topology/metro-paths", handlers.GetMetroPaths)
			r.Get("/api/topology/metro-device-paths", handlers.GetMetroDevicePaths)
			r.Post("/api/topology/maintenance-impact", handlers.PostMaintenanceImpact)
			r.Post("/api/topology/whatif-removal", handlers.PostWhatIfRemoval)
		})

		// SQL endpoints
		r.Post("/api/sql/query", handlers.ExecuteQuery)
		r.Post("/api/sql/generate", handlers.GenerateSQL)
		r.Post("/api/sql/generate/stream", handlers.GenerateSQLStream)

		// Cypher endpoints (require Neo4j — mainnet only)
		r.Group(func(r chi.Router) {
			r.Use(handlers.RequireNeo4jMiddleware)
			r.Post("/api/cypher/query", handlers.ExecuteCypher)
			r.Post("/api/cypher/generate", handlers.GenerateCypher)
			r.Post("/api/cypher/generate/stream", handlers.GenerateCypherStream)
		})

		// Auto-detection endpoint
		r.Post("/api/auto/generate/stream", handlers.AutoGenerateStream)

		// Legacy SQL endpoints (backward compatibility)
		r.Post("/api/query", handlers.ExecuteQuery)
		r.Post("/api/generate", handlers.GenerateSQL)
		r.Post("/api/generate/stream", handlers.GenerateSQLStream)
		r.Post("/api/chat", handlers.Chat)
		r.Post("/api/chat/stream", handlers.ChatStream)
		r.Post("/api/complete", handlers.Complete)
		r.Post("/api/visualize/recommend", handlers.RecommendVisualization)
	})

	// Session persistence routes
	r.Get("/api/sessions", handlers.ListSessions)
	r.Post("/api/sessions", handlers.CreateSession)
	r.Post("/api/sessions/batch", handlers.BatchGetSessions)
	r.Get("/api/sessions/{id}", handlers.GetSession)
	r.Put("/api/sessions/{id}", handlers.UpdateSession)
	r.Delete("/api/sessions/{id}", handlers.DeleteSession)

	// Session workflow route (get running workflow for a session)
	r.Get("/api/sessions/{id}/workflow", handlers.GetWorkflowForSession)

	// Workflow routes (for durable workflow persistence)
	r.Get("/api/workflows/{id}", handlers.GetWorkflow)
	r.Get("/api/workflows/{id}/stream", handlers.StreamWorkflow)

	// Auth routes
	r.Get("/api/auth/me", handlers.GetAuthMe)
	r.Post("/api/auth/logout", handlers.PostAuthLogout)
	r.Get("/api/auth/nonce", handlers.GetAuthNonce)
	r.Post("/api/auth/wallet", handlers.PostAuthWallet)
	r.Post("/api/auth/google", handlers.PostAuthGoogle)
	r.Get("/api/usage/quota", handlers.GetUsageQuota)

	// MCP (Model Context Protocol) server endpoint
	mcpHandler := handlers.InitMCP()
	r.Handle("/api/mcp", mcpHandler)
	r.Handle("/api/mcp/*", mcpHandler)

	// Serve static files from the web dist directory
	webDir := os.Getenv("WEB_DIST_DIR")
	if webDir == "" {
		webDir = "/lake/web/dist"
	}
	// Optional S3 bucket URL for fetching assets not in the local dist
	// (allows serving old assets after deploys while users still have old index.html cached)
	assetBucketURL := os.Getenv("ASSET_BUCKET_URL")
	if _, err := os.Stat(webDir); err == nil {
		slog.Info("serving static files", "dir", webDir)
		if assetBucketURL != "" {
			slog.Info("asset bucket fallback enabled", "url", assetBucketURL)
		}
		r.Get("/*", spaHandler(webDir, assetBucketURL))
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // Disabled for SSE streaming endpoints
		IdleTimeout:  60 * time.Second,
	}

	// Channel to listen for shutdown signals
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Create a cancellable context for all requests - this allows us to signal
	// SSE connections to close during shutdown (http.Server.Shutdown does NOT
	// cancel request contexts by default)
	serverCtx, serverCancel := context.WithCancel(context.Background())
	server.BaseContext = func(_ net.Listener) context.Context {
		return serverCtx
	}

	// Start server in a goroutine
	go func() {
		slog.Info("API server starting", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Start auto-resume of incomplete workflows in background
	go handlers.Manager.ResumeIncompleteWorkflows()

	// Start cleanup worker for expired sessions/nonces
	handlers.StartCleanupWorker(serverCtx)

	// Initialize usage metrics and start daily reset worker
	handlers.InitUsageMetrics(serverCtx)
	handlers.StartDailyResetWorker(serverCtx)

	// Slack OAuth routes (available when SLACK_CLIENT_ID is set, regardless of bot mode)
	if os.Getenv("SLACK_CLIENT_ID") != "" {
		r.Group(func(r chi.Router) {
			r.Use(handlers.RequireAuth)
			r.Get("/api/slack/oauth/start", handlers.GetSlackOAuthStart)
			r.Get("/api/slack/installations", handlers.GetSlackInstallations)
			r.Post("/api/slack/installations/confirm/{pending_id}", handlers.ConfirmSlackInstallation)
			r.Delete("/api/slack/installations/{team_id}", handlers.DeleteSlackInstallation)
		})
		r.Get("/api/slack/oauth/callback", handlers.GetSlackOAuthCallback)
	}

	// Start Slack bot if configured
	var slackEventHandler *slackbot.EventHandler
	if slackBotToken := os.Getenv("SLACK_BOT_TOKEN"); slackBotToken != "" {
		// Single-tenant dev mode
		slackEventHandler = startSlackBot(serverCtx, r)
	} else if os.Getenv("SLACK_CLIENT_ID") != "" && os.Getenv("SLACK_CLIENT_SECRET") != "" {
		// Multi-tenant mode
		slackEventHandler = startSlackBotMultiTenant(serverCtx, r)
	}

	// Wait for shutdown signal
	sig := <-shutdown
	slog.Info("received signal, shutting down gracefully", "signal", sig)

	// Immediately mark as shutting down so readiness probe returns 503
	shuttingDown.Store(true)

	// Stop Slack bot if running (before cancelling server context)
	if slackEventHandler != nil {
		slog.Info("stopping Slack bot...")
		shutdownComplete := slackEventHandler.StopAcceptingNew()
		waitDone := make(chan struct{})
		go func() {
			shutdownComplete()
			close(waitDone)
		}()
		select {
		case <-waitDone:
			slog.Info("Slack bot stopped gracefully")
		case <-time.After(30 * time.Second):
			slog.Warn("Slack bot shutdown timed out")
		}
	}

	// Cancel the server context to signal SSE connections to close
	// This triggers ctx.Done() in all active request handlers
	serverCancel()

	// Stop background cache goroutines (they may be blocking on DB queries)
	handlers.StopPageCache()

	// Give existing connections a short time to complete after context cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("graceful shutdown error", "error", err)
	} else {
		slog.Info("server stopped gracefully")
	}

	// Shutdown metrics server
	if metricsServer != nil {
		if err := metricsServer.Shutdown(ctx); err != nil {
			slog.Error("metrics server shutdown error", "error", err)
		} else {
			slog.Info("metrics server stopped gracefully")
		}
	}
}

// startSlackBot initializes and starts the Slack bot in the background.
// Returns the event handler for graceful shutdown, or nil if startup fails.
func startSlackBot(ctx context.Context, r *chi.Mux) *slackbot.EventHandler {
	// Load Slack config from env
	// Determine mode: socket if SLACK_APP_TOKEN is set, otherwise HTTP
	modeFlag := ""
	cfg, err := slackbot.LoadFromEnv(modeFlag, "", "", false, false)
	if err != nil {
		slog.Error("Slack bot config error, bot will not start", "error", err)
		return nil
	}

	// Initialize Slack client
	slackClient := slackbot.NewClient(cfg.BotToken, cfg.AppToken, slog.Default())
	botUserID, err := slackClient.Initialize(ctx)
	if err != nil {
		slog.Warn("Slack auth test failed, continuing anyway", "error", err)
	}
	cfg.BotUserID = botUserID

	// Set up workflow runner (direct in-process calls instead of HTTP)
	runner := slackbot.NewWorkflowRunner(slog.Default())

	// Set up conversation manager
	convManager := slackbot.NewManager(slog.Default())
	convManager.StartCleanup(ctx)

	// Set up message processor
	msgProcessor := slackbot.NewProcessor(
		slackClient,
		runner,
		convManager,
		slog.Default(),
		cfg.WebBaseURL,
	)
	msgProcessor.StartCleanup(ctx)

	// Set up event handler
	eventHandler := slackbot.NewEventHandler(
		slackClient,
		msgProcessor,
		convManager,
		slog.Default(),
		cfg.BotUserID,
		ctx,
	)
	eventHandler.StartCleanup(ctx)

	// Start bot based on mode
	if cfg.Mode == slackbot.ModeSocket {
		// Socket mode: run in background goroutine
		api := slackClient.API()
		client := socketmode.New(api)

		go func() {
			if err := client.Run(); err != nil {
				slog.Error("Slack socket mode client error", "error", err)
			}
		}()

		go func() {
			if err := eventHandler.HandleSocketMode(ctx, client); err != nil {
				slog.Info("Slack socket mode handler stopped", "error", err)
			}
		}()

		slog.Info("Slack bot started in socket mode")
	} else {
		// HTTP mode: add /slack/events route to the existing router
		r.Post("/slack/events", func(w http.ResponseWriter, r *http.Request) {
			eventHandler.HandleHTTP(w, r, cfg.SigningSecret)
		})

		slog.Info("Slack bot started in HTTP mode", "route", "/slack/events")
	}

	return eventHandler
}

// pgInstallationStore implements slackbot.InstallationStore using the handlers package
type pgInstallationStore struct{}

func (s *pgInstallationStore) GetSlackInstallation(ctx context.Context, teamID string) (*slackbot.Installation, error) {
	inst, err := handlers.GetSlackInstallationByTeamID(ctx, teamID)
	if err != nil {
		return nil, err
	}
	teamName := ""
	if inst.TeamName != nil {
		teamName = *inst.TeamName
	}
	return &slackbot.Installation{
		TeamID:    inst.TeamID,
		TeamName:  teamName,
		BotToken:  inst.BotToken,
		BotUserID: inst.BotUserID,
	}, nil
}

// startSlackBotMultiTenant initializes the Slack bot in multi-tenant mode (HTTP only).
func startSlackBotMultiTenant(ctx context.Context, r *chi.Mux) *slackbot.EventHandler {
	signingSecret := os.Getenv("SLACK_SIGNING_SECRET")
	if signingSecret == "" {
		slog.Error("SLACK_SIGNING_SECRET is required for multi-tenant mode, bot will not start")
		return nil
	}

	// Create client manager backed by Postgres
	store := &pgInstallationStore{}
	clientManager := slackbot.NewClientManager(store, slog.Default())

	// Invalidate cached clients when installations change
	handlers.OnSlackInstallationChange = func(teamID string) {
		clientManager.InvalidateClient(teamID)
	}

	// Set up workflow runner
	runner := slackbot.NewWorkflowRunner(slog.Default())

	// Set up conversation manager
	convManager := slackbot.NewManager(slog.Default())
	convManager.StartCleanup(ctx)

	// Set up message processor (no default client in multi-tenant mode)
	msgProcessor := slackbot.NewProcessor(
		nil, // no default client
		runner,
		convManager,
		slog.Default(),
		os.Getenv("WEB_BASE_URL"),
	)
	msgProcessor.StartCleanup(ctx)

	// Set up event handler (no default client)
	eventHandler := slackbot.NewEventHandler(
		nil, // no default client
		msgProcessor,
		convManager,
		slog.Default(),
		"", // no single bot user ID
		ctx,
	)
	eventHandler.SetClientManager(clientManager)
	eventHandler.SetSigningSecret(signingSecret)
	eventHandler.StartCleanup(ctx)

	// HTTP mode: add /slack/events route
	r.Post("/slack/events", func(w http.ResponseWriter, r *http.Request) {
		eventHandler.HandleHTTPMultiTenant(w, r)
	})

	slog.Info("Slack bot started in multi-tenant HTTP mode", "route", "/slack/events")
	return eventHandler
}
