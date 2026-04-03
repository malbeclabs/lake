package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/joho/godotenv"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	flag "github.com/spf13/pflag"

	"github.com/malbeclabs/lake/admin/remotetables"

	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/malbeclabs/doublezero/config"
	telemetryconfig "github.com/malbeclabs/doublezero/controlplane/telemetry/pkg/config"
	shreds "github.com/malbeclabs/doublezero/sdk/shreds/go"
	"github.com/malbeclabs/doublezero/smartcontract/sdk/go/serviceability"
	"github.com/malbeclabs/doublezero/smartcontract/sdk/go/telemetry"
	"github.com/malbeclabs/doublezero/tools/maxmind/pkg/geoip"
	"github.com/malbeclabs/doublezero/tools/maxmind/pkg/metrodb"
	"github.com/malbeclabs/doublezero/tools/solana/pkg/rpc"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	dztelemusage "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/usage"
	"github.com/malbeclabs/lake/indexer/pkg/dzingest"
	"github.com/malbeclabs/lake/indexer/pkg/indexer"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
	"github.com/malbeclabs/lake/indexer/pkg/rollup"
	"github.com/malbeclabs/lake/indexer/pkg/server"
	"github.com/malbeclabs/lake/indexer/pkg/sol"
	"github.com/malbeclabs/lake/indexer/pkg/solingest"
	"github.com/malbeclabs/lake/indexer/pkg/validatorsapp"
	"github.com/malbeclabs/lake/utils/pkg/logger"
	"github.com/oschwald/geoip2-golang"
)

var (
	// Set by LDFLAGS
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

const (
	defaultListenAddr                   = "0.0.0.0:3010"
	defaultRefreshInterval              = 60 * time.Second
	defaultMaxConcurrency               = 64
	defaultMetricsAddr                  = "0.0.0.0:0"
	defaultGeoipCityDBPath              = "/usr/share/GeoIP/GeoLite2-City.mmdb"
	defaultGeoipASNDBPath               = "/usr/share/GeoIP/GeoLite2-ASN.mmdb"
	defaultDeviceUsageInfluxQueryWindow = 1 * time.Hour
	defaultDeviceUsageRefreshInterval   = 5 * time.Minute

	geoipCityDBPathEnvVar = "GEOIP_CITY_DB_PATH"
	geoipASNDBPathEnvVar  = "GEOIP_ASN_DB_PATH"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	verboseFlag := flag.Bool("verbose", false, "enable verbose (debug) logging")
	enablePprofFlag := flag.Bool("enable-pprof", false, "enable pprof server")
	metricsAddrFlag := flag.String("metrics-addr", defaultMetricsAddr, "Address to listen on for prometheus metrics")
	listenAddrFlag := flag.String("listen-addr", defaultListenAddr, "HTTP server listen address")
	migrationsEnableFlag := flag.Bool("migrations-enable", false, "enable ClickHouse migrations on startup")
	createDatabaseFlag := flag.Bool("create-database", false, "create databases (ClickHouse, Neo4j) before startup (for dev use)")

	// ClickHouse configuration
	clickhouseAddrFlag := flag.String("clickhouse-addr", "", "ClickHouse server address (e.g., localhost:9000, or set CLICKHOUSE_ADDR_TCP env var)")
	clickhouseDatabaseFlag := flag.String("clickhouse-database", "default", "ClickHouse database name (or set CLICKHOUSE_DATABASE env var)")
	clickhouseUsernameFlag := flag.String("clickhouse-username", "default", "ClickHouse username (or set CLICKHOUSE_USERNAME env var)")
	clickhousePasswordFlag := flag.String("clickhouse-password", "", "ClickHouse password (or set CLICKHOUSE_PASSWORD env var)")
	clickhouseSecureFlag := flag.Bool("clickhouse-secure", false, "Enable TLS for ClickHouse Cloud (or set CLICKHOUSE_SECURE=true env var)")

	// Neo4j configuration (optional)
	neo4jURIFlag := flag.String("neo4j-uri", "", "Neo4j server URI (e.g., bolt://localhost:7687, or set NEO4J_URI env var)")
	neo4jDatabaseFlag := flag.String("neo4j-database", "neo4j", "Neo4j database name (or set NEO4J_DATABASE env var)")
	neo4jUsernameFlag := flag.String("neo4j-username", "neo4j", "Neo4j username (or set NEO4J_USERNAME env var)")
	neo4jPasswordFlag := flag.String("neo4j-password", "", "Neo4j password (or set NEO4J_PASSWORD env var)")
	neo4jMigrationsEnableFlag := flag.Bool("neo4j-migrations-enable", false, "Enable Neo4j migrations on startup")

	// GeoIP configuration
	geoipCityDBPathFlag := flag.String("geoip-city-db-path", defaultGeoipCityDBPath, "Path to MaxMind GeoIP2 City database file (or set MCP_GEOIP_CITY_DB_PATH env var)")
	geoipASNDBPathFlag := flag.String("geoip-asn-db-path", defaultGeoipASNDBPath, "Path to MaxMind GeoIP2 ASN database file (or set MCP_GEOIP_ASN_DB_PATH env var)")

	// Indexer configuration
	dzEnvFlag := flag.String("dz-env", config.EnvMainnetBeta, "DZ ledger environment (devnet, testnet, mainnet-beta)")
	solanaEnvFlag := flag.String("solana-env", config.SolanaEnvMainnetBeta, "solana environment (devnet, testnet, mainnet-beta)")
	refreshIntervalFlag := flag.Duration("cache-ttl", defaultRefreshInterval, "cache TTL duration")
	maxConcurrencyFlag := flag.Int("max-concurrency", defaultMaxConcurrency, "maximum number of concurrent operations")
	deviceUsageQueryWindowFlag := flag.Duration("device-usage-query-window", defaultDeviceUsageInfluxQueryWindow, "Query window for device usage (default: 1 hour)")
	deviceUsageRefreshIntervalFlag := flag.Duration("device-usage-refresh-interval", defaultDeviceUsageRefreshInterval, "Refresh interval for device usage (default: 5 minutes)")
	mockDeviceUsageFlag := flag.Bool("mock-device-usage", false, "Use mock data for device usage instead of InfluxDB (for testing/staging)")

	// ISIS configuration (requires Neo4j, enabled by default when Neo4j is configured)
	isisEnabledFlag := flag.Bool("isis-enabled", true, "Enable IS-IS sync from S3 (or set ISIS_ENABLED env var)")
	isisS3BucketFlag := flag.String("isis-s3-bucket", "doublezero-mn-beta-isis-db", "S3 bucket for IS-IS dumps (or set ISIS_S3_BUCKET env var)")
	isisS3RegionFlag := flag.String("isis-s3-region", "us-east-1", "AWS region for IS-IS S3 bucket (or set ISIS_S3_REGION env var)")
	isisRefreshIntervalFlag := flag.Duration("isis-refresh-interval", 30*time.Second, "Refresh interval for IS-IS sync (or set ISIS_REFRESH_INTERVAL env var)")

	// validators.app configuration
	validatorsAppAPIKeyFlag := flag.String("validatorsapp-api-key", "", "validators.app API key (or set VALIDATORSAPP_API_KEY env var)")
	validatorsAppRefreshIntervalFlag := flag.Duration("validatorsapp-refresh-interval", 5*time.Minute, "validators.app refresh interval (or set VALIDATORSAPP_REFRESH_INTERVAL env var)")

	// Temporal worker configuration
	noRollupFlag := flag.Bool("no-rollup", false, "Disable the embedded rollup worker (Temporal-based health bucket computation)")
	noDZIngestFlag := flag.Bool("no-dz-ingest", false, "Disable the embedded DZ mainnet ingest worker (Temporal-based raw data collection)")
	noSolIngestFlag := flag.Bool("no-sol-ingest", false, "Disable the embedded Solana ingest worker (Temporal-based raw data collection)")
	noDevnetFlag := flag.Bool("no-devnet", false, "Disable the devnet secondary network indexer")
	noTestnetFlag := flag.Bool("no-testnet", false, "Disable the testnet secondary network indexer")
	noInfluxDevnetFlag := flag.Bool("no-influx-devnet", false, "Disable InfluxDB ingestion for devnet (or set NO_INFLUX_DEVNET=true env var)")
	noInfluxTestnetFlag := flag.Bool("no-influx-testnet", false, "Disable InfluxDB ingestion for testnet (or set NO_INFLUX_TESTNET=true env var)")

	// Remote tables configuration
	setupRemoteTablesFlag := flag.Bool("setup-remote-tables", false, "Set up remote proxy tables on startup (or set SETUP_REMOTE_TABLES=true env var)")

	// Readiness configuration
	skipReadyWaitFlag := flag.Bool("skip-ready-wait", false, "Skip waiting for views to be ready (for preview/dev environments)")

	flag.Parse()

	// Load .env file. godotenv does not override existing env vars, so
	// process env and explicit exports take precedence.
	_ = godotenv.Load()

	// Override flags with environment variables if set
	if envClickhouseAddr := os.Getenv("CLICKHOUSE_ADDR_TCP"); envClickhouseAddr != "" {
		*clickhouseAddrFlag = envClickhouseAddr
	}
	if envClickhouseDatabase := os.Getenv("CLICKHOUSE_DATABASE"); envClickhouseDatabase != "" {
		*clickhouseDatabaseFlag = envClickhouseDatabase
	}
	if envClickhouseUsername := os.Getenv("CLICKHOUSE_USERNAME"); envClickhouseUsername != "default" {
		*clickhouseUsernameFlag = envClickhouseUsername
	}
	if envClickhousePassword := os.Getenv("CLICKHOUSE_PASSWORD"); envClickhousePassword != "" {
		*clickhousePasswordFlag = envClickhousePassword
	}
	if os.Getenv("CLICKHOUSE_SECURE") == "true" {
		*clickhouseSecureFlag = true
	}
	if os.Getenv("SETUP_REMOTE_TABLES") == "true" {
		*setupRemoteTablesFlag = true
	}
	if envDZEnv := os.Getenv("DZ_ENV"); envDZEnv != "" {
		*dzEnvFlag = envDZEnv
	}

	// Override Neo4j flags with environment variables if set
	if envNeo4jURI := os.Getenv("NEO4J_URI"); envNeo4jURI != "" {
		*neo4jURIFlag = envNeo4jURI
	}
	if envNeo4jDatabase := os.Getenv("NEO4J_DATABASE"); envNeo4jDatabase != "" {
		*neo4jDatabaseFlag = envNeo4jDatabase
	}
	if envNeo4jUsername := os.Getenv("NEO4J_USERNAME"); envNeo4jUsername != "" {
		*neo4jUsernameFlag = envNeo4jUsername
	}
	if envNeo4jPassword := os.Getenv("NEO4J_PASSWORD"); envNeo4jPassword != "" {
		*neo4jPasswordFlag = envNeo4jPassword
	}

	// Override ISIS flags with environment variables if set
	if envISISEnabled := os.Getenv("ISIS_ENABLED"); envISISEnabled != "" {
		*isisEnabledFlag = envISISEnabled == "true"
	}
	if envISISBucket := os.Getenv("ISIS_S3_BUCKET"); envISISBucket != "" {
		*isisS3BucketFlag = envISISBucket
	}
	if envISISRegion := os.Getenv("ISIS_S3_REGION"); envISISRegion != "" {
		*isisS3RegionFlag = envISISRegion
	}
	if envISISRefreshInterval := os.Getenv("ISIS_REFRESH_INTERVAL"); envISISRefreshInterval != "" {
		if d, err := time.ParseDuration(envISISRefreshInterval); err == nil {
			*isisRefreshIntervalFlag = d
		}
	}

	// Override validators.app flags with environment variables
	if envKey := os.Getenv("VALIDATORSAPP_API_KEY"); envKey != "" {
		*validatorsAppAPIKeyFlag = envKey
	}
	if envInterval := os.Getenv("VALIDATORSAPP_REFRESH_INTERVAL"); envInterval != "" {
		if d, err := time.ParseDuration(envInterval); err == nil {
			*validatorsAppRefreshIntervalFlag = d
		}
	}

	// Override mock device usage flag with environment variable if set
	if os.Getenv("MOCK_DEVICE_USAGE") == "true" {
		*mockDeviceUsageFlag = true
	}
	if os.Getenv("NO_INFLUX_DEVNET") == "true" {
		*noInfluxDevnetFlag = true
	}
	if os.Getenv("NO_INFLUX_TESTNET") == "true" {
		*noInfluxTestnetFlag = true
	}

	// For non-mainnet envs, use "lake_<env>" as the ClickHouse database.
	if *dzEnvFlag != config.EnvMainnetBeta {
		*clickhouseDatabaseFlag = "lake_" + *dzEnvFlag
	}

	// Solana, GeoIP, Neo4j, and ISIS are only enabled for mainnet-beta for now.
	solanaEnabled := *dzEnvFlag == config.EnvMainnetBeta
	geoipEnabled := *dzEnvFlag == config.EnvMainnetBeta
	neo4jEnabled := *dzEnvFlag == config.EnvMainnetBeta

	networkConfig, err := config.NetworkConfigForEnv(*dzEnvFlag)
	if err != nil {
		return fmt.Errorf("failed to get network config: %w", err)
	}

	var solanaNetworkConfig *config.SolanaNetworkConfig
	if solanaEnabled {
		solanaNetworkConfig, err = config.SolanaNetworkConfigForEnv(*solanaEnvFlag)
		if err != nil {
			return fmt.Errorf("failed to get solana network config: %w", err)
		}
	}

	log := logger.New(*verboseFlag).With("dz_env", *dzEnvFlag)

	log.Info("indexer starting",
		"version", version,
		"commit", commit,
		"solana_env", *solanaEnvFlag,
		"solana_enabled", solanaEnabled,
		"geoip_enabled", geoipEnabled,
		"neo4j_enabled", neo4jEnabled,
	)

	// Set up signal handling with detailed logging
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Log which signal was received
	go func() {
		sig := <-sigCh
		log.Info("server: received signal", "signal", sig.String())
		cancel()
	}()

	if *enablePprofFlag {
		go func() {
			log.Info("starting pprof server", "address", "localhost:6060")
			err := http.ListenAndServe("localhost:6060", nil)
			if err != nil {
				log.Error("failed to start pprof server", "error", err)
			}
		}()
	}

	var metricsServerErrCh = make(chan error, 1)
	if *metricsAddrFlag != "" {
		metrics.BuildInfo.WithLabelValues(version, commit, date).Set(1)
		go func() {
			listener, err := net.Listen("tcp", *metricsAddrFlag)
			if err != nil {
				log.Error("failed to start prometheus metrics server listener", "error", err)
				metricsServerErrCh <- err
				return
			}
			log.Info("prometheus metrics server listening", "address", listener.Addr().String())
			http.Handle("/metrics", promhttp.Handler())
			if err := http.Serve(listener, nil); err != nil {
				log.Error("failed to start prometheus metrics server", "error", err)
				metricsServerErrCh <- err
				return
			}
		}()
	}

	dzRPCClient := rpc.NewWithRetries(networkConfig.LedgerPublicRPCURL, nil)
	defer dzRPCClient.Close()
	serviceabilityClient := serviceability.New(dzRPCClient, networkConfig.ServiceabilityProgramID)
	telemetryClient := telemetry.New(log, dzRPCClient, nil, networkConfig.TelemetryProgramID)

	// Shreds subscription client (mainnet-beta and testnet only, not devnet).
	// Mainnet uses Solana proper RPC; testnet uses the DZ ledger RPC.
	shredsEnabled := *dzEnvFlag != config.EnvDevnet
	var shredsClient *shreds.Client
	var shredsRawRPC *solanarpc.Client
	if shredsEnabled {
		shredsRPCURL := networkConfig.LedgerPublicRPCURL
		if *dzEnvFlag == config.EnvMainnetBeta {
			shredsRPCURL = networkConfig.SolanaRPCURL
		}
		shredsRawRPC = shreds.NewRPCClient(shredsRPCURL)
		shredsClient = shreds.New(shredsRawRPC, shreds.ProgramID)
		log.Info("shreds subscription client initialized", "env", *dzEnvFlag, "rpc_url", shredsRPCURL)
	}

	var solanaRPC sol.SolanaRPC
	if solanaEnabled {
		solanaRPCClient := rpc.NewWithRetries(solanaNetworkConfig.RPCURL, nil)
		defer solanaRPCClient.Close()
		solanaRPC = solanaRPCClient
	}

	// Initialize ClickHouse client (required)
	if *clickhouseAddrFlag == "" {
		return fmt.Errorf("clickhouse-addr is required")
	}

	// Create the ClickHouse database if requested (for dev use).
	if *createDatabaseFlag {
		log.Info("creating ClickHouse database", "database", *clickhouseDatabaseFlag)
		adminClient, err := clickhouse.NewClient(ctx, log, *clickhouseAddrFlag, "default", *clickhouseUsernameFlag, *clickhousePasswordFlag, *clickhouseSecureFlag)
		if err != nil {
			return fmt.Errorf("failed to create admin ClickHouse client: %w", err)
		}
		adminConn, err := adminClient.Conn(ctx)
		if err != nil {
			adminClient.Close()
			return fmt.Errorf("failed to get admin ClickHouse connection: %w", err)
		}
		if err := clickhouse.CreateDatabase(ctx, log, adminConn, *clickhouseDatabaseFlag); err != nil {
			adminClient.Close()
			return fmt.Errorf("failed to create database %s: %w", *clickhouseDatabaseFlag, err)
		}
		adminClient.Close()
	}

	// Set up remote proxy tables if requested (for dev use).
	if *setupRemoteTablesFlag {
		remoteHost := os.Getenv("REMOTE_CH_HOST")
		remoteUser := os.Getenv("REMOTE_CH_USER")
		remotePassword := os.Getenv("REMOTE_CH_PASSWORD")
		remoteDatabase := os.Getenv("REMOTE_CH_DATABASE")
		if remoteHost != "" && remoteUser != "" && remotePassword != "" {
			log.Info("setting up remote proxy tables", "remote_host", remoteHost)
			if err := remotetables.Setup(log, remotetables.Config{
				LocalAddr:      *clickhouseAddrFlag,
				LocalDatabase:  *clickhouseDatabaseFlag,
				LocalUsername:  *clickhouseUsernameFlag,
				LocalPassword:  *clickhousePasswordFlag,
				LocalSecure:    *clickhouseSecureFlag,
				RemoteHost:     remoteHost,
				RemoteUser:     remoteUser,
				RemotePassword: remotePassword,
				RemoteDatabase: remoteDatabase,
				Force:          true,
			}); err != nil {
				return fmt.Errorf("failed to set up remote tables: %w", err)
			}
		} else {
			log.Info("skipping remote tables setup (REMOTE_CH_HOST, REMOTE_CH_USER, or REMOTE_CH_PASSWORD not set)")
		}
	}

	log.Debug("clickhouse client initializing", "addr", *clickhouseAddrFlag, "database", *clickhouseDatabaseFlag, "username", *clickhouseUsernameFlag, "secure", *clickhouseSecureFlag)
	clickhouseDB, err := clickhouse.NewClient(ctx, log, *clickhouseAddrFlag, *clickhouseDatabaseFlag, *clickhouseUsernameFlag, *clickhousePasswordFlag, *clickhouseSecureFlag)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client: %w", err)
	}
	defer func() {
		if err := clickhouseDB.Close(); err != nil {
			log.Error("failed to close ClickHouse database", "error", err)
		}
	}()
	log.Info("clickhouse client initialized", "addr", *clickhouseAddrFlag, "database", *clickhouseDatabaseFlag)

	// Create ingestion log writer for recording activity runs to ClickHouse.
	chConn, err := clickhouseDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection for ingestion log: %w", err)
	}
	ingestionLogWriter := ingestionlog.NewWriter(chConn, log)

	// Start background ClickHouse connection pool stats collector.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				metrics.CollectClickHousePoolStats(clickhouseDB.Stats())
			}
		}
	}()

	// Determine GeoIP database paths: flag takes precedence, then env var, then default
	geoipCityDBPath := *geoipCityDBPathFlag
	if geoipCityDBPath == defaultGeoipCityDBPath {
		if envPath := os.Getenv(geoipCityDBPathEnvVar); envPath != "" {
			geoipCityDBPath = envPath
		}
	}

	geoipASNDBPath := *geoipASNDBPathFlag
	if geoipASNDBPath == defaultGeoipASNDBPath {
		if envPath := os.Getenv(geoipASNDBPathEnvVar); envPath != "" {
			geoipASNDBPath = envPath
		}
	}

	// Initialize GeoIP resolver (optional)
	var geoIPResolver geoip.Resolver
	if geoipEnabled {
		var geoIPCloseFn func() error
		geoIPResolver, geoIPCloseFn, err = initializeGeoIP(geoipCityDBPath, geoipASNDBPath, log)
		if err != nil {
			return fmt.Errorf("failed to initialize GeoIP: %w", err)
		}
		defer func() {
			if err := geoIPCloseFn(); err != nil {
				log.Error("failed to close GeoIP resolver", "error", err)
			}
		}()
	}

	// Initialize InfluxDB client from environment variables (optional, mainnet-beta only)
	influxEnabled := *dzEnvFlag == config.EnvMainnetBeta
	var influxDBClient dztelemusage.InfluxDBClient
	influxURL := os.Getenv("INFLUX_URL")
	influxToken := os.Getenv("INFLUX_TOKEN")
	influxBucket := os.Getenv("INFLUX_BUCKET")
	var deviceUsageQueryWindow time.Duration
	if *deviceUsageQueryWindowFlag == 0 {
		deviceUsageQueryWindow = defaultDeviceUsageInfluxQueryWindow
	} else {
		deviceUsageQueryWindow = *deviceUsageQueryWindowFlag
	}
	if !influxEnabled {
		log.Info("device usage (InfluxDB) disabled for non-mainnet env")
	} else if *mockDeviceUsageFlag {
		log.Info("device usage: using mock data (--mock-device-usage enabled)")
		influxDBClient = dztelemusage.NewMockInfluxDBClient(dztelemusage.MockInfluxDBClientConfig{
			ClickHouse: clickhouseDB,
			Logger:     log,
		})
		influxBucket = "mock-bucket"
	} else if influxURL != "" && influxToken != "" && influxBucket != "" {
		influxDBClient, err = dztelemusage.NewSDKInfluxDBClient(influxURL, influxToken, influxBucket)
		if err != nil {
			return fmt.Errorf("failed to create InfluxDB client: %w", err)
		}
		defer func() {
			if influxDBClient != nil {
				if closeErr := influxDBClient.Close(); closeErr != nil {
					log.Warn("failed to close InfluxDB client", "error", closeErr)
				}
			}
		}()
		log.Info("device usage (InfluxDB) client initialized")
	} else {
		log.Info("device usage (InfluxDB) environment variables not set, telemetry usage view will be disabled")
	}

	// Initialize Neo4j client (optional, mainnet-beta only)
	var neo4jClient neo4j.Client
	if neo4jEnabled && *neo4jURIFlag != "" {
		if *createDatabaseFlag {
			if err := neo4j.CreateDatabase(ctx, log, *neo4jURIFlag, *neo4jUsernameFlag, *neo4jPasswordFlag, *neo4jDatabaseFlag); err != nil {
				return fmt.Errorf("failed to create Neo4j database: %w", err)
			}
		}
		neo4jClient, err = neo4j.NewClient(ctx, log, *neo4jURIFlag, *neo4jDatabaseFlag, *neo4jUsernameFlag, *neo4jPasswordFlag)
		if err != nil {
			return fmt.Errorf("failed to create Neo4j client: %w", err)
		}
		defer func() {
			if neo4jClient != nil {
				if closeErr := neo4jClient.Close(ctx); closeErr != nil {
					log.Warn("failed to close Neo4j client", "error", closeErr)
				}
			}
		}()

		log.Info("Neo4j client initialized", "uri", *neo4jURIFlag, "database", *neo4jDatabaseFlag)
	} else {
		log.Info("Neo4j disabled", "neo4j_enabled", neo4jEnabled, "neo4j_uri_set", *neo4jURIFlag != "")
	}

	// Initialize validators.app client (optional, mainnet-beta only)
	var validatorsAppClient validatorsapp.Client
	if *dzEnvFlag == config.EnvMainnetBeta && *validatorsAppAPIKeyFlag != "" {
		validatorsAppClient = validatorsapp.NewHTTPClient("https://www.validators.app", *validatorsAppAPIKeyFlag)
		log.Info("validators.app client initialized")
	} else if *validatorsAppAPIKeyFlag != "" {
		log.Info("validators.app disabled (mainnet-beta only)", "dz_env", *dzEnvFlag)
	}

	// Initialize indexer (creates views but does not start refresh loops —
	// Temporal workflows handle scheduling).
	idxCfg := indexer.Config{
		DZEnv:            *dzEnvFlag,
		Logger:           log,
		Clock:            clockwork.NewRealClock(),
		ClickHouse:       clickhouseDB,
		MigrationsEnable: *migrationsEnableFlag,
		MigrationsConfig: clickhouse.MigrationConfig{
			Addr:     *clickhouseAddrFlag,
			Database: *clickhouseDatabaseFlag,
			Username: *clickhouseUsernameFlag,
			Password: *clickhousePasswordFlag,
			Secure:   *clickhouseSecureFlag,
		},

		RefreshInterval: *refreshIntervalFlag,
		MaxConcurrency:  *maxConcurrencyFlag,

		// GeoIP configuration
		GeoIPResolver: geoIPResolver,

		// Serviceability configuration
		ServiceabilityRPC: serviceabilityClient,

		// Telemetry configuration
		TelemetryRPC:           telemetryClient,
		DZEpochRPC:             dzRPCClient,
		InternetLatencyAgentPK: networkConfig.InternetLatencyCollectorPK,
		InternetDataProviders:  telemetryconfig.InternetTelemetryDataProviders,

		// Device usage configuration
		DeviceUsageInfluxClient:      influxDBClient,
		DeviceUsageInfluxBucket:      influxBucket,
		DeviceUsageInfluxQueryWindow: deviceUsageQueryWindow,
		DeviceUsageRefreshInterval:   *deviceUsageRefreshIntervalFlag,

		// Solana configuration
		SolanaRPC: solanaRPC,

		// Neo4j configuration
		Neo4j:                 neo4jClient,
		Neo4jMigrationsEnable: *neo4jMigrationsEnableFlag,
		Neo4jMigrationsConfig: neo4j.MigrationConfig{
			URI:      *neo4jURIFlag,
			Database: *neo4jDatabaseFlag,
			Username: *neo4jUsernameFlag,
			Password: *neo4jPasswordFlag,
		},

		// ISIS configuration
		ISISEnabled:         *isisEnabledFlag,
		ISISS3Bucket:        *isisS3BucketFlag,
		ISISS3Region:        *isisS3RegionFlag,
		ISISRefreshInterval: *isisRefreshIntervalFlag,

		// validators.app configuration
		ValidatorsAppClient:          validatorsAppClient,
		ValidatorsAppRefreshInterval: *validatorsAppRefreshIntervalFlag,

		// Readiness configuration
		SkipReadyWait: *skipReadyWaitFlag,
	}
	if shredsClient != nil {
		idxCfg.ShredsRPC = shredsClient
		idxCfg.ShredsRawRPC = shredsRawRPC
		idxCfg.ShredsProgramID = shreds.ProgramID
	}
	idx, err := indexer.New(ctx, idxCfg)
	if err != nil {
		return fmt.Errorf("failed to create indexer: %w", err)
	}
	defer func() {
		if err := idx.Close(); err != nil {
			log.Error("failed to close indexer", "error", err)
		}
	}()

	// Initialize HTTP server (health/readiness/version endpoints).
	server, err := server.New(server.Config{
		Log:               log,
		ListenAddr:        *listenAddrFlag,
		ReadHeaderTimeout: 30 * time.Second,
		ShutdownTimeout:   10 * time.Second,
		VersionInfo: server.VersionInfo{
			Version: version,
			Commit:  commit,
			Date:    date,
		},
		Indexer: idx,
	})
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	serverErrCh := make(chan error, 1)
	go func() {
		err := server.Run(ctx)
		if err != nil {
			serverErrCh <- err
		}
	}()

	// Start the embedded DZ mainnet ingest worker (Temporal-based raw data collection).
	var dzIngestErrCh chan error
	if !*noDZIngestFlag {
		dzIngestErrCh = make(chan error, 1)
		go func() {
			err := dzingest.Start(ctx, dzingest.Config{
				Log:            log.With("component", "dz-ingest"),
				IngestionLog:   ingestionLogWriter,
				Network:        *dzEnvFlag,
				Serviceability: idx.Serviceability(),
				Shreds:         idx.Shreds(),
				TelemLatency:   idx.TelemLatency(),
				TelemUsage:     idx.TelemUsage(),
				GraphStore:     idx.GraphStore(),
				ISISSource:     idx.ISISSource(),
				ISISStore:      idx.ISISStore(),
			})
			if err != nil {
				dzIngestErrCh <- err
			}
		}()
	} else {
		log.Info("dz ingest worker disabled (--no-dz-ingest)")
	}

	// Start the embedded Solana ingest worker (Temporal-based raw data collection).
	var solIngestErrCh chan error
	if !*noSolIngestFlag && idx.Solana() != nil {
		solIngestErrCh = make(chan error, 1)
		go func() {
			err := solingest.Start(ctx, solingest.Config{
				Log:           log.With("component", "sol-ingest"),
				IngestionLog:  ingestionLogWriter,
				Network:       *dzEnvFlag,
				Solana:        idx.Solana(),
				GeoIP:         idx.GeoIP(),
				ValidatorsApp: idx.ValidatorsApp(),
			})
			if err != nil {
				solIngestErrCh <- err
			}
		}()
	} else {
		log.Info("sol ingest worker disabled", "no_sol_ingest", *noSolIngestFlag, "solana_configured", idx.Solana() != nil)
	}

	// Start secondary network indexers (devnet, testnet).
	// These run lightweight DZ ingest workflows (serviceability + telemetry only).
	// Enabled by default; disable with --no-devnet / --no-testnet.
	// Database names default to lake_devnet / lake_testnet but can be overridden via env vars.
	type secondaryEnvConfig struct {
		database       string
		dzLedgerRPCURL string
		noInflux       bool
	}
	secondaryEnvs := map[string]secondaryEnvConfig{
		"devnet":  {database: "lake_devnet"},
		"testnet": {database: "lake_testnet"},
	}
	if db := os.Getenv("CLICKHOUSE_DATABASE_DEVNET"); db != "" {
		cfg := secondaryEnvs["devnet"]
		cfg.database = db
		secondaryEnvs["devnet"] = cfg
	}
	if db := os.Getenv("CLICKHOUSE_DATABASE_TESTNET"); db != "" {
		cfg := secondaryEnvs["testnet"]
		cfg.database = db
		secondaryEnvs["testnet"] = cfg
	}
	if rpcURL := os.Getenv("DZ_LEDGER_RPC_URL_DEVNET"); rpcURL != "" {
		cfg := secondaryEnvs["devnet"]
		cfg.dzLedgerRPCURL = rpcURL
		secondaryEnvs["devnet"] = cfg
	}
	if rpcURL := os.Getenv("DZ_LEDGER_RPC_URL_TESTNET"); rpcURL != "" {
		cfg := secondaryEnvs["testnet"]
		cfg.dzLedgerRPCURL = rpcURL
		secondaryEnvs["testnet"] = cfg
	}
	if *noInfluxDevnetFlag {
		cfg := secondaryEnvs["devnet"]
		cfg.noInflux = true
		secondaryEnvs["devnet"] = cfg
	}
	if *noInfluxTestnetFlag {
		cfg := secondaryEnvs["testnet"]
		cfg.noInflux = true
		secondaryEnvs["testnet"] = cfg
	}
	if *noDevnetFlag {
		delete(secondaryEnvs, "devnet")
	}
	if *noTestnetFlag {
		delete(secondaryEnvs, "testnet")
	}
	for env, envCfg := range secondaryEnvs {
		go func() {
			// InfluxDB bucket per network (e.g. doublezero-devnet, doublezero-testnet).
			secondaryInfluxURL := influxURL
			secondaryInfluxToken := influxToken
			secondaryInfluxBucket := "doublezero-" + env
			if envCfg.noInflux {
				log.Info("InfluxDB ingestion disabled for secondary network", "env", env)
				secondaryInfluxURL = ""
				secondaryInfluxToken = ""
				secondaryInfluxBucket = ""
			}

			if err := startSecondaryNetwork(ctx, logger.New(*verboseFlag), env, secondaryNetworkConfig{
				clickhouseAddr:             *clickhouseAddrFlag,
				clickhouseDatabase:         envCfg.database,
				dzLedgerRPCURL:             envCfg.dzLedgerRPCURL,
				clickhouseUsername:         *clickhouseUsernameFlag,
				clickhousePassword:         *clickhousePasswordFlag,
				clickhouseSecure:           *clickhouseSecureFlag,
				refreshInterval:            *refreshIntervalFlag,
				maxConcurrency:             *maxConcurrencyFlag,
				migrationsEnable:           *migrationsEnableFlag,
				createDatabase:             true,
				skipReadyWait:              *skipReadyWaitFlag,
				isisS3Bucket:               "doublezero-" + env + "-isis-db",
				isisS3Region:               *isisS3RegionFlag,
				influxURL:                  secondaryInfluxURL,
				influxToken:                secondaryInfluxToken,
				influxBucket:               secondaryInfluxBucket,
				deviceUsageRefreshInterval: *deviceUsageRefreshIntervalFlag,
				deviceUsageQueryWindow:     deviceUsageQueryWindow,
			}); err != nil {
				log.Error("secondary network indexer failed", "env", env, "error", err)
			}
		}()
	}

	// Start the embedded rollup worker (Temporal-based health bucket computation).
	var rollupErrCh chan error
	if !*noRollupFlag {
		rollupErrCh = make(chan error, 1)
		go func() {
			err := rollup.Start(ctx, rollup.Config{
				Log:                log.With("component", "rollup"),
				Network:            *dzEnvFlag,
				ClickHouseAddr:     *clickhouseAddrFlag,
				ClickHouseDatabase: *clickhouseDatabaseFlag,
				ClickHouseUsername: *clickhouseUsernameFlag,
				ClickHousePassword: *clickhousePasswordFlag,
				ClickHouseSecure:   *clickhouseSecureFlag,
			})
			if err != nil {
				rollupErrCh <- err
			}
		}()
	} else {
		log.Info("rollup worker disabled (--no-rollup)")
	}

	select {
	case <-ctx.Done():
		log.Info("server: shutting down", "reason", ctx.Err())
		return nil
	case err := <-serverErrCh:
		log.Error("server: server error causing shutdown", "error", err)
		return err
	case err := <-metricsServerErrCh:
		log.Error("server: metrics server error causing shutdown", "error", err)
		return err
	case err := <-dzIngestErrCh:
		log.Error("server: dz ingest worker error causing shutdown", "error", err)
		return err
	case err := <-solIngestErrCh:
		log.Error("server: sol ingest worker error causing shutdown", "error", err)
		return err
	case err := <-rollupErrCh:
		log.Error("server: rollup worker error causing shutdown", "error", err)
		return err
	}
}

// secondaryNetworkConfig holds ClickHouse and indexer settings for a
// secondary (non-primary) network indexer.
type secondaryNetworkConfig struct {
	clickhouseAddr     string
	clickhouseDatabase string
	clickhouseUsername string
	clickhousePassword string
	clickhouseSecure   bool
	refreshInterval    time.Duration
	maxConcurrency     int
	migrationsEnable   bool
	createDatabase     bool
	skipReadyWait      bool

	// DZ ledger RPC URL override (optional).
	dzLedgerRPCURL string

	// ISIS configuration (optional).
	isisS3Bucket string
	isisS3Region string

	// InfluxDB configuration (optional).
	influxURL                  string
	influxToken                string
	influxBucket               string
	deviceUsageRefreshInterval time.Duration
	deviceUsageQueryWindow     time.Duration
}

// startSecondaryNetwork starts a lightweight indexer for a non-primary DZ
// network (e.g. devnet, testnet). It only runs serviceability and telemetry
// latency — Solana, GeoIP, Neo4j, ISIS, and rollup are mainnet-only.
func startSecondaryNetwork(ctx context.Context, log *slog.Logger, env string, cfg secondaryNetworkConfig) error {
	log = log.With("dz_env", env)
	log.Info("starting secondary network indexer", "database", cfg.clickhouseDatabase)

	networkConfig, err := config.NetworkConfigForEnv(env)
	if err != nil {
		return fmt.Errorf("failed to get network config for %s: %w", env, err)
	}

	if cfg.dzLedgerRPCURL != "" {
		networkConfig.LedgerPublicRPCURL = cfg.dzLedgerRPCURL
	}

	// Create database if requested.
	if cfg.createDatabase {
		adminClient, err := clickhouse.NewClient(ctx, log, cfg.clickhouseAddr, "default", cfg.clickhouseUsername, cfg.clickhousePassword, cfg.clickhouseSecure)
		if err != nil {
			return fmt.Errorf("failed to create admin ClickHouse client: %w", err)
		}
		adminConn, err := adminClient.Conn(ctx)
		if err != nil {
			adminClient.Close()
			return fmt.Errorf("failed to get admin ClickHouse connection: %w", err)
		}
		if err := clickhouse.CreateDatabase(ctx, log, adminConn, cfg.clickhouseDatabase); err != nil {
			adminClient.Close()
			return fmt.Errorf("failed to create database %s: %w", cfg.clickhouseDatabase, err)
		}
		adminClient.Close()
	}

	chClient, err := clickhouse.NewClient(ctx, log, cfg.clickhouseAddr, cfg.clickhouseDatabase, cfg.clickhouseUsername, cfg.clickhousePassword, cfg.clickhouseSecure)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client for %s: %w", env, err)
	}
	defer chClient.Close()

	secondaryChConn, err := chClient.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection for ingestion log (%s): %w", env, err)
	}
	secondaryIngestionLog := ingestionlog.NewWriter(secondaryChConn, log)

	dzRPCClient := rpc.NewWithRetries(networkConfig.LedgerPublicRPCURL, nil)
	defer dzRPCClient.Close()
	serviceabilityClient := serviceability.New(dzRPCClient, networkConfig.ServiceabilityProgramID)
	telemetryClient := telemetry.New(log, dzRPCClient, nil, networkConfig.TelemetryProgramID)

	// Shreds subscription client (testnet only, not devnet).
	var shredsClient *shreds.Client
	var secondaryShredsRawRPC *solanarpc.Client
	if env != config.EnvDevnet {
		secondaryShredsRawRPC = shreds.NewRPCClient(networkConfig.LedgerPublicRPCURL)
		shredsClient = shreds.New(secondaryShredsRawRPC, shreds.ProgramID)
		log.Info("shreds subscription client initialized", "env", env)
	}

	// Initialize InfluxDB client for device usage (optional).
	var influxDBClient dztelemusage.InfluxDBClient
	if cfg.influxURL != "" && cfg.influxToken != "" && cfg.influxBucket != "" {
		influxDBClient, err = dztelemusage.NewSDKInfluxDBClient(cfg.influxURL, cfg.influxToken, cfg.influxBucket)
		if err != nil {
			return fmt.Errorf("failed to create InfluxDB client for %s: %w", env, err)
		}
		defer influxDBClient.Close()
		log.Info("device usage (InfluxDB) client initialized", "bucket", cfg.influxBucket)
	}

	secondaryIdxCfg := indexer.Config{
		DZEnv:            env,
		Logger:           log,
		Clock:            clockwork.NewRealClock(),
		ClickHouse:       chClient,
		MigrationsEnable: cfg.migrationsEnable,
		MigrationsConfig: clickhouse.MigrationConfig{
			Addr:     cfg.clickhouseAddr,
			Database: cfg.clickhouseDatabase,
			Username: cfg.clickhouseUsername,
			Password: cfg.clickhousePassword,
			Secure:   cfg.clickhouseSecure,
		},
		RefreshInterval:        cfg.refreshInterval,
		MaxConcurrency:         cfg.maxConcurrency,
		ServiceabilityRPC:      serviceabilityClient,
		TelemetryRPC:           telemetryClient,
		DZEpochRPC:             dzRPCClient,
		InternetLatencyAgentPK: networkConfig.InternetLatencyCollectorPK,
		InternetDataProviders:  telemetryconfig.InternetTelemetryDataProviders,
		SkipReadyWait:          cfg.skipReadyWait,

		// Device usage configuration.
		DeviceUsageInfluxClient:      influxDBClient,
		DeviceUsageInfluxBucket:      cfg.influxBucket,
		DeviceUsageInfluxQueryWindow: cfg.deviceUsageQueryWindow,
		DeviceUsageRefreshInterval:   cfg.deviceUsageRefreshInterval,

		// ISIS configuration.
		ISISEnabled:  cfg.isisS3Bucket != "",
		ISISS3Bucket: cfg.isisS3Bucket,
		ISISS3Region: cfg.isisS3Region,
	}
	if shredsClient != nil {
		secondaryIdxCfg.ShredsRPC = shredsClient
		secondaryIdxCfg.ShredsRawRPC = secondaryShredsRawRPC
		secondaryIdxCfg.ShredsProgramID = shreds.ProgramID
	}
	idx, err := indexer.New(ctx, secondaryIdxCfg)
	if err != nil {
		return fmt.Errorf("failed to create indexer for %s: %w", env, err)
	}
	defer idx.Close()

	// Start rollup worker in the background.
	rollupErrCh := make(chan error, 1)
	go func() {
		rollupErrCh <- rollup.Start(ctx, rollup.Config{
			Log:                log.With("component", "rollup"),
			Network:            env,
			ClickHouseAddr:     cfg.clickhouseAddr,
			ClickHouseDatabase: cfg.clickhouseDatabase,
			ClickHouseUsername: cfg.clickhouseUsername,
			ClickHousePassword: cfg.clickhousePassword,
			ClickHouseSecure:   cfg.clickhouseSecure,
		})
	}()

	// Start DZ ingest worker (blocks until ctx cancelled or error).
	dzErr := dzingest.Start(ctx, dzingest.Config{
		Log:            log.With("component", "dz-ingest"),
		IngestionLog:   secondaryIngestionLog,
		Network:        env,
		Serviceability: idx.Serviceability(),
		Shreds:         idx.Shreds(),
		TelemLatency:   idx.TelemLatency(),
		TelemUsage:     idx.TelemUsage(),
		GraphStore:     idx.GraphStore(),
		ISISSource:     idx.ISISSource(),
		ISISStore:      idx.ISISStore(),
	})

	select {
	case err := <-rollupErrCh:
		if err != nil {
			return err
		}
		return dzErr
	default:
		return dzErr
	}
}

func initializeGeoIP(cityDBPath, asnDBPath string, log *slog.Logger) (geoip.Resolver, func() error, error) {
	cityDB, err := geoip2.Open(cityDBPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open GeoIP city database: %w", err)
	}

	asnDB, err := geoip2.Open(asnDBPath)
	if err != nil {
		cityDB.Close()
		return nil, nil, fmt.Errorf("failed to open GeoIP ASN database: %w", err)
	}

	metroDB, err := metrodb.New()
	if err != nil {
		cityDB.Close()
		asnDB.Close()
		return nil, nil, fmt.Errorf("failed to create metro database: %w", err)
	}

	resolver, err := geoip.NewResolver(log, cityDB, asnDB, metroDB)
	if err != nil {
		cityDB.Close()
		asnDB.Close()
		return nil, nil, fmt.Errorf("failed to create GeoIP resolver: %w", err)
	}

	return resolver, func() error {
		if err := cityDB.Close(); err != nil {
			return fmt.Errorf("failed to close city database: %w", err)
		}
		if err := asnDB.Close(); err != nil {
			return fmt.Errorf("failed to close ASN database: %w", err)
		}
		return nil
	}, nil
}
