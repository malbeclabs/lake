package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
	"golang.org/x/sync/errgroup"
)

// StatusResponse contains comprehensive health/status information
type StatusResponse struct {
	// Overall status
	Status    string `json:"status"` // "healthy", "degraded", "unhealthy"
	Timestamp string `json:"timestamp"`

	// System health
	System SystemHealth `json:"system"`

	// Network summary
	Network NetworkSummary `json:"network"`

	// Link health
	Links LinkHealth `json:"links"`

	// Interface issues
	Interfaces InterfaceHealth `json:"interfaces"`

	// Infrastructure alerts (non-activated devices/links)
	Alerts InfrastructureAlerts `json:"alerts"`

	// Performance metrics
	Performance PerformanceMetrics `json:"performance"`

	// Device utilization (top by tunnel usage)
	TopDeviceUtil []DeviceUtilization `json:"top_device_util"`

	Error string `json:"error,omitempty"`
}

type SystemHealth struct {
	Database     bool   `json:"database"`
	DatabaseMsg  string `json:"database_msg,omitempty"`
	LastIngested string `json:"last_ingested,omitempty"` // Most recent data timestamp
}

type NetworkSummary struct {
	// Counts
	ValidatorsOnDZ  uint64  `json:"validators_on_dz"`
	TotalStakeSol   float64 `json:"total_stake_sol"`
	StakeSharePct   float64 `json:"stake_share_pct"`
	StakeShareDelta float64 `json:"stake_share_delta"` // Change from 24h ago (percentage points)
	Users           uint64  `json:"users"`
	Devices         uint64  `json:"devices"`
	Links           uint64  `json:"links"`
	Contributors    uint64  `json:"contributors"`
	Metros          uint64  `json:"metros"`
	BandwidthBps    int64   `json:"bandwidth_bps"`
	UserInboundBps  float64 `json:"user_inbound_bps"`

	// Status breakdown
	DevicesByStatus map[string]uint64 `json:"devices_by_status"`
	LinksByStatus   map[string]uint64 `json:"links_by_status"`
}

type LinkHealth struct {
	Total         uint64       `json:"total"`
	Healthy       uint64       `json:"healthy"`
	Degraded      uint64       `json:"degraded"`        // High latency or some loss
	Unhealthy     uint64       `json:"unhealthy"`       // Significant loss
	Down          uint64       `json:"down"`            // 100% packet loss (link is down)
	Issues        []LinkIssue  `json:"issues"`          // Top issues
	HighUtilLinks []LinkMetric `json:"high_util_links"` // Links with high utilization
	TopUtilLinks  []LinkMetric `json:"top_util_links"`  // Top 10 links by max utilization
}

type LinkIssue struct {
	Code        string  `json:"code"`
	LinkType    string  `json:"link_type"`
	Contributor string  `json:"contributor"`
	Issue       string  `json:"issue"`     // "packet_loss", "high_latency", "down"
	Value       float64 `json:"value"`     // The problematic value
	Threshold   float64 `json:"threshold"` // The threshold exceeded
	SideAMetro  string  `json:"side_a_metro"`
	SideZMetro  string  `json:"side_z_metro"`
	Since       string  `json:"since"`   // ISO timestamp when issue started
	IsDown      bool    `json:"is_down"` // 100% loss in last 5 minutes
}

type LinkMetric struct {
	PK             string  `json:"pk"`
	Code           string  `json:"code"`
	LinkType       string  `json:"link_type"`
	Contributor    string  `json:"contributor"`
	BandwidthBps   int64   `json:"bandwidth_bps"`
	InBps          float64 `json:"in_bps"`
	OutBps         float64 `json:"out_bps"`
	UtilizationIn  float64 `json:"utilization_in"`
	UtilizationOut float64 `json:"utilization_out"`
	SideAMetro     string  `json:"side_a_metro"`
	SideZMetro     string  `json:"side_z_metro"`
}

type DeviceUtilization struct {
	PK           string  `json:"pk"`
	Code         string  `json:"code"`
	DeviceType   string  `json:"device_type"`
	Contributor  string  `json:"contributor"`
	Metro        string  `json:"metro"`
	CurrentUsers int32   `json:"current_users"`
	MaxUsers     int32   `json:"max_users"`
	Utilization  float64 `json:"utilization"` // percentage
}

type PerformanceMetrics struct {
	// Latency stats (WAN links, last 3 hours)
	AvgLatencyUs float64 `json:"avg_latency_us"`
	P95LatencyUs float64 `json:"p95_latency_us"`
	MinLatencyUs float64 `json:"min_latency_us"`
	MaxLatencyUs float64 `json:"max_latency_us"`

	// Packet loss (WAN links, last 3 hours)
	AvgLossPercent float64 `json:"avg_loss_percent"`

	// Jitter (WAN links, last 3 hours)
	AvgJitterUs float64 `json:"avg_jitter_us"`

	// Total throughput
	TotalInBps  float64 `json:"total_in_bps"`
	TotalOutBps float64 `json:"total_out_bps"`
}

type InterfaceHealth struct {
	Issues []InterfaceIssue `json:"issues"` // Interfaces with errors/discards/carrier transitions
}

type InterfaceIssue struct {
	DevicePK           string `json:"device_pk"`
	DeviceCode         string `json:"device_code"`
	DeviceType         string `json:"device_type"`
	Contributor        string `json:"contributor"`
	Metro              string `json:"metro"`
	InterfaceName      string `json:"interface_name"`
	LinkPK             string `json:"link_pk,omitempty"`   // Empty if not a link interface
	LinkCode           string `json:"link_code,omitempty"` // Empty if not a link interface
	LinkType           string `json:"link_type,omitempty"` // WAN, DZX, etc.
	LinkSide           string `json:"link_side,omitempty"` // A or Z
	InErrors           uint64 `json:"in_errors"`
	OutErrors          uint64 `json:"out_errors"`
	InFcsErrors        uint64 `json:"in_fcs_errors"`
	InDiscards         uint64 `json:"in_discards"`
	OutDiscards        uint64 `json:"out_discards"`
	CarrierTransitions uint64 `json:"carrier_transitions"`
	FirstSeen          string `json:"first_seen"` // When issues first appeared in window
	LastSeen           string `json:"last_seen"`  // Most recent occurrence in window
}

type NonActivatedDevice struct {
	PK         string `json:"pk"`
	Code       string `json:"code"`
	DeviceType string `json:"device_type"`
	Metro      string `json:"metro"`
	Status     string `json:"status"`
	Since      string `json:"since"` // ISO timestamp when entered this status
}

type NonActivatedLink struct {
	PK         string `json:"pk"`
	Code       string `json:"code"`
	LinkType   string `json:"link_type"`
	SideAMetro string `json:"side_a_metro"`
	SideZMetro string `json:"side_z_metro"`
	Status     string `json:"status"`
	Since      string `json:"since"` // ISO timestamp when entered this status
}

type ISISDeviceIssue struct {
	Code       string `json:"code"`
	DeviceType string `json:"device_type"`
	Metro      string `json:"metro"`
	Issue      string `json:"issue"` // "overload", "unreachable"
	Since      string `json:"since"` // ISO timestamp
}

type InfrastructureAlerts struct {
	Devices     []NonActivatedDevice `json:"devices"`
	Links       []NonActivatedLink   `json:"links"`
	ISISDevices []ISISDeviceIssue    `json:"isis_devices"`
}

// Thresholds for health classification (matching methodology)
// Packet loss severity: Minor (<1%), Moderate (1-10%), Severe (≥10%)
const (
	LatencyWarningPct  = 20.0 // 20% over committed RTT
	LatencyCriticalPct = 50.0 // 50% over committed RTT
	LossWarningPct     = 1.0  // 1% - Moderate (degraded)
	LossCriticalPct    = 10.0 // 10% - Severe (unhealthy)
	UtilWarningPct     = 70.0 // 70%
	UtilCriticalPct    = 90.0 // 90%

	// committedRttProvisioningNs is the sentinel committed_rtt_ns value (1000ms)
	// that indicates a link is still being provisioned and not yet operational.
	committedRttProvisioningNs = 1_000_000_000
)

func GetStatus(w http.ResponseWriter, r *http.Request) {
	// Try to serve from cache first (cache only holds mainnet data)
	if isMainnet(r.Context()) && pageCache != nil {
		if cached := pageCache.GetStatus(); cached != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			if err := json.NewEncoder(w).Encode(cached); err != nil {
				slog.Error("failed to encode response", "error", err)
			}
			return
		}
	}

	// Cache miss - fetch fresh data
	w.Header().Set("X-Cache", "MISS")
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	resp := fetchStatusData(ctx)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

// fetchStatusData performs the actual status data fetch from the database.
// This is called by both the cache refresh and direct requests.
func fetchStatusData(ctx context.Context) *StatusResponse {
	start := time.Now()

	resp := &StatusResponse{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Network: NetworkSummary{
			DevicesByStatus: make(map[string]uint64),
			LinksByStatus:   make(map[string]uint64),
		},
		Links: LinkHealth{
			Issues:        []LinkIssue{},
			HighUtilLinks: []LinkMetric{},
		},
		Interfaces: InterfaceHealth{
			Issues: []InterfaceIssue{},
		},
		Alerts: InfrastructureAlerts{
			Devices:     []NonActivatedDevice{},
			Links:       []NonActivatedLink{},
			ISISDevices: []ISISDeviceIssue{},
		},
	}

	g, ctx := errgroup.WithContext(ctx)
	// Limit concurrent ClickHouse queries to avoid exhausting the connection pool
	// during cache refreshes (this function launches ~22 parallel queries).
	g.SetLimit(10)

	// Check database connectivity
	g.Go(func() error {
		pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
		defer pingCancel()
		if err := config.DB.Ping(pingCtx); err != nil {
			resp.System.Database = false
			resp.System.DatabaseMsg = err.Error()
		} else {
			resp.System.Database = true
		}
		return nil
	})

	// Get last ingested timestamp
	g.Go(func() error {
		query := `
			SELECT formatDateTime(max(event_ts), '%Y-%m-%dT%H:%i:%sZ', 'UTC')
			FROM fact_dz_device_link_latency
			WHERE event_ts > now() - INTERVAL 1 HOUR
		`
		row := envDB(ctx).QueryRow(ctx, query)
		var ts string
		if err := row.Scan(&ts); err == nil && ts != "" {
			resp.System.LastIngested = ts
		}
		return nil
	})

	// Network summary stats (same as /api/stats)
	g.Go(func() error {
		query := `
			SELECT COUNT(DISTINCT va.vote_pubkey) AS validators_on_dz
			FROM dz_users_current u
			JOIN solana_gossip_nodes_current gn ON u.client_ip = gn.gossip_ip
			JOIN solana_vote_accounts_current va ON gn.pubkey = va.node_pubkey
			WHERE u.status = 'activated'
			  AND u.client_ip != ''
			  AND va.epoch_vote_account = 'true'
			  AND va.activated_stake_lamports > 0
		`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.ValidatorsOnDZ)
	})

	g.Go(func() error {
		query := `
			SELECT COALESCE(SUM(stake), 0) / 1000000000.0 AS total_stake_sol
			FROM (
				SELECT DISTINCT va.vote_pubkey, va.activated_stake_lamports AS stake
				FROM dz_users_current u
				JOIN solana_gossip_nodes_current gn ON u.client_ip = gn.gossip_ip
				JOIN solana_vote_accounts_current va ON gn.pubkey = va.node_pubkey
				WHERE u.status = 'activated'
				  AND u.client_ip != ''
				  AND va.epoch_vote_account = 'true'
				  AND va.activated_stake_lamports > 0
			)
		`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.TotalStakeSol)
	})

	g.Go(func() error {
		query := `
			SELECT
				COALESCE(
					(SELECT SUM(stake) FROM (
					 SELECT DISTINCT va.vote_pubkey, va.activated_stake_lamports AS stake
					 FROM dz_users_current u
					 JOIN solana_gossip_nodes_current gn ON u.client_ip = gn.gossip_ip
					 JOIN solana_vote_accounts_current va ON gn.pubkey = va.node_pubkey
					 WHERE u.status = 'activated' AND u.client_ip != '' AND va.epoch_vote_account = 'true' AND va.activated_stake_lamports > 0
					))
					* 100.0 / NULLIF((SELECT SUM(activated_stake_lamports) FROM solana_vote_accounts_current WHERE activated_stake_lamports > 0), 0),
					0
				) AS stake_share_pct
		`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.StakeSharePct)
	})

	// Calculate stake share delta from 24 hours ago (or oldest available if less than 24h of data)
	g.Go(func() error {
		query := `
			WITH historical_ts AS (
				-- Get the oldest snapshot that's at least 1 hour old
				SELECT max(snapshot_ts) as ts
				FROM dim_solana_vote_accounts_history
				WHERE snapshot_ts <= now() - INTERVAL 1 HOUR
			),
			current_share AS (
				SELECT COALESCE(
					(SELECT SUM(stake) FROM (
					 SELECT DISTINCT va.vote_pubkey, va.activated_stake_lamports AS stake
					 FROM dz_users_current u
					 JOIN solana_gossip_nodes_current gn ON u.client_ip = gn.gossip_ip
					 JOIN solana_vote_accounts_current va ON gn.pubkey = va.node_pubkey
					 WHERE u.status = 'activated' AND u.client_ip != '' AND va.epoch_vote_account = 'true' AND va.activated_stake_lamports > 0
					))
					* 100.0 / NULLIF((SELECT SUM(activated_stake_lamports) FROM solana_vote_accounts_current WHERE activated_stake_lamports > 0), 0),
					0
				) AS pct
			),
			gossip_at_time AS (
				SELECT
					entity_id,
					argMax(gossip_ip, snapshot_ts) as gossip_ip,
					argMax(pubkey, snapshot_ts) as pubkey
				FROM dim_solana_gossip_nodes_history
				WHERE snapshot_ts <= (SELECT ts FROM historical_ts)
				GROUP BY entity_id
				HAVING gossip_ip != ''
			),
			historical_share AS (
				SELECT COALESCE(
					(SELECT SUM(stake) FROM (
					 SELECT DISTINCT va.node_pubkey, va.activated_stake_lamports AS stake
					 FROM dim_dz_users_history u
					 JOIN gossip_at_time gn ON u.client_ip = gn.gossip_ip
					 JOIN dim_solana_vote_accounts_history va ON gn.pubkey = va.node_pubkey
					 WHERE u.status = 'activated'
					   AND va.activated_stake_lamports > 0
					   AND u.snapshot_ts = (SELECT max(snapshot_ts) FROM dim_dz_users_history WHERE snapshot_ts <= (SELECT ts FROM historical_ts))
					   AND va.snapshot_ts = (SELECT ts FROM historical_ts)))
					* 100.0 / NULLIF((SELECT SUM(activated_stake_lamports) FROM dim_solana_vote_accounts_history
					  WHERE activated_stake_lamports > 0
					    AND snapshot_ts = (SELECT ts FROM historical_ts)), 0),
					0
				) AS pct
			)
			SELECT
				-- Only show delta if we have valid historical data (non-zero historical share)
				CASE WHEN historical_share.pct > 0
				     THEN current_share.pct - historical_share.pct
				     ELSE 0
				END AS delta
			FROM current_share, historical_share
		`
		row := envDB(ctx).QueryRow(ctx, query)
		var delta float64
		if err := row.Scan(&delta); err != nil {
			// If historical data unavailable, delta is 0
			resp.Network.StakeShareDelta = 0
			return nil
		}
		resp.Network.StakeShareDelta = delta
		return nil
	})

	g.Go(func() error {
		query := `SELECT COUNT(*) FROM dz_users_current`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.Users)
	})

	g.Go(func() error {
		query := `SELECT COUNT(*) FROM dz_devices_current`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.Devices)
	})

	g.Go(func() error {
		query := `SELECT COUNT(*) FROM dz_links_current`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.Links)
	})

	g.Go(func() error {
		query := `SELECT COUNT(DISTINCT pk) FROM dz_contributors_current`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.Contributors)
	})

	g.Go(func() error {
		query := `SELECT COUNT(DISTINCT pk) FROM dz_metros_current`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.Metros)
	})

	// Sum total bandwidth for all links
	g.Go(func() error {
		query := `SELECT COALESCE(SUM(bandwidth_bps), 0) FROM dz_links_current`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.BandwidthBps)
	})

	g.Go(func() error {
		query := `
			SELECT COALESCE(SUM(interface_rate), 0) FROM (
				SELECT SUM(in_octets_delta) * 8.0 / NULLIF(SUM(delta_duration), 0) AS interface_rate
				FROM fact_dz_device_interface_counters
				WHERE event_ts > now() - INTERVAL 1 HOUR
				  AND user_tunnel_id IS NOT NULL
				  AND delta_duration > 0
				  AND in_octets_delta >= 0
				GROUP BY device_pk, intf
			)
		`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.UserInboundBps)
	})

	// Device status breakdown
	g.Go(func() error {
		query := `SELECT status, COUNT(*) as cnt FROM dz_devices_current GROUP BY status`
		rows, err := envDB(ctx).Query(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var status string
			var cnt uint64
			if err := rows.Scan(&status, &cnt); err != nil {
				return err
			}
			resp.Network.DevicesByStatus[status] = cnt
		}
		return rows.Err()
	})

	// Link status breakdown
	g.Go(func() error {
		query := `SELECT
			CASE WHEN committed_rtt_ns = ? THEN 'provisioning' ELSE status END as effective_status,
			COUNT(*) as cnt
		FROM dz_links_current
		GROUP BY effective_status`
		rows, err := envDB(ctx).Query(ctx, query, committedRttProvisioningNs)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var status string
			var cnt uint64
			if err := rows.Scan(&status, &cnt); err != nil {
				return err
			}
			resp.Network.LinksByStatus[status] = cnt
		}
		return rows.Err()
	})

	// Link health analysis
	g.Go(func() error {
		// 1000ms delay override in nanoseconds indicates soft-drained
		const delayOverrideSoftDrainedNs = 1_000_000_000
		query := `
			SELECT
				l.pk,
				l.code,
				l.link_type,
				COALESCE(c.code, '') as contributor,
				l.bandwidth_bps,
				l.committed_rtt_ns / 1000.0 as committed_rtt_us,
				ma.code as side_a_metro,
				mz.code as side_z_metro,
				COALESCE(lat.avg_rtt_us, 0) as latency_us,
				COALESCE(lat.loss_percent, 0) as loss_percent,
				-- Use direct link traffic if available, otherwise use parent interface traffic
				COALESCE(traffic_direct.in_bps, traffic_parent.in_bps, 0) as in_bps,
				COALESCE(traffic_direct.out_bps, traffic_parent.out_bps, 0) as out_bps,
				COALESCE(h.is_down, false) as is_down
			FROM dz_links_current l
			JOIN dz_devices_current da ON l.side_a_pk = da.pk
			JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			JOIN dz_metros_current ma ON da.metro_pk = ma.pk
			JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
			LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
			LEFT JOIN dz_links_health_current h ON l.pk = h.pk
			LEFT JOIN (
				SELECT link_pk,
					avg(rtt_us) as avg_rtt_us,
					countIf(loss OR rtt_us = 0) * 100.0 / count(*) as loss_percent
				FROM fact_dz_device_link_latency
				WHERE event_ts > now() - INTERVAL 1 HOUR
				GROUP BY link_pk
			) lat ON l.pk = lat.link_pk
			-- Direct link traffic (where link_pk is populated)
			LEFT JOIN (
				SELECT link_pk,
					quantile(0.95)(CASE WHEN delta_duration > 0 THEN in_octets_delta * 8 / delta_duration ELSE 0 END) as in_bps,
					quantile(0.95)(CASE WHEN delta_duration > 0 THEN out_octets_delta * 8 / delta_duration ELSE 0 END) as out_bps
				FROM fact_dz_device_interface_counters
				WHERE event_ts > now() - INTERVAL 24 HOUR
					AND link_pk != ''
					AND delta_duration > 0
					AND in_octets_delta >= 0
					AND out_octets_delta >= 0
				GROUP BY link_pk
			) traffic_direct ON l.pk = traffic_direct.link_pk
			-- Parent interface traffic (for sub-interfaces like PortChannel2000.10023)
			LEFT JOIN (
				SELECT device_pk, intf,
					quantile(0.95)(CASE WHEN delta_duration > 0 THEN in_octets_delta * 8 / delta_duration ELSE 0 END) as in_bps,
					quantile(0.95)(CASE WHEN delta_duration > 0 THEN out_octets_delta * 8 / delta_duration ELSE 0 END) as out_bps
				FROM fact_dz_device_interface_counters
				WHERE event_ts > now() - INTERVAL 24 HOUR
					AND delta_duration > 0
					AND in_octets_delta >= 0
					AND out_octets_delta >= 0
				GROUP BY device_pk, intf
			) traffic_parent ON traffic_parent.device_pk = l.side_a_pk
				AND traffic_parent.intf = splitByChar('.', l.side_a_iface_name)[1]
				AND traffic_direct.link_pk IS NULL
			WHERE l.status = 'activated'
				AND l.isis_delay_override_ns != ?
				AND l.committed_rtt_ns != ?
		`
		rows, err := envDB(ctx).Query(ctx, query, delayOverrideSoftDrainedNs, committedRttProvisioningNs)
		if err != nil {
			return err
		}
		defer rows.Close()

		var healthy, degraded, unhealthy, down uint64
		var issues []LinkIssue
		var highUtil []LinkMetric
		var allUtilLinks []LinkMetric

		for rows.Next() {
			var pk, code, linkType, contributor, sideAMetro, sideZMetro string
			var bandwidthBps int64
			var committedRttUs, latencyUs, lossPct, inBps, outBps float64
			var isDown bool

			if err := rows.Scan(&pk, &code, &linkType, &contributor, &bandwidthBps, &committedRttUs, &sideAMetro, &sideZMetro, &latencyUs, &lossPct, &inBps, &outBps, &isDown); err != nil {
				return err
			}

			// Calculate latency overage percentage vs committed RTT
			// Only consider latency for inter-metro WAN links
			var latencyOveragePct float64
			isInterMetroWAN := linkType == "WAN" && sideAMetro != sideZMetro
			if isInterMetroWAN && committedRttUs > 0 && latencyUs > 0 {
				latencyOveragePct = ((latencyUs - committedRttUs) / committedRttUs) * 100
			}

			// Classify link health
			isUnhealthy := !isDown && (lossPct >= LossCriticalPct || latencyOveragePct >= LatencyCriticalPct)
			isDegraded := !isDown && !isUnhealthy && (lossPct >= LossWarningPct || latencyOveragePct >= LatencyWarningPct)

			if isDown {
				down++
			} else if isUnhealthy {
				unhealthy++
			} else if isDegraded {
				degraded++
			} else {
				healthy++
			}

			// Track issues (top 10)
			if lossPct >= LossWarningPct && len(issues) < 10 {
				issues = append(issues, LinkIssue{
					Code:        code,
					LinkType:    linkType,
					Contributor: contributor,
					Issue:       "packet_loss",
					Value:       lossPct,
					Threshold:   LossWarningPct,
					SideAMetro:  sideAMetro,
					SideZMetro:  sideZMetro,
					IsDown:      isDown,
				})
			}
			if isInterMetroWAN && latencyOveragePct >= LatencyWarningPct && len(issues) < 10 {
				issues = append(issues, LinkIssue{
					Code:        code,
					LinkType:    linkType,
					Contributor: contributor,
					Issue:       "high_latency",
					Value:       latencyOveragePct, // Now shows % over committed
					Threshold:   LatencyWarningPct,
					SideAMetro:  sideAMetro,
					SideZMetro:  sideZMetro,
				})
			}

			// Track utilization links
			if bandwidthBps > 0 {
				utilIn := (inBps / float64(bandwidthBps)) * 100
				utilOut := (outBps / float64(bandwidthBps)) * 100
				metric := LinkMetric{
					PK:             pk,
					Code:           code,
					LinkType:       linkType,
					Contributor:    contributor,
					BandwidthBps:   bandwidthBps,
					InBps:          inBps,
					OutBps:         outBps,
					UtilizationIn:  utilIn,
					UtilizationOut: utilOut,
					SideAMetro:     sideAMetro,
					SideZMetro:     sideZMetro,
				}
				// Track all for top utilization list
				allUtilLinks = append(allUtilLinks, metric)
				// Track high utilization (>70%) separately
				if (utilIn >= UtilWarningPct || utilOut >= UtilWarningPct) && len(highUtil) < 10 {
					highUtil = append(highUtil, metric)
				}
			}
		}

		// Sort all links by max utilization (descending) and take top 10
		sort.Slice(allUtilLinks, func(i, j int) bool {
			maxI := allUtilLinks[i].UtilizationIn
			if allUtilLinks[i].UtilizationOut > maxI {
				maxI = allUtilLinks[i].UtilizationOut
			}
			maxJ := allUtilLinks[j].UtilizationIn
			if allUtilLinks[j].UtilizationOut > maxJ {
				maxJ = allUtilLinks[j].UtilizationOut
			}
			return maxI > maxJ
		})
		if len(allUtilLinks) > 100 {
			allUtilLinks = allUtilLinks[:100]
		}

		resp.Links.Total = healthy + degraded + unhealthy + down
		resp.Links.Healthy = healthy
		resp.Links.Degraded = degraded
		resp.Links.Unhealthy = unhealthy
		resp.Links.Down = down
		resp.Links.HighUtilLinks = highUtil
		resp.Links.TopUtilLinks = allUtilLinks

		// Populate issue start times - find when the CURRENT continuous issue started
		if len(issues) > 0 {
			// Build list of link codes to query
			linkCodes := make([]string, len(issues))
			for i, issue := range issues {
				linkCodes[i] = issue.Code
			}

			// Query to find when the current continuous issue started:
			// Use 5-minute buckets (matching incident detection granularity) and find the
			// last sustained healthy period, then the issue started the bucket after.
			// A healthy period requires 3 consecutive healthy buckets (15 minutes),
			// matching the detection layer's coalesce gap. This prevents brief dips
			// below threshold from resetting the duration counter.
			// If no healthy period exists in the last 7 days, use the earliest data we have.
			issueStartQuery := `
				WITH buckets AS (
					SELECT
						l.code,
						toStartOfInterval(event_ts, INTERVAL 5 MINUTE) as bucket,
						countIf(loss OR rtt_us = 0) * 100.0 / count(*) as loss_pct
					FROM fact_dz_device_link_latency lat
					JOIN dz_links_current l ON lat.link_pk = l.pk
					WHERE lat.event_ts > now() - INTERVAL 7 DAY
					  AND l.code IN (?)
					GROUP BY l.code, bucket
					HAVING count(*) >= 3
				),
				last_healthy AS (
					-- Find 3 consecutive healthy buckets (15 min, matching detection coalesce gap).
					-- The last such triplet's end marks when the issue started.
					SELECT b1.code as code, max(b3.bucket) as last_good_bucket
					FROM buckets b1
					JOIN buckets b2 ON b1.code = b2.code
						AND b2.bucket = b1.bucket + INTERVAL 5 MINUTE AND b2.loss_pct < ?
					JOIN buckets b3 ON b1.code = b3.code
						AND b3.bucket = b1.bucket + INTERVAL 10 MINUTE AND b3.loss_pct < ?
					WHERE b1.loss_pct < ?
					GROUP BY b1.code
				),
				earliest_issue AS (
					SELECT code, min(bucket) as first_issue_bucket
					FROM buckets
					WHERE loss_pct >= ?
					GROUP BY code
				)
				SELECT
					ei.code,
					if(lh.code != '',
					   lh.last_good_bucket + INTERVAL 5 MINUTE,
					   ei.first_issue_bucket) as issue_start
				FROM earliest_issue ei
				LEFT JOIN last_healthy lh ON ei.code = lh.code
			`
			issueRows, err := envDB(ctx).Query(ctx, issueStartQuery, linkCodes, LossWarningPct, LossWarningPct, LossWarningPct, LossWarningPct)
			if err == nil {
				defer issueRows.Close()
				issueSince := make(map[string]time.Time)
				for issueRows.Next() {
					var code string
					var issueStart time.Time
					if err := issueRows.Scan(&code, &issueStart); err == nil {
						issueSince[code] = issueStart
					}
				}
				// Populate Since field and filter out resolved issues
				// An issue is considered resolved if its calculated start time is in the future,
				// which happens when the current bucket is healthy (last_good_bucket + 5min > now)
				now := time.Now()

				// Save original issues so we can adjust severity counts for any that get filtered out.
				// Without this, the banner can show "N links with degraded performance" while the
				// expanded details list is empty (the issue was resolved between metric snapshot and
				// bucket-based persistence check).
				originalIssues := make([]LinkIssue, len(issues))
				copy(originalIssues, issues)

				filtered := issues[:0]
				for i := range issues {
					if since, ok := issueSince[issues[i].Code]; ok {
						if since.After(now) {
							// Issue has ended - the current hour is healthy
							continue
						}
						issues[i].Since = since.UTC().Format(time.RFC3339)
					}
					filtered = append(filtered, issues[i])
				}
				issues = filtered

				// Adjust health counts for links whose issues were filtered out as resolved.
				// Classify each removed link by its most severe original issue.
				removedSeverity := make(map[string]int) // code -> 0=degraded, 1=unhealthy, 2=down
				for _, orig := range originalIssues {
					sev := 0
					if orig.IsDown {
						sev = 2
					} else if (orig.Issue == "packet_loss" && orig.Value >= LossCriticalPct) ||
						(orig.Issue == "high_latency" && orig.Value >= LatencyCriticalPct) {
						sev = 1
					}
					if existing, ok := removedSeverity[orig.Code]; !ok || sev > existing {
						removedSeverity[orig.Code] = sev
					}
				}
				// Remove links that still have at least one issue remaining
				for _, remaining := range issues {
					delete(removedSeverity, remaining.Code)
				}
				// Decrement the appropriate counter and move to healthy
				for _, sev := range removedSeverity {
					switch sev {
					case 2:
						if resp.Links.Down > 0 {
							resp.Links.Down--
							resp.Links.Healthy++
						}
					case 1:
						if resp.Links.Unhealthy > 0 {
							resp.Links.Unhealthy--
							resp.Links.Healthy++
						}
					case 0:
						if resp.Links.Degraded > 0 {
							resp.Links.Degraded--
							resp.Links.Healthy++
						}
					}
				}
			}
		}

		// Add "no_data" issues for links that stopped reporting latency data
		// These are links with historical data (30 days) but no recent data (15 minutes)
		noDataQuery := `
			WITH link_last_seen AS (
				SELECT
					link_pk,
					max(event_ts) as last_seen
				FROM fact_dz_device_link_latency
				WHERE event_ts >= now() - INTERVAL 30 DAY
				  AND link_pk != ''
				GROUP BY link_pk
			)
			SELECT
				l.code,
				l.link_type,
				COALESCE(c.code, '') as contributor,
				COALESCE(ma.code, '') as side_a_metro,
				COALESCE(mz.code, '') as side_z_metro,
				lls.last_seen
			FROM link_last_seen lls
			JOIN dz_links_current l ON lls.link_pk = l.pk
			LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
			LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
			LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
			LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
			WHERE lls.last_seen < now() - INTERVAL 15 MINUTE
			  AND lls.last_seen >= now() - INTERVAL 30 DAY
			  AND l.status NOT IN ('soft-drained', 'hard-drained')
			  AND l.committed_rtt_ns != ?
			ORDER BY lls.last_seen DESC
			LIMIT 10
		`
		noDataRows, err := envDB(ctx).Query(ctx, noDataQuery, committedRttProvisioningNs)
		if err == nil {
			defer noDataRows.Close()
			for noDataRows.Next() {
				var code, linkType, contributor, sideAMetro, sideZMetro string
				var lastSeen time.Time
				if err := noDataRows.Scan(&code, &linkType, &contributor, &sideAMetro, &sideZMetro, &lastSeen); err == nil {
					// The outage started when we last saw data (plus 5 min buffer for expected interval)
					since := lastSeen.Add(5 * time.Minute)
					issues = append(issues, LinkIssue{
						Code:        code,
						LinkType:    linkType,
						Contributor: contributor,
						Issue:       "no_data",
						Value:       0,
						Threshold:   0,
						SideAMetro:  sideAMetro,
						SideZMetro:  sideZMetro,
						Since:       since.UTC().Format(time.RFC3339),
					})
				}
			}
		}

		resp.Links.Issues = issues

		return rows.Err()
	})

	// Performance metrics (WAN links, last 3 hours)
	g.Go(func() error {
		query := `
			SELECT
				ifNotFinite(avg(rtt_us), 0) as avg_latency,
				ifNotFinite(quantile(0.95)(rtt_us), 0) as p95_latency,
				ifNotFinite(toFloat64(min(rtt_us)), 0) as min_latency,
				ifNotFinite(toFloat64(max(rtt_us)), 0) as max_latency,
				ifNotFinite(countIf(loss OR rtt_us = 0) * 100.0 / count(*), 0) as avg_loss,
				ifNotFinite(avg(abs(ipdv_us)), 0) as avg_jitter
			FROM fact_dz_device_link_latency lat
			JOIN dz_links_current l ON lat.link_pk = l.pk
			WHERE lat.event_ts > now() - INTERVAL 3 HOUR
			  AND l.link_type = 'WAN'
			  AND lat.loss = false
			  AND lat.rtt_us > 0
		`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(
			&resp.Performance.AvgLatencyUs,
			&resp.Performance.P95LatencyUs,
			&resp.Performance.MinLatencyUs,
			&resp.Performance.MaxLatencyUs,
			&resp.Performance.AvgLossPercent,
			&resp.Performance.AvgJitterUs,
		)
	})

	// Total throughput (sum of per-interface rates)
	g.Go(func() error {
		query := `
			SELECT
				COALESCE(SUM(in_rate), 0) as total_in_bps,
				COALESCE(SUM(out_rate), 0) as total_out_bps
			FROM (
				SELECT
					SUM(in_octets_delta) * 8.0 / NULLIF(SUM(delta_duration), 0) AS in_rate,
					SUM(out_octets_delta) * 8.0 / NULLIF(SUM(delta_duration), 0) AS out_rate
				FROM fact_dz_device_interface_counters
				WHERE event_ts > now() - INTERVAL 5 MINUTE
				  AND link_pk != ''
				  AND delta_duration > 0
				  AND in_octets_delta >= 0
				  AND out_octets_delta >= 0
				GROUP BY device_pk, intf
			)
		`
		row := envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Performance.TotalInBps, &resp.Performance.TotalOutBps)
	})

	// Top device utilization by tunnel usage
	g.Go(func() error {
		query := `
			SELECT
				d.pk,
				d.code,
				d.device_type,
				COALESCE(c.code, '') as contributor,
				m.code as metro,
				toInt32(count(u.pk)) as current_users,
				d.max_users,
				CASE WHEN d.max_users > 0 THEN count(u.pk) * 100.0 / d.max_users ELSE 0 END as utilization
			FROM dz_devices_current d
			LEFT JOIN dz_users_current u ON u.device_pk = d.pk
			LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			WHERE d.status = 'activated'
			  AND d.max_users > 0
			GROUP BY d.pk, d.code, d.device_type, c.code, m.code, d.max_users
			ORDER BY utilization DESC
			LIMIT 100
		`
		rows, err := envDB(ctx).Query(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		var devices []DeviceUtilization
		for rows.Next() {
			var d DeviceUtilization
			if err := rows.Scan(&d.PK, &d.Code, &d.DeviceType, &d.Contributor, &d.Metro, &d.CurrentUsers, &d.MaxUsers, &d.Utilization); err != nil {
				return err
			}
			devices = append(devices, d)
		}
		resp.TopDeviceUtil = devices
		return rows.Err()
	})

	// Interface issues (errors, discards, carrier transitions in last 1 hour)
	g.Go(func() error {
		query := `
			SELECT
				d.pk as device_pk,
				d.code as device_code,
				d.device_type,
				COALESCE(contrib.code, '') as contributor,
				m.code as metro,
				c.intf as interface_name,
				COALESCE(l.pk, '') as link_pk,
				COALESCE(l.code, '') as link_code,
				COALESCE(l.link_type, '') as link_type,
				COALESCE(c.link_side, '') as link_side,
				toUInt64(SUM(greatest(0, c.in_errors_delta))) as in_errors,
				toUInt64(SUM(greatest(0, c.out_errors_delta))) as out_errors,
				toUInt64(SUM(greatest(0, c.in_fcs_errors_delta))) as in_fcs_errors,
				toUInt64(SUM(greatest(0, c.in_discards_delta))) as in_discards,
				toUInt64(SUM(greatest(0, c.out_discards_delta))) as out_discards,
				toUInt64(SUM(greatest(0, c.carrier_transitions_delta))) as carrier_transitions,
				formatDateTime(min(c.event_ts), '%Y-%m-%dT%H:%i:%sZ', 'UTC') as first_seen,
				formatDateTime(max(c.event_ts), '%Y-%m-%dT%H:%i:%sZ', 'UTC') as last_seen
			FROM fact_dz_device_interface_counters c
			JOIN dz_devices_current d ON c.device_pk = d.pk
			JOIN dz_metros_current m ON d.metro_pk = m.pk
			LEFT JOIN dz_contributors_current contrib ON d.contributor_pk = contrib.pk
			LEFT JOIN dz_links_current l ON c.link_pk = l.pk
			WHERE c.event_ts > now() - INTERVAL 1 HOUR
			  AND d.status = 'activated'
			  AND (c.in_errors_delta > 0 OR c.out_errors_delta > 0 OR c.in_fcs_errors_delta > 0 OR c.in_discards_delta > 0 OR c.out_discards_delta > 0 OR c.carrier_transitions_delta > 0)
			GROUP BY d.pk, d.code, d.device_type, contrib.code, m.code, c.intf, l.pk, l.code, l.link_type, c.link_side
			ORDER BY (in_errors + out_errors + in_fcs_errors + in_discards + out_discards + carrier_transitions) DESC
			LIMIT 20
		`
		rows, err := envDB(ctx).Query(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		var issues []InterfaceIssue
		for rows.Next() {
			var issue InterfaceIssue
			if err := rows.Scan(
				&issue.DevicePK,
				&issue.DeviceCode,
				&issue.DeviceType,
				&issue.Contributor,
				&issue.Metro,
				&issue.InterfaceName,
				&issue.LinkPK,
				&issue.LinkCode,
				&issue.LinkType,
				&issue.LinkSide,
				&issue.InErrors,
				&issue.OutErrors,
				&issue.InFcsErrors,
				&issue.InDiscards,
				&issue.OutDiscards,
				&issue.CarrierTransitions,
				&issue.FirstSeen,
				&issue.LastSeen,
			); err != nil {
				return err
			}
			issues = append(issues, issue)
		}
		resp.Interfaces.Issues = issues
		return rows.Err()
	})

	// Non-activated devices
	g.Go(func() error {
		query := `
			SELECT
				d.pk,
				d.code,
				d.device_type,
				m.code as metro,
				d.status,
				formatDateTime(d.snapshot_ts, '%Y-%m-%dT%H:%i:%sZ', 'UTC') as since
			FROM dz_devices_current d
			JOIN dz_metros_current m ON d.metro_pk = m.pk
			WHERE d.status != 'activated'
			ORDER BY d.snapshot_ts DESC
			LIMIT 50
		`
		rows, err := envDB(ctx).Query(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		var devices []NonActivatedDevice
		for rows.Next() {
			var dev NonActivatedDevice
			if err := rows.Scan(&dev.PK, &dev.Code, &dev.DeviceType, &dev.Metro, &dev.Status, &dev.Since); err != nil {
				return err
			}
			devices = append(devices, dev)
		}
		resp.Alerts.Devices = devices
		return rows.Err()
	})

	// Drained and provisioning links
	g.Go(func() error {
		query := `
			SELECT
				l.pk,
				l.code,
				l.link_type,
				ma.code as side_a_metro,
				mz.code as side_z_metro,
				CASE WHEN l.committed_rtt_ns = ? THEN 'provisioning' ELSE l.status END as effective_status,
				formatDateTime(l.snapshot_ts, '%Y-%m-%dT%H:%i:%sZ', 'UTC') as since
			FROM dz_links_current l
			JOIN dz_devices_current da ON l.side_a_pk = da.pk
			JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			JOIN dz_metros_current ma ON da.metro_pk = ma.pk
			JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
			WHERE l.status IN ('soft-drained', 'hard-drained')
			   OR l.committed_rtt_ns = ?
			ORDER BY l.snapshot_ts DESC
			LIMIT 50
		`
		rows, err := envDB(ctx).Query(ctx, query, committedRttProvisioningNs, committedRttProvisioningNs)
		if err != nil {
			return err
		}
		defer rows.Close()

		var links []NonActivatedLink
		for rows.Next() {
			var link NonActivatedLink
			if err := rows.Scan(&link.PK, &link.Code, &link.LinkType, &link.SideAMetro, &link.SideZMetro, &link.Status, &link.Since); err != nil {
				return err
			}
			links = append(links, link)
		}
		resp.Alerts.Links = links
		return rows.Err()
	})

	// ISIS device issues (overload, unreachable)
	g.Go(func() error {
		query := `
			SELECT
				id.hostname as code,
				COALESCE(d.device_type, '') as device_type,
				COALESCE(m.code, '') as metro,
				CASE
					WHEN id.overload = 1 AND id.node_unreachable = 1 THEN 'unreachable'
					WHEN id.node_unreachable = 1 THEN 'unreachable'
					ELSE 'overload'
				END as issue,
				formatDateTime(id.snapshot_ts, '%Y-%m-%dT%H:%i:%sZ', 'UTC') as since
			FROM isis_devices_current id
			LEFT JOIN dz_devices_current d ON id.device_pk = d.pk AND id.device_pk != ''
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			WHERE id.overload = 1 OR id.node_unreachable = 1
			ORDER BY id.hostname
			LIMIT 50
		`
		rows, err := envDB(ctx).Query(ctx, query)
		if err != nil {
			slog.Warn("status: failed to query ISIS device issues", "error", err)
			return nil
		}
		defer rows.Close()

		var issues []ISISDeviceIssue
		for rows.Next() {
			var issue ISISDeviceIssue
			if err := rows.Scan(&issue.Code, &issue.DeviceType, &issue.Metro, &issue.Issue, &issue.Since); err != nil {
				return err
			}
			issues = append(issues, issue)
		}
		resp.Alerts.ISISDevices = issues
		return rows.Err()
	})

	// Missing ISIS adjacencies (activated links with tunnel_net but no ISIS adjacency)
	var missingAdjIssues []LinkIssue
	g.Go(func() error {
		query := `
			SELECT
				l.code,
				l.link_type,
				COALESCE(c.code, '') as contributor,
				COALESCE(ma.code, '') as side_a_metro,
				COALESCE(mz.code, '') as side_z_metro,
				l.pk as link_pk
			FROM dz_links_current l
			LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
			LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
			LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
			LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
			WHERE l.status = 'activated'
			  AND l.tunnel_net != ''
			  AND l.committed_rtt_ns != ?
			  AND l.pk NOT IN (
			    SELECT DISTINCT link_pk
			    FROM isis_adjacencies_current
			    WHERE link_pk != ''
			  )
			  AND l.tunnel_net NOT IN (
			    SELECT DISTINCT l2.tunnel_net
			    FROM dz_links_current l2
			    JOIN isis_adjacencies_current a ON a.link_pk = l2.pk
			    WHERE l2.tunnel_net != '' AND a.link_pk != ''
			  )
			ORDER BY l.code
			LIMIT 50
		`
		rows, err := envDB(ctx).Query(ctx, query, committedRttProvisioningNs)
		if err != nil {
			slog.Warn("status: failed to query missing ISIS adjacencies", "error", err)
			return nil
		}
		defer rows.Close()

		type missingAdj struct {
			code, linkType, contributor, sideAMetro, sideZMetro, linkPK string
		}
		var missing []missingAdj
		for rows.Next() {
			var m missingAdj
			if err := rows.Scan(&m.code, &m.linkType, &m.contributor, &m.sideAMetro, &m.sideZMetro, &m.linkPK); err != nil {
				return err
			}
			missing = append(missing, m)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		// Find when each adjacency was last seen to compute accurate "since" time.
		// The last snapshot where is_deleted=0 is when the adjacency was still up.
		lastSeen := make(map[string]string) // link_pk -> ISO timestamp
		if len(missing) > 0 {
			linkPKs := make([]string, len(missing))
			for i, m := range missing {
				linkPKs[i] = m.linkPK
			}
			sinceQuery := `
				SELECT
					link_pk,
					formatDateTime(max(snapshot_ts), '%Y-%m-%dT%H:%i:%sZ', 'UTC') as last_seen
				FROM dim_isis_adjacencies_history
				WHERE link_pk IN ?
				  AND is_deleted = 0
				GROUP BY link_pk
			`
			sinceRows, sinceErr := envDB(ctx).Query(ctx, sinceQuery, linkPKs)
			if sinceErr == nil {
				defer sinceRows.Close()
				for sinceRows.Next() {
					var pk, ts string
					if err := sinceRows.Scan(&pk, &ts); err == nil {
						lastSeen[pk] = ts
					}
				}
			}
		}

		for _, m := range missing {
			missingAdjIssues = append(missingAdjIssues, LinkIssue{
				Code:        m.code,
				LinkType:    m.linkType,
				Contributor: m.contributor,
				Issue:       "missing_adjacency",
				SideAMetro:  m.sideAMetro,
				SideZMetro:  m.sideZMetro,
				Since:       lastSeen[m.linkPK],
				IsDown:      true,
			})
		}
		return nil
	})

	err := g.Wait()

	// Merge missing adjacency issues after all goroutines complete (avoids race on resp.Links.Issues)
	// Also update the Down count for links that weren't already counted during the latency scan.
	existingIssueCodes := make(map[string]bool)
	for _, issue := range resp.Links.Issues {
		if issue.IsDown {
			existingIssueCodes[issue.Code] = true
		}
	}
	for _, issue := range missingAdjIssues {
		if !existingIssueCodes[issue.Code] {
			resp.Links.Down++
			if resp.Links.Healthy > 0 {
				resp.Links.Healthy--
			}
		}
	}
	resp.Links.Issues = append(resp.Links.Issues, missingAdjIssues...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		resp.Error = err.Error()
	}

	// Determine overall status
	resp.Status = determineOverallStatus(resp)

	return resp
}

// Link history types for status timeline
type LinkHourStatus struct {
	Hour         string  `json:"hour"`
	Status       string  `json:"status"`                 // "healthy", "degraded", "unhealthy", "no_data"
	Collecting   bool    `json:"collecting,omitempty"`   // true for the current incomplete bucket
	DrainStatus  string  `json:"drain_status,omitempty"` // "", "soft-drained", "hard-drained"
	AvgLatencyUs float64 `json:"avg_latency_us"`
	AvgLossPct   float64 `json:"avg_loss_pct"`
	Samples      uint64  `json:"samples"`
	// Per-side latency/loss metrics (direction: A→Z vs Z→A)
	SideALatencyUs float64 `json:"side_a_latency_us,omitempty"`
	SideALossPct   float64 `json:"side_a_loss_pct,omitempty"`
	SideASamples   uint64  `json:"side_a_samples,omitempty"`
	SideZLatencyUs float64 `json:"side_z_latency_us,omitempty"`
	SideZLossPct   float64 `json:"side_z_loss_pct,omitempty"`
	SideZSamples   uint64  `json:"side_z_samples,omitempty"`
	// Per-side interface issues (errors, discards, carrier transitions)
	SideAInErrors           uint64 `json:"side_a_in_errors,omitempty"`
	SideAOutErrors          uint64 `json:"side_a_out_errors,omitempty"`
	SideAInFcsErrors        uint64 `json:"side_a_in_fcs_errors,omitempty"`
	SideAInDiscards         uint64 `json:"side_a_in_discards,omitempty"`
	SideAOutDiscards        uint64 `json:"side_a_out_discards,omitempty"`
	SideACarrierTransitions uint64 `json:"side_a_carrier_transitions,omitempty"`
	SideZInErrors           uint64 `json:"side_z_in_errors,omitempty"`
	SideZOutErrors          uint64 `json:"side_z_out_errors,omitempty"`
	SideZInFcsErrors        uint64 `json:"side_z_in_fcs_errors,omitempty"`
	SideZInDiscards         uint64 `json:"side_z_in_discards,omitempty"`
	SideZOutDiscards        uint64 `json:"side_z_out_discards,omitempty"`
	SideZCarrierTransitions uint64 `json:"side_z_carrier_transitions,omitempty"`
	// Utilization (traffic rate / capacity)
	UtilizationInPct  float64 `json:"utilization_in_pct,omitempty"`
	UtilizationOutPct float64 `json:"utilization_out_pct,omitempty"`
	// ISIS state
	ISISDown bool `json:"isis_down,omitempty"` // true when link has no ISIS adjacency in this bucket
}

type LinkHistory struct {
	PK             string           `json:"pk"`
	Code           string           `json:"code"`
	LinkType       string           `json:"link_type"`
	Contributor    string           `json:"contributor"`
	SideAMetro     string           `json:"side_a_metro"`
	SideZMetro     string           `json:"side_z_metro"`
	SideADevice    string           `json:"side_a_device"`
	SideZDevice    string           `json:"side_z_device"`
	BandwidthBps   int64            `json:"bandwidth_bps"`
	CommittedRttUs float64          `json:"committed_rtt_us"`
	IsDown         bool             `json:"is_down"`
	DrainStatus    string           `json:"drain_status,omitempty"`
	Provisioning   bool             `json:"provisioning,omitempty"`
	Hours          []LinkHourStatus `json:"hours"`
	IssueReasons   []string         `json:"issue_reasons"` // "packet_loss", "high_latency", "no_data", "missing_adjacency", "interface_errors", "discards", "carrier_transitions", "high_utilization"
}

type LinkHistoryResponse struct {
	Links         []LinkHistory `json:"links"`
	TimeRange     string        `json:"time_range"`     // "24h", "3d", "7d"
	BucketMinutes int           `json:"bucket_minutes"` // Size of each bucket in minutes
	BucketCount   int           `json:"bucket_count"`   // Number of buckets
	Error         string        `json:"error,omitempty"`
}

func GetLinkHistory(w http.ResponseWriter, r *http.Request) {
	// Parse time range parameter
	timeRange := r.URL.Query().Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}

	// Parse optional bucket count (for responsive display)
	requestedBuckets := 72 // default
	if b := r.URL.Query().Get("buckets"); b != "" {
		if n, err := strconv.Atoi(b); err == nil && n >= 12 && n <= 10000 {
			requestedBuckets = n
		}
	}

	// Try to serve from cache first (cache only holds mainnet data)
	if isMainnet(r.Context()) && pageCache != nil {
		if cached := pageCache.GetLinkHistory(timeRange, requestedBuckets); cached != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			if err := json.NewEncoder(w).Encode(cached); err != nil {
				slog.Error("failed to encode response", "error", err)
			}
			return
		}
	}

	// Cache miss - fetch fresh data
	w.Header().Set("X-Cache", "MISS")
	timeout := 20 * time.Second
	switch timeRange {
	case "3d":
		timeout = 40 * time.Second
	case "7d":
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	resp, err := fetchLinkHistoryData(ctx, timeRange, requestedBuckets)
	if err != nil {
		if dberror.IsTransient(err) {
			cancel()
			var retryCancel context.CancelFunc
			ctx, retryCancel = context.WithTimeout(r.Context(), timeout)
			defer retryCancel()
			resp, err = fetchLinkHistoryData(ctx, timeRange, requestedBuckets)
		}
	}
	if err != nil {
		slog.Warn("link history query failed", "error", err)
		http.Error(w, "Failed to fetch link history", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

// snapBucketMinutes rounds the bucket size to a clean interval that divides
// evenly into hours for readable bucket boundaries.
func snapBucketMinutes(raw int) int {
	if raw < 5 {
		return 5
	}
	clean := []int{240, 180, 120, 60, 30, 20, 15, 10, 5}
	for _, c := range clean {
		if raw >= c {
			return c
		}
	}
	return 5
}

// fetchLinkHistoryData performs the actual link history data fetch from the database.
// This is called by both the cache refresh and direct requests.
func fetchLinkHistoryData(ctx context.Context, timeRange string, requestedBuckets int) (*LinkHistoryResponse, error) {
	start := time.Now()

	// Configure bucket size based on time range and requested bucket count
	var totalMinutes int
	switch timeRange {
	case "1h":
		totalMinutes = 60
	case "3h":
		totalMinutes = 3 * 60
	case "6h":
		totalMinutes = 6 * 60
	case "12h":
		totalMinutes = 12 * 60
	case "3d":
		totalMinutes = 3 * 24 * 60
	case "7d":
		totalMinutes = 7 * 24 * 60
	default: // "24h"
		timeRange = "24h"
		totalMinutes = 24 * 60
	}

	// Calculate bucket size to fit requested number of buckets
	bucketMinutes := snapBucketMinutes(totalMinutes / requestedBuckets)
	bucketCount := totalMinutes / bucketMinutes
	totalHours := totalMinutes / 60

	// Build the bucket interval expression
	var bucketInterval string
	if bucketMinutes >= 60 && bucketMinutes%60 == 0 {
		bucketInterval = fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %d HOUR, 'UTC')", bucketMinutes/60)
	} else {
		bucketInterval = fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %d MINUTE, 'UTC')", bucketMinutes)
	}

	// Get all WAN links with their metadata
	linkQuery := `
		SELECT
			l.pk,
			l.code,
			l.link_type,
			COALESCE(c.code, '') as contributor,
			ma.code as side_a_metro,
			mz.code as side_z_metro,
			da.code as side_a_device,
			dz.code as side_z_device,
			l.bandwidth_bps,
			l.committed_rtt_ns / 1000.0 as committed_rtt_us,
			l.committed_rtt_ns,
			l.isis_delay_override_ns,
			l.status
		FROM dz_links_current l
		JOIN dz_devices_current da ON l.side_a_pk = da.pk
		JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		JOIN dz_metros_current ma ON da.metro_pk = ma.pk
		JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
		LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
		WHERE l.status IN ('activated', 'soft-drained', 'hard-drained')
	`

	linkRows, err := envDB(ctx).Query(ctx, linkQuery)
	if err != nil {
		return nil, fmt.Errorf("link history query error: %w", err)
	}
	defer linkRows.Close()

	// Build map of link metadata
	type linkMeta struct {
		code            string
		linkType        string
		contributor     string
		sideAMetro      string
		sideZMetro      string
		sideADevice     string
		sideZDevice     string
		bandwidthBps    int64
		committedRttUs  float64
		committedRttNs  int64
		delayOverrideNs int64
		status          string
	}
	linkMap := make(map[string]linkMeta)

	for linkRows.Next() {
		var pk string
		var meta linkMeta
		if err := linkRows.Scan(&pk, &meta.code, &meta.linkType, &meta.contributor, &meta.sideAMetro, &meta.sideZMetro, &meta.sideADevice, &meta.sideZDevice, &meta.bandwidthBps, &meta.committedRttUs, &meta.committedRttNs, &meta.delayOverrideNs, &meta.status); err != nil {
			return nil, fmt.Errorf("link scan error: %w", err)
		}
		linkMap[pk] = meta
	}
	if err := linkRows.Err(); err != nil {
		return nil, fmt.Errorf("link rows iteration error: %w", err)
	}

	// Get stats for the configured time range, grouped by direction (A→Z vs Z→A).
	// Loss is computed as the max of 5-minute sub-bucket loss percentages within each
	// display bucket, matching Grafana's [5m] window for sharper spike visibility.
	lossBucketInterval := fmt.Sprintf("toStartOfInterval(f.event_ts, INTERVAL %d MINUTE, 'UTC')", min(bucketMinutes, 5))
	// Include the current (possibly incomplete) bucket — the frontend marks it as "collecting".
	timeFilterExpr := fmt.Sprintf("f.event_ts > now() - INTERVAL %d HOUR", totalHours)
	historyQuery := `
		WITH loss_sub AS (
			SELECT
				f.link_pk,
				` + bucketInterval + ` as display_bucket,
				if(f.origin_device_pk = l.side_a_pk, 'A', 'Z') as direction,
				countIf(f.loss OR f.rtt_us = 0) * 100.0 / count(*) as loss_pct
			FROM fact_dz_device_link_latency f
			JOIN dz_links_current l ON f.link_pk = l.pk
			WHERE ` + timeFilterExpr + `
			GROUP BY f.link_pk, display_bucket, direction, ` + lossBucketInterval + `
		),
		loss_max AS (
			SELECT link_pk, display_bucket, direction, max(loss_pct) as loss_pct
			FROM loss_sub
			GROUP BY link_pk, display_bucket, direction
		)
		SELECT
			f.link_pk,
			` + bucketInterval + ` as bucket,
			if(f.origin_device_pk = l.side_a_pk, 'A', 'Z') as direction,
			avg(f.rtt_us) as avg_latency,
			max(lm.loss_pct) as loss_pct,
			count(*) as samples
		FROM fact_dz_device_link_latency f
		JOIN dz_links_current l ON f.link_pk = l.pk
		LEFT JOIN loss_max lm ON f.link_pk = lm.link_pk
			AND ` + bucketInterval + ` = lm.display_bucket
			AND if(f.origin_device_pk = l.side_a_pk, 'A', 'Z') = lm.direction
		WHERE ` + timeFilterExpr + `
		GROUP BY f.link_pk, bucket, direction
		ORDER BY f.link_pk, bucket, direction
	`

	historyRows, err := envDB(ctx).Query(ctx, historyQuery)
	if err != nil {
		return nil, fmt.Errorf("link history stats query error: %w", err)
	}
	defer historyRows.Close()

	// Build bucket stats per link with per-side breakdown
	type sideStats struct {
		avgLatency float64
		lossPct    float64
		samples    uint64
	}
	type bucketStats struct {
		bucket     time.Time
		avgLatency float64
		lossPct    float64
		samples    uint64
		sideA      *sideStats
		sideZ      *sideStats
	}
	// Use a nested map: linkPK -> bucket time string -> bucketStats
	linkBucketMap := make(map[string]map[string]*bucketStats)

	for historyRows.Next() {
		var linkPK, direction string
		var bucket time.Time
		var avgLatency, lossPct float64
		var samples uint64
		if err := historyRows.Scan(&linkPK, &bucket, &direction, &avgLatency, &lossPct, &samples); err != nil {
			return nil, fmt.Errorf("history scan error: %w", err)
		}

		bucketKey := bucket.UTC().Format(time.RFC3339)
		if linkBucketMap[linkPK] == nil {
			linkBucketMap[linkPK] = make(map[string]*bucketStats)
		}
		if linkBucketMap[linkPK][bucketKey] == nil {
			linkBucketMap[linkPK][bucketKey] = &bucketStats{bucket: bucket}
		}
		stats := linkBucketMap[linkPK][bucketKey]

		// Store per-side stats
		if direction == "A" {
			stats.sideA = &sideStats{avgLatency: avgLatency, lossPct: lossPct, samples: samples}
		} else {
			stats.sideZ = &sideStats{avgLatency: avgLatency, lossPct: lossPct, samples: samples}
		}
	}
	if err := historyRows.Err(); err != nil {
		return nil, fmt.Errorf("history rows iteration error: %w", err)
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("context cancelled during history query: %w", ctx.Err())
	}
	// If we have links but the history query returned nothing, the query likely failed silently
	if len(linkMap) > 0 && len(linkBucketMap) == 0 {
		return nil, fmt.Errorf("history query returned no data for %d links (range=%s) — likely timed out", len(linkMap), timeRange)
	}

	// Compute aggregate stats for each bucket (combining both directions).
	// Latency is sample-weighted averaged; loss uses the max of either direction
	// so one-sided packet loss isn't diluted by the healthy side's samples.
	linkBuckets := make(map[string][]bucketStats)
	for linkPK, bucketMap := range linkBucketMap {
		var buckets []bucketStats
		for _, stats := range bucketMap {
			var totalLatency float64
			var totalSamples uint64
			var maxLoss float64

			if stats.sideA != nil {
				totalLatency += stats.sideA.avgLatency * float64(stats.sideA.samples)
				totalSamples += stats.sideA.samples
				if stats.sideA.lossPct > maxLoss {
					maxLoss = stats.sideA.lossPct
				}
			}
			if stats.sideZ != nil {
				totalLatency += stats.sideZ.avgLatency * float64(stats.sideZ.samples)
				totalSamples += stats.sideZ.samples
				if stats.sideZ.lossPct > maxLoss {
					maxLoss = stats.sideZ.lossPct
				}
			}

			if totalSamples > 0 {
				stats.avgLatency = totalLatency / float64(totalSamples)
				stats.lossPct = maxLoss
				stats.samples = totalSamples
			}
			buckets = append(buckets, *stats)
		}
		linkBuckets[linkPK] = buckets
	}

	// Get interface issues per link per bucket (grouped by side)
	// Use greatest(0, delta) to ignore negative deltas (counter resets from device restarts)
	interfaceQuery := `
		SELECT
			link_pk,
			link_side,
			` + bucketInterval + ` as bucket,
			toUInt64(SUM(greatest(0, in_errors_delta))) as in_errors,
			toUInt64(SUM(greatest(0, out_errors_delta))) as out_errors,
			toUInt64(SUM(greatest(0, in_fcs_errors_delta))) as in_fcs_errors,
			toUInt64(SUM(greatest(0, in_discards_delta))) as in_discards,
			toUInt64(SUM(greatest(0, out_discards_delta))) as out_discards,
			toUInt64(SUM(greatest(0, carrier_transitions_delta))) as carrier_transitions
		FROM fact_dz_device_interface_counters
		WHERE event_ts > now() - INTERVAL ? HOUR
		  AND link_pk != ''
		GROUP BY link_pk, link_side, bucket
		ORDER BY link_pk, link_side, bucket
	`

	interfaceRows, err := envDB(ctx).Query(ctx, interfaceQuery, totalHours)
	if err != nil {
		return nil, fmt.Errorf("link interface query error: %w", err)
	}
	defer interfaceRows.Close()

	// Build interface stats per link per bucket per side
	type interfaceStats struct {
		inErrors           uint64
		outErrors          uint64
		inFcsErrors        uint64
		inDiscards         uint64
		outDiscards        uint64
		carrierTransitions uint64
	}
	type linkInterfaceBucketKey struct {
		linkPK string
		bucket string
	}
	linkInterfaceBuckets := make(map[linkInterfaceBucketKey]map[string]*interfaceStats) // key -> side -> stats

	for interfaceRows.Next() {
		var linkPK, linkSide string
		var bucket time.Time
		var inErrors, outErrors, inFcsErrors, inDiscards, outDiscards, carrierTransitions uint64
		if err := interfaceRows.Scan(&linkPK, &linkSide, &bucket, &inErrors, &outErrors, &inFcsErrors, &inDiscards, &outDiscards, &carrierTransitions); err != nil {
			return nil, fmt.Errorf("interface scan error: %w", err)
		}
		bucketKey := bucket.UTC().Format(time.RFC3339)
		key := linkInterfaceBucketKey{linkPK: linkPK, bucket: bucketKey}
		if linkInterfaceBuckets[key] == nil {
			linkInterfaceBuckets[key] = make(map[string]*interfaceStats)
		}
		linkInterfaceBuckets[key][linkSide] = &interfaceStats{
			inErrors:           inErrors,
			outErrors:          outErrors,
			inFcsErrors:        inFcsErrors,
			inDiscards:         inDiscards,
			outDiscards:        outDiscards,
			carrierTransitions: carrierTransitions,
		}
	}
	if err := interfaceRows.Err(); err != nil {
		return nil, fmt.Errorf("interface rows iteration error: %w", err)
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("context cancelled during interface query: %w", ctx.Err())
	}

	// Get utilization per link per bucket (traffic rate / capacity)
	utilizationQuery := `
		SELECT
			link_pk,
			` + bucketInterval + ` as bucket,
			quantile(0.95)(CASE WHEN delta_duration > 0 THEN in_octets_delta * 8 / delta_duration ELSE 0 END) as in_bps,
			quantile(0.95)(CASE WHEN delta_duration > 0 THEN out_octets_delta * 8 / delta_duration ELSE 0 END) as out_bps
		FROM fact_dz_device_interface_counters
		WHERE event_ts > now() - INTERVAL ? HOUR
		  AND link_pk != ''
		  AND delta_duration > 0
		  AND in_octets_delta >= 0
		  AND out_octets_delta >= 0
		GROUP BY link_pk, bucket
		ORDER BY link_pk, bucket
	`

	utilizationRows, err := envDB(ctx).Query(ctx, utilizationQuery, totalHours)
	if err != nil {
		return nil, fmt.Errorf("link utilization query error: %w", err)
	}
	defer utilizationRows.Close()

	// Build utilization stats per link per bucket
	type utilizationStats struct {
		inBps  float64
		outBps float64
	}
	linkUtilizationBuckets := make(map[string]map[string]*utilizationStats) // linkPK -> bucket -> stats

	for utilizationRows.Next() {
		var linkPK string
		var bucket time.Time
		var inBps, outBps float64
		if err := utilizationRows.Scan(&linkPK, &bucket, &inBps, &outBps); err != nil {
			return nil, fmt.Errorf("utilization scan error: %w", err)
		}
		bucketKey := bucket.UTC().Format(time.RFC3339)
		if linkUtilizationBuckets[linkPK] == nil {
			linkUtilizationBuckets[linkPK] = make(map[string]*utilizationStats)
		}
		linkUtilizationBuckets[linkPK][bucketKey] = &utilizationStats{
			inBps:  inBps,
			outBps: outBps,
		}
	}
	if err := utilizationRows.Err(); err != nil {
		return nil, fmt.Errorf("utilization rows iteration error: %w", err)
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("context cancelled during utilization query: %w", ctx.Err())
	}

	// Get historical link status per bucket from dim_dz_links_history
	// This tells us if a link was drained at each point in time
	// Build bucket interval for snapshot_ts (history table uses snapshot_ts, not event_ts)
	var historyBucketInterval string
	if bucketMinutes >= 60 && bucketMinutes%60 == 0 {
		historyBucketInterval = fmt.Sprintf("toStartOfInterval(snapshot_ts, INTERVAL %d HOUR, 'UTC')", bucketMinutes/60)
	} else {
		historyBucketInterval = fmt.Sprintf("toStartOfInterval(snapshot_ts, INTERVAL %d MINUTE, 'UTC')", bucketMinutes)
	}

	statusHistoryQuery := `
		SELECT
			pk as link_pk,
			` + historyBucketInterval + ` as bucket,
			argMax(status, snapshot_ts) as status
		FROM dim_dz_links_history
		WHERE snapshot_ts > now() - INTERVAL ? HOUR
		GROUP BY link_pk, bucket
		ORDER BY link_pk, bucket
	`

	statusRows, err := safeQueryRows(ctx, statusHistoryQuery, totalHours)
	if err != nil {
		slog.Error("link status history query error", "error", err)
	}

	// Also fetch the most recent status BEFORE the time range for each link.
	// This serves as the baseline for carry-forward when the drain happened before the window.
	baselineQuery := `
		SELECT pk as link_pk, argMax(status, snapshot_ts) as status
		FROM dim_dz_links_history
		WHERE snapshot_ts <= now() - INTERVAL ? HOUR
		GROUP BY link_pk
	`
	baselineRows, baselineErr := safeQueryRows(ctx, baselineQuery, totalHours)
	if baselineErr != nil {
		slog.Warn("link status baseline query error", "error", baselineErr)
	}
	linkBaselineStatus := make(map[string]string) // linkPK -> status before time range
	if baselineRows != nil {
		defer baselineRows.Close()
		for baselineRows.Next() {
			var linkPK, status string
			if err := baselineRows.Scan(&linkPK, &status); err != nil {
				slog.Error("baseline scan error", "error", err)
				break
			}
			linkBaselineStatus[linkPK] = status
		}
		if err := baselineRows.Err(); err != nil {
			slog.Error("baseline rows iteration error", "error", err)
		}
	}

	// Build per-link sorted history of statuses, keyed by bucket time string
	// Since history is sparse (only records transitions), we need to carry forward
	// the last known status for buckets without entries.
	type statusEntry struct {
		bucket string
		status string
	}
	linkStatusEntries := make(map[string][]statusEntry) // linkPK -> sorted entries

	if statusRows != nil {
		defer statusRows.Close()
		for statusRows.Next() {
			var linkPK, status string
			var bucket time.Time
			if err := statusRows.Scan(&linkPK, &bucket, &status); err != nil {
				return nil, fmt.Errorf("status history scan error: %w", err)
			}
			key := bucket.UTC().Format(time.RFC3339)
			linkStatusEntries[linkPK] = append(linkStatusEntries[linkPK], statusEntry{bucket: key, status: status})
		}
		if err := statusRows.Err(); err != nil {
			return nil, fmt.Errorf("status rows iteration error: %w", err)
		}
	}

	// Build a fast lookup map and also sort entries for carry-forward
	type linkBucketKey struct {
		linkPK string
		bucket string
	}
	linkStatusHistory := make(map[linkBucketKey]string)
	for linkPK, entries := range linkStatusEntries {
		// Sort by bucket time
		sort.Slice(entries, func(i, j int) bool { return entries[i].bucket < entries[j].bucket })
		linkStatusEntries[linkPK] = entries
		for _, e := range entries {
			linkStatusHistory[linkBucketKey{linkPK: linkPK, bucket: e.bucket}] = e.status
		}
	}

	// Query is_down from dz_links_health_current
	downLinkPKs := make(map[string]bool)
	downQuery := `SELECT pk FROM dz_links_health_current WHERE is_down = true`
	downRows, downErr := envDB(ctx).Query(ctx, downQuery)
	if downErr != nil {
		slog.Warn("is_down query error", "error", downErr)
	} else {
		defer downRows.Close()
		for downRows.Next() {
			var pk string
			if err := downRows.Scan(&pk); err != nil {
				slog.Error("is_down scan error", "error", err)
				break
			}
			downLinkPKs[pk] = true
		}
		if err := downRows.Err(); err != nil {
			slog.Error("is_down rows iteration error", "error", err)
		}
	}

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	// Query link PKs missing ISIS adjacencies (for marking as missing_adjacency issue)
	missingISISLinkPKs := make(map[string]bool)
	missingISISQuery := `
		SELECT l.pk
		FROM dz_links_current l
		WHERE l.status = 'activated'
		  AND l.tunnel_net != ''
		  AND l.committed_rtt_ns != ?
		  AND l.pk NOT IN (
		    SELECT DISTINCT link_pk
		    FROM isis_adjacencies_current
		    WHERE link_pk != ''
		  )
		  AND l.tunnel_net NOT IN (
		    SELECT DISTINCT l2.tunnel_net
		    FROM dz_links_current l2
		    JOIN isis_adjacencies_current a ON a.link_pk = l2.pk
		    WHERE l2.tunnel_net != '' AND a.link_pk != ''
		  )
	`
	missingRows, missingErr := envDB(ctx).Query(ctx, missingISISQuery, committedRttProvisioningNs)
	if missingErr != nil {
		slog.Warn("link history: failed to query missing ISIS adjacencies", "error", missingErr)
	} else {
		defer missingRows.Close()
		for missingRows.Next() {
			var pk string
			if err := missingRows.Scan(&pk); err == nil {
				missingISISLinkPKs[pk] = true
			}
		}
	}

	// Query ISIS adjacency history per link per bucket
	// For each link_pk, find whether an ISIS adjacency existed at each bucket
	// by looking at the SCD2 history (is_deleted=0 means adjacency exists, is_deleted=1 means gone)
	type isisAdjEntry struct {
		bucket    string
		isDeleted bool
	}
	isisAdjEntries := make(map[string][]isisAdjEntry) // link_pk -> sorted entries
	isisAdjHistory := make(map[linkBucketKey]bool)    // link_pk+bucket -> is_deleted

	isisAdjHistQuery := `
		SELECT
			link_pk,
			` + historyBucketInterval + ` as bucket,
			argMax(is_deleted, snapshot_ts) as is_deleted
		FROM dim_isis_adjacencies_history
		WHERE snapshot_ts > now() - INTERVAL ? HOUR
		  AND link_pk != ''
		GROUP BY link_pk, bucket
		ORDER BY link_pk, bucket
	`
	isisAdjHistRows, isisAdjHistErr := safeQueryRows(ctx, isisAdjHistQuery, totalHours)
	if isisAdjHistErr != nil {
		slog.Warn("link history: failed to query ISIS adjacency history", "error", isisAdjHistErr)
	}
	if isisAdjHistRows != nil {
		defer isisAdjHistRows.Close()
		for isisAdjHistRows.Next() {
			var linkPK string
			var bucket time.Time
			var isDeleted uint8
			if err := isisAdjHistRows.Scan(&linkPK, &bucket, &isDeleted); err != nil {
				slog.Error("ISIS adjacency history scan error", "error", err)
				break
			}
			key := bucket.UTC().Format(time.RFC3339)
			deleted := isDeleted == 1
			isisAdjEntries[linkPK] = append(isisAdjEntries[linkPK], isisAdjEntry{bucket: key, isDeleted: deleted})
			isisAdjHistory[linkBucketKey{linkPK: linkPK, bucket: key}] = deleted
		}
	}

	// Baseline: ISIS adjacency state before the time range for each link
	isisAdjBaseline := make(map[string]bool) // link_pk -> is_deleted (true = no adjacency)
	isisAdjBaselineQuery := `
		SELECT link_pk, argMax(is_deleted, snapshot_ts) as is_deleted
		FROM dim_isis_adjacencies_history
		WHERE snapshot_ts <= now() - INTERVAL ? HOUR
		  AND link_pk != ''
		GROUP BY link_pk
	`
	isisAdjBaselineRows, isisAdjBaselineErr := safeQueryRows(ctx, isisAdjBaselineQuery, totalHours)
	if isisAdjBaselineErr != nil {
		slog.Warn("ISIS adjacency baseline query error", "error", isisAdjBaselineErr)
	}
	if isisAdjBaselineRows != nil {
		defer isisAdjBaselineRows.Close()
		for isisAdjBaselineRows.Next() {
			var linkPK string
			var isDeleted uint8
			if err := isisAdjBaselineRows.Scan(&linkPK, &isDeleted); err == nil {
				isisAdjBaseline[linkPK] = isDeleted == 1
			}
		}
	}

	// Set of link_pks that have any ISIS adjacency data at all (history or current)
	isisAdjKnownLinks := make(map[string]bool)
	for linkPK := range isisAdjEntries {
		isisAdjKnownLinks[linkPK] = true
	}
	for linkPK := range isisAdjBaseline {
		isisAdjKnownLinks[linkPK] = true
	}

	// Build response with all buckets for each link
	now := time.Now().UTC()
	bucketDuration := time.Duration(bucketMinutes) * time.Minute
	var links []LinkHistory

	// 1000ms delay override in nanoseconds indicates soft-drained
	const delayOverrideSoftDrainedNs = 1_000_000_000

	for pk, meta := range linkMap {
		// Determine current drain status
		var currentDrainStatus string
		if meta.status == "soft-drained" || meta.status == "hard-drained" {
			currentDrainStatus = meta.status
		} else if meta.delayOverrideNs == delayOverrideSoftDrainedNs {
			currentDrainStatus = "soft-drained"
		}
		isProvisioning := meta.committedRttNs == committedRttProvisioningNs

		// Track issue reasons for this link
		issueReasons := make(map[string]bool)

		// Check if this link has any issues in the time range
		buckets := linkBuckets[pk]

		// Check latency/loss issues (skip buckets where link was drained)
		for _, b := range buckets {
			bucketKey := b.bucket.UTC().Format(time.RFC3339)
			histKey := linkBucketKey{linkPK: pk, bucket: bucketKey}
			if hs, ok := linkStatusHistory[histKey]; ok && (hs == "soft-drained" || hs == "hard-drained") {
				continue
			}

			// Check for packet loss issues
			if b.lossPct >= LossWarningPct {
				issueReasons["packet_loss"] = true
			}
			// Check for high latency issues (WAN links only, excluding intra-metro)
			isInterMetro := meta.sideAMetro != meta.sideZMetro
			if meta.linkType == "WAN" && isInterMetro && meta.committedRttUs > 0 && b.avgLatency > 0 {
				latencyOveragePct := ((b.avgLatency - meta.committedRttUs) / meta.committedRttUs) * 100
				if latencyOveragePct >= LatencyWarningPct {
					issueReasons["high_latency"] = true
				}
			}
		}

		// Top-level drain status for this link
		linkDrainStatus := currentDrainStatus

		// Include all links (both healthy and those with issues)

		// Build bucket status array
		bucketMap := make(map[string]bucketStats)
		for _, b := range buckets {
			key := b.bucket.UTC().Format(time.RFC3339)
			bucketMap[key] = b
		}

		// Build a function to resolve drained status per bucket for this link.
		// History is sparse (only records transitions), so for buckets without entries,
		// we carry forward the most recent known status before that bucket.
		entries := linkStatusEntries[pk]
		isDrainedStatus := func(s string) bool {
			return s == "soft-drained" || s == "hard-drained"
		}
		// Returns the drain status string ("soft-drained", "hard-drained") or "" if not drained.
		resolveDrainStatus := func(bucketKey string) string {
			// Direct hit in history
			if s, ok := linkStatusHistory[linkBucketKey{linkPK: pk, bucket: bucketKey}]; ok {
				if isDrainedStatus(s) {
					return s
				}
				return ""
			}
			// No direct hit — find the most recent entry before this bucket
			if len(entries) > 0 {
				idx := sort.Search(len(entries), func(i int) bool { return entries[i].bucket > bucketKey })
				if idx > 0 {
					if isDrainedStatus(entries[idx-1].status) {
						return entries[idx-1].status
					}
					return ""
				}
			}
			// No in-range entries before this bucket — fall back to baseline (last status before time range)
			if baseline, ok := linkBaselineStatus[pk]; ok {
				if isDrainedStatus(baseline) {
					return baseline
				}
				return ""
			}
			return ""
		}

		// Resolve whether this link is missing its ISIS adjacency at a given bucket.
		// Returns true if the link should have an adjacency but doesn't.
		// Only applies to links we know should have adjacencies (those that appear in
		// missingISISLinkPKs or have ISIS history data).
		isisAdjEntriesForLink := isisAdjEntries[pk]
		resolveISISDown := func(bucketKey string) bool {
			// Direct hit in history
			if isDeleted, ok := isisAdjHistory[linkBucketKey{linkPK: pk, bucket: bucketKey}]; ok {
				return isDeleted
			}
			// No direct hit — find the most recent entry before this bucket
			if len(isisAdjEntriesForLink) > 0 {
				idx := sort.Search(len(isisAdjEntriesForLink), func(i int) bool { return isisAdjEntriesForLink[i].bucket > bucketKey })
				if idx > 0 {
					return isisAdjEntriesForLink[idx-1].isDeleted
				}
			}
			// No in-range entries before this bucket — fall back to baseline
			if baseline, ok := isisAdjBaseline[pk]; ok {
				return baseline
			}
			// No ISIS history at all for this link — check if it's in the known set
			// If not known, it never had an adjacency, so it's "down"
			if !isisAdjKnownLinks[pk] {
				return missingISISLinkPKs[pk]
			}
			return false
		}

		var hourStatuses []LinkHourStatus
		for i := bucketCount - 1; i >= 0; i-- {
			bucketStart := now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
			key := bucketStart.UTC().Format(time.RFC3339)
			isCollecting := i == 0

			drainStatus := resolveDrainStatus(key)

			// Check if we have latency/traffic data for this bucket
			if stats, ok := bucketMap[key]; ok {
				// Only consider latency for inter-metro WAN links
				committedRtt := meta.committedRttUs
				if meta.linkType != "WAN" || meta.sideAMetro == meta.sideZMetro {
					committedRtt = 0
				}
				status := classifyLinkStatus(stats.avgLatency, stats.lossPct, committedRtt)

				// If only one direction is reporting, the missing side likely
				// can't send probes. For completed buckets, treat as unhealthy.
				// For the collecting bucket, treat as no_data since we don't
				// have enough data to classify health yet.
				// Skip for hard-drained links (fully offline), but still apply
				// for soft-drained links since their health color is visible.
				if drainStatus != "hard-drained" && (stats.sideA == nil) != (stats.sideZ == nil) {
					if isCollecting {
						status = "no_data"
					} else if status == "healthy" || status == "degraded" {
						status = "unhealthy"
					}
				}

				hourStatus := LinkHourStatus{
					Hour:         key,
					Status:       status,
					Collecting:   isCollecting,
					DrainStatus:  drainStatus,
					AvgLatencyUs: stats.avgLatency,
					AvgLossPct:   stats.lossPct,
					Samples:      stats.samples,
				}
				// Add per-side latency/loss metrics if available
				if stats.sideA != nil {
					hourStatus.SideALatencyUs = stats.sideA.avgLatency
					hourStatus.SideALossPct = stats.sideA.lossPct
					hourStatus.SideASamples = stats.sideA.samples
				}
				if stats.sideZ != nil {
					hourStatus.SideZLatencyUs = stats.sideZ.avgLatency
					hourStatus.SideZLossPct = stats.sideZ.lossPct
					hourStatus.SideZSamples = stats.sideZ.samples
				}
				// Add per-side interface issues if available
				intfKey := linkInterfaceBucketKey{linkPK: pk, bucket: key}
				hasErrors := false
				hasDiscards := false
				hasCarrier := false
				if intfBucket, ok := linkInterfaceBuckets[intfKey]; ok {
					if sideA, ok := intfBucket["A"]; ok {
						hourStatus.SideAInErrors = sideA.inErrors
						hourStatus.SideAOutErrors = sideA.outErrors
						hourStatus.SideAInFcsErrors = sideA.inFcsErrors
						hourStatus.SideAInDiscards = sideA.inDiscards
						hourStatus.SideAOutDiscards = sideA.outDiscards
						hourStatus.SideACarrierTransitions = sideA.carrierTransitions
						// Track issue reasons
						if sideA.inErrors > 0 || sideA.outErrors > 0 {
							issueReasons["interface_errors"] = true
							hasErrors = true
						}
						if sideA.inFcsErrors > 0 {
							issueReasons["fcs_errors"] = true
							hasErrors = true
						}
						if sideA.inDiscards > 0 || sideA.outDiscards > 0 {
							issueReasons["discards"] = true
							hasDiscards = true
						}
						if sideA.carrierTransitions > 0 {
							issueReasons["carrier_transitions"] = true
							hasCarrier = true
						}
					}
					if sideZ, ok := intfBucket["Z"]; ok {
						hourStatus.SideZInErrors = sideZ.inErrors
						hourStatus.SideZOutErrors = sideZ.outErrors
						hourStatus.SideZInFcsErrors = sideZ.inFcsErrors
						hourStatus.SideZInDiscards = sideZ.inDiscards
						hourStatus.SideZOutDiscards = sideZ.outDiscards
						hourStatus.SideZCarrierTransitions = sideZ.carrierTransitions
						// Track issue reasons
						if sideZ.inErrors > 0 || sideZ.outErrors > 0 {
							issueReasons["interface_errors"] = true
							hasErrors = true
						}
						if sideZ.inFcsErrors > 0 {
							issueReasons["fcs_errors"] = true
							hasErrors = true
						}
						if sideZ.inDiscards > 0 || sideZ.outDiscards > 0 {
							issueReasons["discards"] = true
							hasDiscards = true
						}
						if sideZ.carrierTransitions > 0 {
							issueReasons["carrier_transitions"] = true
							hasCarrier = true
						}
					}
				}
				// Upgrade status based on interface issues
				// Use same thresholds as device health: >= 100 = unhealthy, > 0 = degraded
				const InterfaceUnhealthyThreshold = uint64(100)
				totalErrors := hourStatus.SideAInErrors + hourStatus.SideAOutErrors + hourStatus.SideZInErrors + hourStatus.SideZOutErrors
				totalDiscards := hourStatus.SideAInDiscards + hourStatus.SideAOutDiscards + hourStatus.SideZInDiscards + hourStatus.SideZOutDiscards
				totalCarrier := hourStatus.SideACarrierTransitions + hourStatus.SideZCarrierTransitions

				if totalErrors >= InterfaceUnhealthyThreshold || totalDiscards >= InterfaceUnhealthyThreshold || totalCarrier >= InterfaceUnhealthyThreshold {
					if hourStatus.Status == "healthy" || hourStatus.Status == "degraded" {
						hourStatus.Status = "unhealthy"
					}
				} else if (hasErrors || hasDiscards || hasCarrier) && hourStatus.Status == "healthy" {
					hourStatus.Status = "degraded"
				}

				// Add utilization data if available
				if utilBuckets, ok := linkUtilizationBuckets[pk]; ok {
					if utilStats, ok := utilBuckets[key]; ok {
						if meta.bandwidthBps > 0 {
							hourStatus.UtilizationInPct = (utilStats.inBps / float64(meta.bandwidthBps)) * 100
							hourStatus.UtilizationOutPct = (utilStats.outBps / float64(meta.bandwidthBps)) * 100
							// Track high utilization (>80%) as an issue - marks as degraded
							const HighUtilizationThreshold = 80.0
							if hourStatus.UtilizationInPct > HighUtilizationThreshold || hourStatus.UtilizationOutPct > HighUtilizationThreshold {
								issueReasons["high_utilization"] = true
								if hourStatus.Status == "healthy" {
									hourStatus.Status = "degraded"
								}
							}
						}
					}
				}

				// Check ISIS adjacency state for this bucket
				isisDown := resolveISISDown(key)
				if isisDown {
					hourStatus.ISISDown = true
					issueReasons["missing_adjacency"] = true
				}

				hourStatuses = append(hourStatuses, hourStatus)
			} else {
				isisDown := resolveISISDown(key)
				hourStatuses = append(hourStatuses, LinkHourStatus{
					Hour:        key,
					Status:      "no_data",
					Collecting:  isCollecting,
					DrainStatus: drainStatus,
					ISISDown:    isisDown,
				})
			}
		}

		// Check if there are any no_data buckets (missing telemetry)
		// Also check for per-side missing data (one side has samples, the other doesn't)
		// Skip the collecting bucket since it may not have data yet
		for _, h := range hourStatuses {
			if h.Collecting {
				continue
			}
			if h.Status == "no_data" {
				issueReasons["no_data"] = true
				break
			}
			// Per-side: bucket has data overall but one side is missing
			if h.Samples > 0 && (h.SideASamples == 0 || h.SideZSamples == 0) {
				issueReasons["no_data"] = true
				break
			}
		}

		isDown := downLinkPKs[pk] && currentDrainStatus == ""

		// Mark link as down if currently missing ISIS adjacency
		if missingISISLinkPKs[pk] && currentDrainStatus == "" && !isProvisioning {
			isDown = true
		}

		// Convert issue reasons to slice (after all tracking is complete)
		var issueReasonsList []string
		for reason := range issueReasons {
			issueReasonsList = append(issueReasonsList, reason)
		}
		sort.Strings(issueReasonsList)

		// Only expose committed RTT for inter-metro WAN links (not DZX)
		// so the frontend doesn't apply latency classification to DZX links
		responseCommittedRtt := meta.committedRttUs
		if meta.linkType != "WAN" || meta.sideAMetro == meta.sideZMetro {
			responseCommittedRtt = 0
		}

		links = append(links, LinkHistory{
			PK:             pk,
			Code:           meta.code,
			LinkType:       meta.linkType,
			Contributor:    meta.contributor,
			SideAMetro:     meta.sideAMetro,
			SideZMetro:     meta.sideZMetro,
			SideADevice:    meta.sideADevice,
			SideZDevice:    meta.sideZDevice,
			BandwidthBps:   meta.bandwidthBps,
			CommittedRttUs: responseCommittedRtt,
			IsDown:         isDown,
			DrainStatus:    linkDrainStatus,
			Provisioning:   isProvisioning,
			Hours:          hourStatuses,
			IssueReasons:   issueReasonsList,
		})
	}

	// Sort links by code for consistent ordering
	sort.Slice(links, func(i, j int) bool {
		return links[i].Code < links[j].Code
	})

	resp := &LinkHistoryResponse{
		Links:         links,
		TimeRange:     timeRange,
		BucketMinutes: bucketMinutes,
		BucketCount:   bucketCount,
	}

	slog.Info("fetchLinkHistoryData completed", "duration", time.Since(start), "range", timeRange, "buckets", bucketCount, "links", len(links))

	return resp, nil
}

func classifyLinkStatus(avgLatency, lossPct, committedRttUs float64) string {
	// Calculate latency overage percentage vs committed RTT
	var latencyOveragePct float64
	if committedRttUs > 0 && avgLatency > 0 {
		latencyOveragePct = ((avgLatency - committedRttUs) / committedRttUs) * 100
	}

	// Classify based on thresholds
	if lossPct >= LossCriticalPct || latencyOveragePct >= LatencyCriticalPct {
		return "unhealthy"
	}
	if lossPct >= LossWarningPct || latencyOveragePct >= LatencyWarningPct {
		return "degraded"
	}
	return "healthy"
}

func determineOverallStatus(resp *StatusResponse) string {
	// Check critical issues
	if !resp.System.Database {
		return "unhealthy"
	}

	// Check link health (down links are treated like unhealthy for overall status)
	if resp.Links.Total > 0 {
		unhealthyPct := float64(resp.Links.Unhealthy+resp.Links.Down) / float64(resp.Links.Total) * 100
		degradedPct := float64(resp.Links.Degraded) / float64(resp.Links.Total) * 100

		if unhealthyPct > 10 {
			return "unhealthy"
		}
		if degradedPct > 20 || unhealthyPct > 0 {
			return "degraded"
		}
	}

	// Check performance
	if resp.Performance.AvgLossPercent >= LossCriticalPct {
		return "unhealthy"
	}
	if resp.Performance.AvgLossPercent >= LossWarningPct {
		return "degraded"
	}

	return "healthy"
}

// Device history types for status timeline
type DeviceHourStatus struct {
	Hour               string  `json:"hour"`
	Status             string  `json:"status"`               // "healthy", "degraded", "unhealthy", "no_data", "disabled"
	Collecting         bool    `json:"collecting,omitempty"` // true for the current incomplete bucket
	CurrentUsers       int32   `json:"current_users"`
	MaxUsers           int32   `json:"max_users"`
	UtilizationPct     float64 `json:"utilization_pct"`
	InErrors           uint64  `json:"in_errors"`
	OutErrors          uint64  `json:"out_errors"`
	InFcsErrors        uint64  `json:"in_fcs_errors"`
	InDiscards         uint64  `json:"in_discards"`
	OutDiscards        uint64  `json:"out_discards"`
	CarrierTransitions uint64  `json:"carrier_transitions"`
	DrainStatus        string  `json:"drain_status,omitempty"` // "", "soft-drained", "hard-drained", "suspended"
	NoProbes           bool    `json:"no_probes,omitempty"`    // true when device has interface data but no latency probes
	// ISIS state
	ISISOverload    bool `json:"isis_overload,omitempty"`    // true when device is in ISIS overload state
	ISISUnreachable bool `json:"isis_unreachable,omitempty"` // true when device is unreachable in ISIS topology
}

type DeviceHistory struct {
	PK           string             `json:"pk"`
	Code         string             `json:"code"`
	DeviceType   string             `json:"device_type"`
	Contributor  string             `json:"contributor"`
	Metro        string             `json:"metro"`
	MaxUsers     int32              `json:"max_users"`
	Hours        []DeviceHourStatus `json:"hours"`
	IssueReasons []string           `json:"issue_reasons"` // "interface_errors", "discards", "carrier_transitions", "drained", "isis_overload", "isis_unreachable"
}

type DeviceHistoryResponse struct {
	Devices       []DeviceHistory `json:"devices"`
	TimeRange     string          `json:"time_range"`
	BucketMinutes int             `json:"bucket_minutes"`
	BucketCount   int             `json:"bucket_count"`
}

func GetDeviceHistory(w http.ResponseWriter, r *http.Request) {
	// Parse time range parameter
	timeRange := r.URL.Query().Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}

	// Parse optional bucket count (for responsive display)
	requestedBuckets := 72 // default
	if b := r.URL.Query().Get("buckets"); b != "" {
		if n, err := strconv.Atoi(b); err == nil && n >= 12 && n <= 10000 {
			requestedBuckets = n
		}
	}

	// Try to serve from cache first (cache only holds mainnet data)
	if isMainnet(r.Context()) && pageCache != nil {
		if cached := pageCache.GetDeviceHistory(timeRange, requestedBuckets); cached != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			if err := json.NewEncoder(w).Encode(cached); err != nil {
				slog.Error("failed to encode response", "error", err)
			}
			return
		}
	}

	// Cache miss - fetch fresh data
	w.Header().Set("X-Cache", "MISS")
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	resp, err := fetchDeviceHistoryData(ctx, timeRange, requestedBuckets)
	if err != nil {
		slog.Error("fetchDeviceHistoryData error", "error", err)
		http.Error(w, "Failed to fetch device history", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

// fetchDeviceHistoryData performs the actual device history data fetch from the database.
func fetchDeviceHistoryData(ctx context.Context, timeRange string, requestedBuckets int) (*DeviceHistoryResponse, error) {
	start := time.Now()

	// Configure bucket size based on time range and requested bucket count
	var totalMinutes int
	switch timeRange {
	case "1h":
		totalMinutes = 60
	case "3h":
		totalMinutes = 3 * 60
	case "6h":
		totalMinutes = 6 * 60
	case "12h":
		totalMinutes = 12 * 60
	case "3d":
		totalMinutes = 3 * 24 * 60
	case "7d":
		totalMinutes = 7 * 24 * 60
	default: // "24h"
		timeRange = "24h"
		totalMinutes = 24 * 60
	}

	// Calculate bucket size to fit requested number of buckets
	bucketMinutes := snapBucketMinutes(totalMinutes / requestedBuckets)
	bucketCount := totalMinutes / bucketMinutes
	totalHours := totalMinutes / 60

	// Build the bucket interval expression
	var bucketInterval string
	if bucketMinutes >= 60 && bucketMinutes%60 == 0 {
		bucketInterval = fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %d HOUR, 'UTC')", bucketMinutes/60)
	} else {
		bucketInterval = fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %d MINUTE, 'UTC')", bucketMinutes)
	}

	// Get all devices with their metadata
	deviceQuery := `
		SELECT
			d.pk,
			d.code,
			d.device_type,
			COALESCE(c.code, '') as contributor,
			m.code as metro,
			d.max_users,
			d.status
		FROM dz_devices_current d
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		WHERE d.status IN ('activated', 'soft-drained', 'hard-drained', 'suspended')
	`

	deviceRows, err := envDB(ctx).Query(ctx, deviceQuery)
	if err != nil {
		return nil, fmt.Errorf("device history query error: %w", err)
	}
	defer deviceRows.Close()

	// Build map of device metadata
	type deviceMeta struct {
		code        string
		deviceType  string
		contributor string
		metro       string
		maxUsers    int32
		status      string
	}
	deviceMap := make(map[string]deviceMeta)

	for deviceRows.Next() {
		var pk string
		var meta deviceMeta
		if err := deviceRows.Scan(&pk, &meta.code, &meta.deviceType, &meta.contributor, &meta.metro, &meta.maxUsers, &meta.status); err != nil {
			return nil, fmt.Errorf("device scan error: %w", err)
		}
		deviceMap[pk] = meta
	}
	if err := deviceRows.Err(); err != nil {
		return nil, fmt.Errorf("device rows iteration error: %w", err)
	}

	// Get interface issues per bucket
	// Use greatest(0, delta) to ignore negative deltas (counter resets from device restarts)
	interfaceQuery := `
		SELECT
			device_pk,
			` + bucketInterval + ` as bucket,
			toUInt64(SUM(greatest(0, in_errors_delta))) as in_errors,
			toUInt64(SUM(greatest(0, out_errors_delta))) as out_errors,
			toUInt64(SUM(greatest(0, in_fcs_errors_delta))) as in_fcs_errors,
			toUInt64(SUM(greatest(0, in_discards_delta))) as in_discards,
			toUInt64(SUM(greatest(0, out_discards_delta))) as out_discards,
			toUInt64(SUM(greatest(0, carrier_transitions_delta))) as carrier_transitions
		FROM fact_dz_device_interface_counters
		WHERE event_ts > now() - INTERVAL ? HOUR
		GROUP BY device_pk, bucket
		ORDER BY device_pk, bucket
	`

	interfaceRows, err := envDB(ctx).Query(ctx, interfaceQuery, totalHours)
	if err != nil {
		return nil, fmt.Errorf("device interface query error: %w", err)
	}
	defer interfaceRows.Close()

	// Build bucket stats per device
	type bucketStats struct {
		bucket             time.Time
		inErrors           uint64
		outErrors          uint64
		inFcsErrors        uint64
		inDiscards         uint64
		outDiscards        uint64
		carrierTransitions uint64
	}
	deviceBuckets := make(map[string][]bucketStats)

	for interfaceRows.Next() {
		var devicePK string
		var stats bucketStats
		if err := interfaceRows.Scan(&devicePK, &stats.bucket, &stats.inErrors, &stats.outErrors, &stats.inFcsErrors, &stats.inDiscards, &stats.outDiscards, &stats.carrierTransitions); err != nil {
			return nil, fmt.Errorf("interface scan error: %w", err)
		}
		deviceBuckets[devicePK] = append(deviceBuckets[devicePK], stats)
	}
	if err := interfaceRows.Err(); err != nil {
		return nil, fmt.Errorf("interface rows iteration error: %w", err)
	}

	// Get latency probe presence per device per bucket.
	// Used to detect devices that have interface data but aren't sending probes.
	probeQuery := `
		SELECT
			origin_device_pk,
			` + bucketInterval + ` as bucket,
			count(*) as samples
		FROM fact_dz_device_link_latency
		WHERE event_ts > now() - INTERVAL ? HOUR
		GROUP BY origin_device_pk, bucket
		ORDER BY origin_device_pk, bucket
	`
	probeRows, err := envDB(ctx).Query(ctx, probeQuery, totalHours)
	if err != nil {
		return nil, fmt.Errorf("device probe query error: %w", err)
	}
	defer probeRows.Close()

	type deviceBucketProbeKey struct {
		devicePK string
		bucket   string
	}
	deviceProbes := make(map[deviceBucketProbeKey]bool)

	for probeRows.Next() {
		var devicePK string
		var bucket time.Time
		var samples uint64
		if err := probeRows.Scan(&devicePK, &bucket, &samples); err != nil {
			return nil, fmt.Errorf("probe scan error: %w", err)
		}
		if samples > 0 {
			key := deviceBucketProbeKey{devicePK: devicePK, bucket: bucket.UTC().Format(time.RFC3339)}
			deviceProbes[key] = true
		}
	}
	if err := probeRows.Err(); err != nil {
		return nil, fmt.Errorf("probe rows iteration error: %w", err)
	}

	// Get historical device status per bucket
	var historyBucketInterval string
	if bucketMinutes >= 60 && bucketMinutes%60 == 0 {
		historyBucketInterval = fmt.Sprintf("toStartOfInterval(snapshot_ts, INTERVAL %d HOUR, 'UTC')", bucketMinutes/60)
	} else {
		historyBucketInterval = fmt.Sprintf("toStartOfInterval(snapshot_ts, INTERVAL %d MINUTE, 'UTC')", bucketMinutes)
	}

	statusHistoryQuery := `
		SELECT
			pk as device_pk,
			` + historyBucketInterval + ` as bucket,
			argMax(status, snapshot_ts) as status
		FROM dim_dz_devices_history
		WHERE snapshot_ts > now() - INTERVAL ? HOUR
		GROUP BY device_pk, bucket
		ORDER BY device_pk, bucket
	`

	statusRows, err := safeQueryRows(ctx, statusHistoryQuery, totalHours)
	if err != nil {
		slog.Error("device status history query error", "error", err)
	}

	// Build map of device status per bucket
	type deviceBucketKey struct {
		devicePK string
		bucket   string
	}
	deviceStatusHistory := make(map[deviceBucketKey]string)

	if statusRows != nil {
		defer statusRows.Close()
		for statusRows.Next() {
			var devicePK, status string
			var bucket time.Time
			if err := statusRows.Scan(&devicePK, &bucket, &status); err != nil {
				return nil, fmt.Errorf("device status history scan error: %w", err)
			}
			key := deviceBucketKey{devicePK: devicePK, bucket: bucket.UTC().Format(time.RFC3339)}
			deviceStatusHistory[key] = status
		}
		if err := statusRows.Err(); err != nil {
			return nil, fmt.Errorf("device status rows iteration error: %w", err)
		}
	}

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	// Query ISIS device issues per bucket from SCD2 history
	type isisDevState struct {
		overload    bool
		unreachable bool
	}
	type isisDevEntry struct {
		bucket string
		state  isisDevState
	}
	isisDevEntries := make(map[string][]isisDevEntry) // device_pk -> sorted entries
	isisDevHistory := make(map[deviceBucketKey]isisDevState)

	isisDevHistQuery := `
		SELECT
			device_pk,
			` + historyBucketInterval + ` as bucket,
			argMax(overload, snapshot_ts) as overload,
			argMax(node_unreachable, snapshot_ts) as node_unreachable,
			argMax(is_deleted, snapshot_ts) as is_deleted
		FROM dim_isis_devices_history
		WHERE snapshot_ts > now() - INTERVAL ? HOUR
		  AND device_pk != ''
		GROUP BY device_pk, bucket
		ORDER BY device_pk, bucket
	`
	isisDevHistRows, isisDevHistErr := safeQueryRows(ctx, isisDevHistQuery, totalHours)
	if isisDevHistErr != nil {
		slog.Warn("device history: failed to query ISIS device history", "error", isisDevHistErr)
	}
	if isisDevHistRows != nil {
		defer isisDevHistRows.Close()
		for isisDevHistRows.Next() {
			var devicePK string
			var bucket time.Time
			var overload, unreachable, isDeleted uint8
			if err := isisDevHistRows.Scan(&devicePK, &bucket, &overload, &unreachable, &isDeleted); err != nil {
				slog.Error("ISIS device history scan error", "error", err)
				break
			}
			key := bucket.UTC().Format(time.RFC3339)
			state := isisDevState{}
			if isDeleted == 0 {
				state.overload = overload == 1
				state.unreachable = unreachable == 1
			}
			isisDevEntries[devicePK] = append(isisDevEntries[devicePK], isisDevEntry{bucket: key, state: state})
			isisDevHistory[deviceBucketKey{devicePK: devicePK, bucket: key}] = state
		}
	}

	// Baseline: ISIS device state before the time range
	isisDevBaseline := make(map[string]isisDevState)
	isisDevBaselineQuery := `
		SELECT
			device_pk,
			argMax(overload, snapshot_ts) as overload,
			argMax(node_unreachable, snapshot_ts) as node_unreachable,
			argMax(is_deleted, snapshot_ts) as is_deleted
		FROM dim_isis_devices_history
		WHERE snapshot_ts <= now() - INTERVAL ? HOUR
		  AND device_pk != ''
		GROUP BY device_pk
	`
	isisDevBaselineRows, isisDevBaselineErr := safeQueryRows(ctx, isisDevBaselineQuery, totalHours)
	if isisDevBaselineErr != nil {
		slog.Warn("ISIS device baseline query error", "error", isisDevBaselineErr)
	}
	if isisDevBaselineRows != nil {
		defer isisDevBaselineRows.Close()
		for isisDevBaselineRows.Next() {
			var devicePK string
			var overload, unreachable, isDeleted uint8
			if err := isisDevBaselineRows.Scan(&devicePK, &overload, &unreachable, &isDeleted); err == nil {
				state := isisDevState{}
				if isDeleted == 0 {
					state.overload = overload == 1
					state.unreachable = unreachable == 1
				}
				isisDevBaseline[devicePK] = state
			}
		}
	}

	// Build response with all buckets for each device
	now := time.Now().UTC()
	bucketDuration := time.Duration(bucketMinutes) * time.Minute
	var devices []DeviceHistory

	for pk, meta := range deviceMap {
		// Check if device is currently drained
		isCurrentlyDrained := meta.status == "soft-drained" || meta.status == "hard-drained" || meta.status == "suspended"

		// Track issue reasons for this device
		issueReasons := make(map[string]bool)

		if isCurrentlyDrained {
			issueReasons["drained"] = true
		}

		// Build ISIS state resolver for this device (SCD2 carry-forward)
		isisDevEntriesForDevice := isisDevEntries[pk]
		resolveISISDevState := func(bucketKey string) isisDevState {
			// Direct hit in history
			if state, ok := isisDevHistory[deviceBucketKey{devicePK: pk, bucket: bucketKey}]; ok {
				return state
			}
			// No direct hit — find the most recent entry before this bucket
			if len(isisDevEntriesForDevice) > 0 {
				idx := sort.Search(len(isisDevEntriesForDevice), func(i int) bool { return isisDevEntriesForDevice[i].bucket > bucketKey })
				if idx > 0 {
					return isisDevEntriesForDevice[idx-1].state
				}
			}
			// Fall back to baseline
			if baseline, ok := isisDevBaseline[pk]; ok {
				return baseline
			}
			return isisDevState{}
		}

		// Get interface stats for this device
		buckets := deviceBuckets[pk]

		// Check for interface issues
		for _, b := range buckets {
			totalErrors := b.inErrors + b.outErrors
			totalDiscards := b.inDiscards + b.outDiscards
			if totalErrors > 0 {
				issueReasons["interface_errors"] = true
			}
			if b.inFcsErrors > 0 {
				issueReasons["fcs_errors"] = true
			}
			if totalDiscards > 0 {
				issueReasons["discards"] = true
			}
			if b.carrierTransitions > 0 {
				issueReasons["carrier_transitions"] = true
			}
		}

		// Also check if device was drained at any point in the history
		for key := range deviceStatusHistory {
			if key.devicePK == pk {
				status := deviceStatusHistory[key]
				if status == "soft-drained" || status == "hard-drained" || status == "suspended" {
					issueReasons["drained"] = true
					break
				}
			}
		}

		// Convert issue reasons to slice
		var issueReasonsList []string
		for reason := range issueReasons {
			issueReasonsList = append(issueReasonsList, reason)
		}
		sort.Strings(issueReasonsList)

		// Build bucket status array
		bucketMap := make(map[string]bucketStats)
		for _, b := range buckets {
			key := b.bucket.UTC().Format(time.RFC3339)
			bucketMap[key] = b
		}

		var hourStatuses []DeviceHourStatus
		for i := bucketCount - 1; i >= 0; i-- {
			bucketStart := now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
			key := bucketStart.UTC().Format(time.RFC3339)
			isCollecting := i == 0

			// Check historical status for this bucket
			histKey := deviceBucketKey{devicePK: pk, bucket: key}
			historicalStatus, hasHistory := deviceStatusHistory[histKey]
			wasDrained := hasHistory && (historicalStatus == "soft-drained" || historicalStatus == "hard-drained" || historicalStatus == "suspended")

			// Resolve ISIS state for this bucket
			isisState := resolveISISDevState(key)
			if isisState.overload {
				issueReasons["isis_overload"] = true
			}
			if isisState.unreachable {
				issueReasons["isis_unreachable"] = true
			}

			// If device was drained at this time, show as disabled
			if wasDrained {
				hourStatuses = append(hourStatuses, DeviceHourStatus{
					Hour:            key,
					Status:          "disabled",
					Collecting:      isCollecting,
					DrainStatus:     historicalStatus,
					ISISOverload:    isisState.overload,
					ISISUnreachable: isisState.unreachable,
				})
				continue
			}

			// Check if we have interface data for this bucket
			if stats, ok := bucketMap[key]; ok {
				status := classifyDeviceStatus(stats.inErrors+stats.outErrors+stats.inFcsErrors, stats.inDiscards+stats.outDiscards, stats.carrierTransitions)

				// If device has interface data but no latency probes, mark unhealthy.
				// Skip collecting bucket since probes may not have arrived yet.
				probeKey := deviceBucketProbeKey{devicePK: pk, bucket: key}
				noProbes := !isCollecting && !deviceProbes[probeKey]
				if noProbes {
					if status == "healthy" || status == "degraded" {
						status = "unhealthy"
					}
					issueReasons["no_probes"] = true
				}

				hourStatuses = append(hourStatuses, DeviceHourStatus{
					Hour:               key,
					Status:             status,
					Collecting:         isCollecting,
					MaxUsers:           meta.maxUsers,
					InErrors:           stats.inErrors,
					OutErrors:          stats.outErrors,
					InFcsErrors:        stats.inFcsErrors,
					InDiscards:         stats.inDiscards,
					OutDiscards:        stats.outDiscards,
					CarrierTransitions: stats.carrierTransitions,
					NoProbes:           noProbes,
					ISISOverload:       isisState.overload,
					ISISUnreachable:    isisState.unreachable,
				})
			} else {
				// No interface data for this bucket — show as no_data.
				hourStatuses = append(hourStatuses, DeviceHourStatus{
					Hour:            key,
					Status:          "no_data",
					Collecting:      isCollecting,
					MaxUsers:        meta.maxUsers,
					ISISOverload:    isisState.overload,
					ISISUnreachable: isisState.unreachable,
				})
			}
		}

		// Check for no_data buckets (missing interface counters)
		for _, h := range hourStatuses {
			if h.Collecting {
				continue
			}
			if h.Status == "no_data" {
				issueReasons["no_data"] = true
				break
			}
		}

		// Rebuild issue reasons list after no_data check
		issueReasonsList = nil
		for reason := range issueReasons {
			issueReasonsList = append(issueReasonsList, reason)
		}
		sort.Strings(issueReasonsList)

		devices = append(devices, DeviceHistory{
			PK:           pk,
			Code:         meta.code,
			DeviceType:   meta.deviceType,
			Contributor:  meta.contributor,
			Metro:        meta.metro,
			MaxUsers:     meta.maxUsers,
			Hours:        hourStatuses,
			IssueReasons: issueReasonsList,
		})
	}

	// Sort devices by code for consistent ordering
	sort.Slice(devices, func(i, j int) bool {
		return devices[i].Code < devices[j].Code
	})

	resp := &DeviceHistoryResponse{
		Devices:       devices,
		TimeRange:     timeRange,
		BucketMinutes: bucketMinutes,
		BucketCount:   bucketCount,
	}

	slog.Info("fetchDeviceHistoryData completed", "duration", time.Since(start), "range", timeRange, "buckets", bucketCount, "devices", len(devices))

	return resp, nil
}

func classifyDeviceStatus(totalErrors, totalDiscards uint64, carrierTransitions uint64) string {
	// Thresholds for device health (per bucket)
	// Unhealthy: >= 100 of any metric
	// Degraded: > 0 and < 100 of any metric
	const UnhealthyThreshold = 100

	if totalErrors >= UnhealthyThreshold || totalDiscards >= UnhealthyThreshold || carrierTransitions >= UnhealthyThreshold {
		return "unhealthy"
	}
	if totalErrors > 0 || totalDiscards > 0 || carrierTransitions > 0 {
		return "degraded"
	}
	return "healthy"
}

// InterfaceIssuesResponse is the response for interface issues endpoint
type InterfaceIssuesResponse struct {
	Issues    []InterfaceIssue `json:"issues"`
	TimeRange string           `json:"time_range"`
}

// GetInterfaceIssues returns interface issues for a given time range
func GetInterfaceIssues(w http.ResponseWriter, r *http.Request) {
	timeRange := r.URL.Query().Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}

	// Convert time range to duration
	var duration time.Duration
	switch timeRange {
	case "3h":
		duration = 3 * time.Hour
	case "6h":
		duration = 6 * time.Hour
	case "12h":
		duration = 12 * time.Hour
	case "24h":
		duration = 24 * time.Hour
	case "3d":
		duration = 3 * 24 * time.Hour
	case "7d":
		duration = 7 * 24 * time.Hour
	default:
		duration = 24 * time.Hour
		timeRange = "24h"
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	issues, err := fetchInterfaceIssuesData(ctx, duration)
	if err != nil {
		slog.Error("error fetching interface issues", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	resp := &InterfaceIssuesResponse{
		Issues:    issues,
		TimeRange: timeRange,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func fetchInterfaceIssuesData(ctx context.Context, duration time.Duration) ([]InterfaceIssue, error) {
	// Convert duration to hours for the SQL interval
	hours := int(duration.Hours())

	query := fmt.Sprintf(`
		SELECT
			d.pk as device_pk,
			d.code as device_code,
			d.device_type,
			COALESCE(contrib.code, '') as contributor,
			m.code as metro,
			c.intf as interface_name,
			COALESCE(l.pk, '') as link_pk,
			COALESCE(l.code, '') as link_code,
			COALESCE(l.link_type, '') as link_type,
			COALESCE(c.link_side, '') as link_side,
			toUInt64(SUM(greatest(0, c.in_errors_delta))) as in_errors,
			toUInt64(SUM(greatest(0, c.out_errors_delta))) as out_errors,
			toUInt64(SUM(greatest(0, c.in_fcs_errors_delta))) as in_fcs_errors,
			toUInt64(SUM(greatest(0, c.in_discards_delta))) as in_discards,
			toUInt64(SUM(greatest(0, c.out_discards_delta))) as out_discards,
			toUInt64(SUM(greatest(0, c.carrier_transitions_delta))) as carrier_transitions,
			formatDateTime(min(c.event_ts), '%%Y-%%m-%%dT%%H:%%i:%%sZ', 'UTC') as first_seen,
			formatDateTime(max(c.event_ts), '%%Y-%%m-%%dT%%H:%%i:%%sZ', 'UTC') as last_seen
		FROM fact_dz_device_interface_counters c
		JOIN dz_devices_current d ON c.device_pk = d.pk
		JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current contrib ON d.contributor_pk = contrib.pk
		LEFT JOIN dz_links_current l ON c.link_pk = l.pk
		WHERE c.event_ts > now() - INTERVAL %d HOUR
		  AND d.status = 'activated'
		  AND (c.in_errors_delta > 0 OR c.out_errors_delta > 0 OR c.in_fcs_errors_delta > 0 OR c.in_discards_delta > 0 OR c.out_discards_delta > 0 OR c.carrier_transitions_delta > 0)
		GROUP BY d.pk, d.code, d.device_type, contrib.code, m.code, c.intf, l.pk, l.code, l.link_type, c.link_side
		ORDER BY (in_errors + out_errors + in_fcs_errors + in_discards + out_discards + carrier_transitions) DESC
		LIMIT 50
	`, hours)

	rows, err := envDB(ctx).Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var issues []InterfaceIssue
	for rows.Next() {
		var issue InterfaceIssue
		if err := rows.Scan(
			&issue.DevicePK,
			&issue.DeviceCode,
			&issue.DeviceType,
			&issue.Contributor,
			&issue.Metro,
			&issue.InterfaceName,
			&issue.LinkPK,
			&issue.LinkCode,
			&issue.LinkType,
			&issue.LinkSide,
			&issue.InErrors,
			&issue.OutErrors,
			&issue.InFcsErrors,
			&issue.InDiscards,
			&issue.OutDiscards,
			&issue.CarrierTransitions,
			&issue.FirstSeen,
			&issue.LastSeen,
		); err != nil {
			return nil, err
		}
		issues = append(issues, issue)
	}

	return issues, rows.Err()
}

// DeviceInterfaceHistoryResponse is the response for device interface history endpoint
type DeviceInterfaceHistoryResponse struct {
	Interfaces    []InterfaceHistory `json:"interfaces"`
	TimeRange     string             `json:"time_range"`
	BucketMinutes int                `json:"bucket_minutes"`
	BucketCount   int                `json:"bucket_count"`
}

// InterfaceHistory is the history of a single interface
type InterfaceHistory struct {
	InterfaceName string                `json:"interface_name"`
	LinkPK        string                `json:"link_pk,omitempty"`
	LinkCode      string                `json:"link_code,omitempty"`
	LinkType      string                `json:"link_type,omitempty"`
	LinkSide      string                `json:"link_side,omitempty"`
	Hours         []InterfaceHourStatus `json:"hours"`
}

// InterfaceHourStatus is the status of an interface for a single time bucket
type InterfaceHourStatus struct {
	Hour               string `json:"hour"`
	InErrors           uint64 `json:"in_errors"`
	OutErrors          uint64 `json:"out_errors"`
	InFcsErrors        uint64 `json:"in_fcs_errors"`
	InDiscards         uint64 `json:"in_discards"`
	OutDiscards        uint64 `json:"out_discards"`
	CarrierTransitions uint64 `json:"carrier_transitions"`
}

// GetDeviceInterfaceHistory returns interface-level history for a specific device
func GetDeviceInterfaceHistory(w http.ResponseWriter, r *http.Request) {
	devicePK := chi.URLParam(r, "pk")
	if devicePK == "" {
		http.Error(w, "Device PK is required", http.StatusBadRequest)
		return
	}

	timeRange := r.URL.Query().Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}

	bucketsStr := r.URL.Query().Get("buckets")
	requestedBuckets := 72 // default
	if bucketsStr != "" {
		if b, err := strconv.Atoi(bucketsStr); err == nil && b > 0 && b <= 10000 {
			requestedBuckets = b
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	resp, err := fetchDeviceInterfaceHistoryData(ctx, devicePK, timeRange, requestedBuckets)
	if err != nil {
		slog.Error("error fetching device interface history", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func fetchDeviceInterfaceHistoryData(ctx context.Context, devicePK string, timeRange string, requestedBuckets int) (*DeviceInterfaceHistoryResponse, error) {
	// Calculate bucket size based on time range and requested buckets
	var totalHours int
	switch timeRange {
	case "3h":
		totalHours = 3
	case "6h":
		totalHours = 6
	case "12h":
		totalHours = 12
	case "24h":
		totalHours = 24
	case "3d":
		totalHours = 72
	case "7d":
		totalHours = 168
	default:
		totalHours = 24
		timeRange = "24h"
	}

	// Calculate bucket size in minutes
	totalMinutes := totalHours * 60
	bucketMinutes := snapBucketMinutes(totalMinutes / requestedBuckets)
	bucketCount := totalMinutes / bucketMinutes

	// Build interval expression for ClickHouse
	bucketInterval := fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %d MINUTE, 'UTC')", bucketMinutes)

	// Query interface stats per bucket for this device (including the current collecting bucket).
	query := `
		SELECT
			c.intf as interface_name,
			COALESCE(l.pk, '') as link_pk,
			COALESCE(l.code, '') as link_code,
			COALESCE(l.link_type, '') as link_type,
			COALESCE(c.link_side, '') as link_side,
			` + bucketInterval + ` as bucket,
			toUInt64(SUM(greatest(0, c.in_errors_delta))) as in_errors,
			toUInt64(SUM(greatest(0, c.out_errors_delta))) as out_errors,
			toUInt64(SUM(greatest(0, c.in_fcs_errors_delta))) as in_fcs_errors,
			toUInt64(SUM(greatest(0, c.in_discards_delta))) as in_discards,
			toUInt64(SUM(greatest(0, c.out_discards_delta))) as out_discards,
			toUInt64(SUM(greatest(0, c.carrier_transitions_delta))) as carrier_transitions
		FROM fact_dz_device_interface_counters c
		LEFT JOIN dz_links_current l ON c.link_pk = l.pk
		WHERE c.device_pk = ?
		  AND c.event_ts > now() - INTERVAL ? HOUR
		GROUP BY c.intf, l.pk, l.code, l.link_type, c.link_side, bucket
		ORDER BY c.intf, bucket
	`

	rows, err := envDB(ctx).Query(ctx, query, devicePK, totalHours)
	if err != nil {
		return nil, fmt.Errorf("interface history query error: %w", err)
	}
	defer rows.Close()

	// Build interface history map
	type interfaceMeta struct {
		linkPK   string
		linkCode string
		linkType string
		linkSide string
	}
	type bucketStats struct {
		bucket             time.Time
		inErrors           uint64
		outErrors          uint64
		inFcsErrors        uint64
		inDiscards         uint64
		outDiscards        uint64
		carrierTransitions uint64
	}

	interfaceMetaMap := make(map[string]interfaceMeta)
	interfaceBuckets := make(map[string][]bucketStats)

	for rows.Next() {
		var intfName string
		var meta interfaceMeta
		var stats bucketStats
		if err := rows.Scan(
			&intfName,
			&meta.linkPK,
			&meta.linkCode,
			&meta.linkType,
			&meta.linkSide,
			&stats.bucket,
			&stats.inErrors,
			&stats.outErrors,
			&stats.inFcsErrors,
			&stats.inDiscards,
			&stats.outDiscards,
			&stats.carrierTransitions,
		); err != nil {
			return nil, fmt.Errorf("interface history scan error: %w", err)
		}
		interfaceMetaMap[intfName] = meta
		interfaceBuckets[intfName] = append(interfaceBuckets[intfName], stats)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("interface history rows iteration error: %w", err)
	}

	// Build response with all buckets for each interface
	now := time.Now().UTC()
	bucketDuration := time.Duration(bucketMinutes) * time.Minute
	var interfaces []InterfaceHistory

	for intfName, meta := range interfaceMetaMap {
		buckets := interfaceBuckets[intfName]
		bucketMap := make(map[string]bucketStats)
		for _, b := range buckets {
			key := b.bucket.UTC().Format(time.RFC3339)
			bucketMap[key] = b
		}

		var hourStatuses []InterfaceHourStatus
		for i := bucketCount - 1; i >= 0; i-- {
			bucketStart := now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
			key := bucketStart.UTC().Format(time.RFC3339)

			if stats, ok := bucketMap[key]; ok {
				hourStatuses = append(hourStatuses, InterfaceHourStatus{
					Hour:               key,
					InErrors:           stats.inErrors,
					OutErrors:          stats.outErrors,
					InFcsErrors:        stats.inFcsErrors,
					InDiscards:         stats.inDiscards,
					OutDiscards:        stats.outDiscards,
					CarrierTransitions: stats.carrierTransitions,
				})
			} else {
				hourStatuses = append(hourStatuses, InterfaceHourStatus{
					Hour: key,
				})
			}
		}

		interfaces = append(interfaces, InterfaceHistory{
			InterfaceName: intfName,
			LinkPK:        meta.linkPK,
			LinkCode:      meta.linkCode,
			LinkType:      meta.linkType,
			LinkSide:      meta.linkSide,
			Hours:         hourStatuses,
		})
	}

	// Sort interfaces by name
	sort.Slice(interfaces, func(i, j int) bool {
		return interfaces[i].InterfaceName < interfaces[j].InterfaceName
	})

	return &DeviceInterfaceHistoryResponse{
		Interfaces:    interfaces,
		TimeRange:     timeRange,
		BucketMinutes: bucketMinutes,
		BucketCount:   bucketCount,
	}, nil
}

// SingleLinkHistoryResponse is the response for a single link's status history
type SingleLinkHistoryResponse struct {
	PK             string           `json:"pk"`
	Code           string           `json:"code"`
	CommittedRttUs float64          `json:"committed_rtt_us"`
	Hours          []LinkHourStatus `json:"hours"`
	TimeRange      string           `json:"time_range"`
	BucketMinutes  int              `json:"bucket_minutes"`
	BucketCount    int              `json:"bucket_count"`
}

// GetSingleLinkHistory returns the status history for a single link
func GetSingleLinkHistory(w http.ResponseWriter, r *http.Request) {
	linkPK := chi.URLParam(r, "pk")
	if linkPK == "" {
		http.Error(w, "missing link pk", http.StatusBadRequest)
		return
	}

	// Parse time range parameter
	timeRange := r.URL.Query().Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}

	// Parse optional bucket count
	requestedBuckets := 24 // default
	if b := r.URL.Query().Get("buckets"); b != "" {
		if n, err := strconv.Atoi(b); err == nil && n >= 12 && n <= 10000 {
			requestedBuckets = n
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	resp, err := fetchSingleLinkHistoryData(ctx, linkPK, timeRange, requestedBuckets)
	if err != nil {
		slog.Error("error fetching single link history", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if resp == nil {
		http.Error(w, "Link not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

func fetchSingleLinkHistoryData(ctx context.Context, linkPK string, timeRange string, requestedBuckets int) (*SingleLinkHistoryResponse, error) {
	// Calculate bucket size based on time range and requested buckets
	var totalMinutes int
	switch timeRange {
	case "1h":
		totalMinutes = 60
	case "3h":
		totalMinutes = 3 * 60
	case "6h":
		totalMinutes = 6 * 60
	case "12h":
		totalMinutes = 12 * 60
	case "3d":
		totalMinutes = 3 * 24 * 60
	case "7d":
		totalMinutes = 7 * 24 * 60
	default: // "24h"
		timeRange = "24h"
		totalMinutes = 24 * 60
	}

	bucketMinutes := snapBucketMinutes(totalMinutes / requestedBuckets)
	bucketCount := totalMinutes / bucketMinutes
	totalHours := totalMinutes / 60
	bucketDuration := time.Duration(bucketMinutes) * time.Minute
	now := time.Now().UTC()

	// Build the bucket interval expression
	var bucketInterval string
	if bucketMinutes >= 60 && bucketMinutes%60 == 0 {
		bucketInterval = fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %d HOUR, 'UTC')", bucketMinutes/60)
	} else {
		bucketInterval = fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %d MINUTE, 'UTC')", bucketMinutes)
	}

	// Get link metadata
	linkQuery := `
		SELECT l.code, l.bandwidth_bps, l.committed_rtt_ns / 1000.0 as committed_rtt_us,
			   l.side_a_pk, l.side_z_pk, l.side_a_iface_name, l.side_z_iface_name,
			   l.link_type,
			   COALESCE(da.metro_pk, '') as side_a_metro,
			   COALESCE(dz.metro_pk, '') as side_z_metro
		FROM dz_links_current l
		LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
		LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		WHERE l.pk = ?
	`
	var code string
	var bandwidthBps int64
	var committedRttUs float64
	var sideAPK, sideZPK, sideAIface, sideZIface string
	var linkType, sideAMetro, sideZMetro string
	err := envDB(ctx).QueryRow(ctx, linkQuery, linkPK).Scan(&code, &bandwidthBps, &committedRttUs, &sideAPK, &sideZPK, &sideAIface, &sideZIface, &linkType, &sideAMetro, &sideZMetro)
	if err != nil {
		return nil, nil // Link not found
	}

	// Only use committed RTT for inter-metro WAN links (not DZX)
	if linkType != "WAN" || sideAMetro == sideZMetro {
		committedRttUs = 0
	}

	// Get latency/loss stats per direction.
	// Loss is computed as the max of 5-minute sub-bucket loss percentages within each
	// display bucket, matching Grafana's [5m] window for sharper spike visibility.
	singleLossBucket := fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %d MINUTE, 'UTC')", min(bucketMinutes, 5))
	// Include the current (possibly incomplete) bucket — the frontend marks it as "collecting".
	singleTimeFilter := fmt.Sprintf("event_ts > now() - INTERVAL %d HOUR", totalHours)
	latencyQuery := `
		WITH loss_sub AS (
			SELECT
				` + bucketInterval + ` as display_bucket,
				if(origin_device_pk = ?, 'A', 'Z') as direction,
				countIf(loss OR rtt_us = 0) * 100.0 / count(*) as loss_pct
			FROM fact_dz_device_link_latency
			WHERE link_pk = ? AND ` + singleTimeFilter + `
			GROUP BY display_bucket, direction, ` + singleLossBucket + `
		),
		loss_max AS (
			SELECT display_bucket, direction, max(loss_pct) as loss_pct
			FROM loss_sub
			GROUP BY display_bucket, direction
		)
		SELECT
			` + bucketInterval + ` as bucket,
			if(origin_device_pk = ?, 'A', 'Z') as direction,
			avg(rtt_us) as avg_latency,
			max(lm.loss_pct) as loss_pct,
			count(*) as samples
		FROM fact_dz_device_link_latency f
		LEFT JOIN loss_max lm ON ` + bucketInterval + ` = lm.display_bucket
			AND if(origin_device_pk = ?, 'A', 'Z') = lm.direction
		WHERE link_pk = ? AND ` + singleTimeFilter + `
		GROUP BY bucket, direction
		ORDER BY bucket, direction
	`
	latencyRows, err := envDB(ctx).Query(ctx, latencyQuery, sideAPK, linkPK, sideAPK, sideAPK, linkPK)
	if err != nil {
		return nil, fmt.Errorf("latency query error: %w", err)
	}
	defer latencyRows.Close()

	type sideStats struct {
		avgLatency float64
		lossPct    float64
		samples    uint64
	}
	type bucketStats struct {
		sideA *sideStats
		sideZ *sideStats
	}
	bucketMap := make(map[string]*bucketStats)

	for latencyRows.Next() {
		var bucket time.Time
		var direction string
		var avgLatency, lossPct float64
		var samples uint64
		if err := latencyRows.Scan(&bucket, &direction, &avgLatency, &lossPct, &samples); err != nil {
			return nil, fmt.Errorf("latency scan error: %w", err)
		}

		key := bucket.UTC().Format(time.RFC3339)
		if bucketMap[key] == nil {
			bucketMap[key] = &bucketStats{}
		}
		stats := &sideStats{avgLatency: avgLatency, lossPct: lossPct, samples: samples}
		if direction == "A" {
			bucketMap[key].sideA = stats
		} else {
			bucketMap[key].sideZ = stats
		}
	}

	// Get interface counters for both sides
	interfaceQuery := `
		SELECT
			` + bucketInterval + ` as bucket,
			if(device_pk = ?, 'A', 'Z') as side,
			toUInt64(sum(greatest(0, in_errors_delta))) as in_errors,
			toUInt64(sum(greatest(0, out_errors_delta))) as out_errors,
			toUInt64(sum(greatest(0, in_fcs_errors_delta))) as in_fcs_errors,
			toUInt64(sum(greatest(0, in_discards_delta))) as in_discards,
			toUInt64(sum(greatest(0, out_discards_delta))) as out_discards,
			toUInt64(sum(greatest(0, carrier_transitions_delta))) as carrier_transitions,
			toUInt64(sum(greatest(0, in_octets_delta))) as in_octets,
			toUInt64(sum(greatest(0, out_octets_delta))) as out_octets,
			sum(delta_duration) as duration
		FROM fact_dz_device_interface_counters
		WHERE link_pk = ? AND event_ts > now() - INTERVAL ? HOUR
		GROUP BY bucket, side
		ORDER BY bucket, side
	`
	ifaceRows, err := envDB(ctx).Query(ctx, interfaceQuery, sideAPK, linkPK, totalHours)
	if err != nil {
		return nil, fmt.Errorf("interface query error: %w", err)
	}
	defer ifaceRows.Close()

	type ifaceStats struct {
		inErrors           uint64
		outErrors          uint64
		inFcsErrors        uint64
		inDiscards         uint64
		outDiscards        uint64
		carrierTransitions uint64
		inOctets           uint64
		outOctets          uint64
		duration           float64
	}
	type ifaceBucketStats struct {
		sideA *ifaceStats
		sideZ *ifaceStats
	}
	ifaceBucketMap := make(map[string]*ifaceBucketStats)

	for ifaceRows.Next() {
		var bucket time.Time
		var side string
		var stats ifaceStats
		if err := ifaceRows.Scan(&bucket, &side, &stats.inErrors, &stats.outErrors, &stats.inFcsErrors, &stats.inDiscards, &stats.outDiscards, &stats.carrierTransitions, &stats.inOctets, &stats.outOctets, &stats.duration); err != nil {
			return nil, fmt.Errorf("interface scan error: %w", err)
		}

		key := bucket.UTC().Format(time.RFC3339)
		if ifaceBucketMap[key] == nil {
			ifaceBucketMap[key] = &ifaceBucketStats{}
		}
		if side == "A" {
			ifaceBucketMap[key].sideA = &stats
		} else {
			ifaceBucketMap[key].sideZ = &stats
		}
	}

	// Get historical link status per bucket from dim_dz_links_history for drain status
	var historyBucketInterval string
	if bucketMinutes >= 60 && bucketMinutes%60 == 0 {
		historyBucketInterval = fmt.Sprintf("toStartOfInterval(snapshot_ts, INTERVAL %d HOUR, 'UTC')", bucketMinutes/60)
	} else {
		historyBucketInterval = fmt.Sprintf("toStartOfInterval(snapshot_ts, INTERVAL %d MINUTE, 'UTC')", bucketMinutes)
	}

	type statusEntry struct {
		bucket string
		status string
	}
	linkDrainHistory := make(map[string]string) // bucket -> status
	var drainEntries []statusEntry

	historyQuery := `
		SELECT
			` + historyBucketInterval + ` as bucket,
			argMax(status, snapshot_ts) as status
		FROM dim_dz_links_history
		WHERE pk = ? AND snapshot_ts > now() - INTERVAL ? HOUR
		GROUP BY bucket
		ORDER BY bucket
	`
	historyRows, err := envDB(ctx).Query(ctx, historyQuery, linkPK, totalHours)
	if err == nil {
		defer historyRows.Close()
		for historyRows.Next() {
			var bucket time.Time
			var status string
			if err := historyRows.Scan(&bucket, &status); err == nil {
				key := bucket.UTC().Format(time.RFC3339)
				linkDrainHistory[key] = status
				drainEntries = append(drainEntries, statusEntry{bucket: key, status: status})
			}
		}
	}

	// Get baseline status before the time range
	var baselineDrainStatus string
	baselineQuery := `
		SELECT argMax(status, snapshot_ts) as status
		FROM dim_dz_links_history
		WHERE pk = ? AND snapshot_ts <= now() - INTERVAL ? HOUR
	`
	_ = envDB(ctx).QueryRow(ctx, baselineQuery, linkPK, totalHours).Scan(&baselineDrainStatus)

	isDrainedStatus := func(s string) bool {
		return s == "soft-drained" || s == "hard-drained"
	}
	resolveDrainStatus := func(bucketKey string) string {
		if s, ok := linkDrainHistory[bucketKey]; ok {
			if isDrainedStatus(s) {
				return s
			}
			return ""
		}
		if len(drainEntries) > 0 {
			idx := sort.Search(len(drainEntries), func(i int) bool { return drainEntries[i].bucket > bucketKey })
			if idx > 0 {
				if isDrainedStatus(drainEntries[idx-1].status) {
					return drainEntries[idx-1].status
				}
				return ""
			}
		}
		if isDrainedStatus(baselineDrainStatus) {
			return baselineDrainStatus
		}
		return ""
	}

	// Query ISIS adjacency history for this link (SCD2 carry-forward)
	type isisAdjEntry struct {
		bucket    string
		isDeleted bool
	}
	var isisAdjEntriesForLink []isisAdjEntry
	isisAdjHistoryMap := make(map[string]bool) // bucket -> is_deleted

	isisAdjHistQuery := `
		SELECT
			` + historyBucketInterval + ` as bucket,
			argMax(is_deleted, snapshot_ts) as is_deleted
		FROM dim_isis_adjacencies_history
		WHERE link_pk = ? AND snapshot_ts > now() - INTERVAL ? HOUR
		GROUP BY bucket
		ORDER BY bucket
	`
	isisAdjHistRows, isisAdjHistErr := envDB(ctx).Query(ctx, isisAdjHistQuery, linkPK, totalHours)
	if isisAdjHistErr != nil {
		slog.Warn("single link history: failed to query ISIS adjacency history", "error", isisAdjHistErr)
	}
	if isisAdjHistRows != nil {
		defer isisAdjHistRows.Close()
		for isisAdjHistRows.Next() {
			var bucket time.Time
			var isDeleted uint8
			if err := isisAdjHistRows.Scan(&bucket, &isDeleted); err != nil {
				slog.Error("ISIS adjacency history scan error", "error", err)
				break
			}
			key := bucket.UTC().Format(time.RFC3339)
			deleted := isDeleted == 1
			isisAdjEntriesForLink = append(isisAdjEntriesForLink, isisAdjEntry{bucket: key, isDeleted: deleted})
			isisAdjHistoryMap[key] = deleted
		}
	}

	// Baseline: ISIS adjacency state before the time range
	var isisAdjBaselineIsDeleted *bool
	isisAdjBaselineQuery := `
		SELECT argMax(is_deleted, snapshot_ts) as is_deleted, count() as cnt
		FROM dim_isis_adjacencies_history
		WHERE link_pk = ? AND snapshot_ts <= now() - INTERVAL ? HOUR
	`
	var baselineIsDeleted uint8
	var baselineCnt uint64
	if err := envDB(ctx).QueryRow(ctx, isisAdjBaselineQuery, linkPK, totalHours).Scan(&baselineIsDeleted, &baselineCnt); err == nil && baselineCnt > 0 {
		v := baselineIsDeleted == 1
		isisAdjBaselineIsDeleted = &v
	}

	hasISISHistory := len(isisAdjEntriesForLink) > 0 || isisAdjBaselineIsDeleted != nil

	// Check if this link is currently missing its ISIS adjacency (with sibling check)
	isMissingISISAdj := false
	if !hasISISHistory {
		missingISISQuery := `
			SELECT count() > 0
			FROM dz_links_current l
			WHERE l.pk = ?
			  AND l.status = 'activated'
			  AND l.tunnel_net != ''
			  AND l.committed_rtt_ns != ?
			  AND l.pk NOT IN (
			    SELECT DISTINCT link_pk
			    FROM isis_adjacencies_current
			    WHERE link_pk != ''
			  )
			  AND l.tunnel_net NOT IN (
			    SELECT DISTINCT l2.tunnel_net
			    FROM dz_links_current l2
			    JOIN isis_adjacencies_current a ON a.link_pk = l2.pk
			    WHERE l2.tunnel_net != '' AND a.link_pk != ''
			  )
		`
		var missing uint8
		if err := envDB(ctx).QueryRow(ctx, missingISISQuery, linkPK, committedRttProvisioningNs).Scan(&missing); err == nil {
			isMissingISISAdj = missing == 1
		}
	}

	resolveISISDown := func(bucketKey string) bool {
		// Direct hit in history
		if isDeleted, ok := isisAdjHistoryMap[bucketKey]; ok {
			return isDeleted
		}
		// No direct hit — find the most recent entry before this bucket
		if len(isisAdjEntriesForLink) > 0 {
			idx := sort.Search(len(isisAdjEntriesForLink), func(i int) bool { return isisAdjEntriesForLink[i].bucket > bucketKey })
			if idx > 0 {
				return isisAdjEntriesForLink[idx-1].isDeleted
			}
		}
		// No in-range entries — fall back to baseline
		if isisAdjBaselineIsDeleted != nil {
			return *isisAdjBaselineIsDeleted
		}
		// No history at all — use current missing check
		if !hasISISHistory {
			return isMissingISISAdj
		}
		return false
	}

	// Build hour statuses including the current collecting bucket.
	var hourStatuses []LinkHourStatus
	for i := bucketCount - 1; i >= 0; i-- {
		bucketStart := now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
		key := bucketStart.UTC().Format(time.RFC3339)

		var hs LinkHourStatus
		hs.Hour = key
		hs.Collecting = i == 0
		hs.DrainStatus = resolveDrainStatus(key)

		// Latency/loss stats
		// Loss uses max of either direction so one-sided loss isn't diluted.
		if bs := bucketMap[key]; bs != nil {
			var totalLatency float64
			var totalSamples uint64
			var maxLoss float64
			if bs.sideA != nil {
				hs.SideALatencyUs = bs.sideA.avgLatency
				hs.SideALossPct = bs.sideA.lossPct
				hs.SideASamples = bs.sideA.samples
				totalLatency += bs.sideA.avgLatency * float64(bs.sideA.samples)
				totalSamples += bs.sideA.samples
				if bs.sideA.lossPct > maxLoss {
					maxLoss = bs.sideA.lossPct
				}
			}
			if bs.sideZ != nil {
				hs.SideZLatencyUs = bs.sideZ.avgLatency
				hs.SideZLossPct = bs.sideZ.lossPct
				hs.SideZSamples = bs.sideZ.samples
				totalLatency += bs.sideZ.avgLatency * float64(bs.sideZ.samples)
				totalSamples += bs.sideZ.samples
				if bs.sideZ.lossPct > maxLoss {
					maxLoss = bs.sideZ.lossPct
				}
			}
			if totalSamples > 0 {
				hs.AvgLatencyUs = totalLatency / float64(totalSamples)
				hs.AvgLossPct = maxLoss
				hs.Samples = totalSamples
			}
		}

		// Interface stats
		if ibs := ifaceBucketMap[key]; ibs != nil {
			if ibs.sideA != nil {
				hs.SideAInErrors = ibs.sideA.inErrors
				hs.SideAOutErrors = ibs.sideA.outErrors
				hs.SideAInFcsErrors = ibs.sideA.inFcsErrors
				hs.SideAInDiscards = ibs.sideA.inDiscards
				hs.SideAOutDiscards = ibs.sideA.outDiscards
				hs.SideACarrierTransitions = ibs.sideA.carrierTransitions
				if ibs.sideA.duration > 0 && bandwidthBps > 0 {
					inRate := float64(ibs.sideA.inOctets) * 8 / ibs.sideA.duration
					outRate := float64(ibs.sideA.outOctets) * 8 / ibs.sideA.duration
					hs.UtilizationInPct = inRate * 100 / float64(bandwidthBps)
					hs.UtilizationOutPct = outRate * 100 / float64(bandwidthBps)
				}
			}
			if ibs.sideZ != nil {
				hs.SideZInErrors = ibs.sideZ.inErrors
				hs.SideZOutErrors = ibs.sideZ.outErrors
				hs.SideZInFcsErrors = ibs.sideZ.inFcsErrors
				hs.SideZInDiscards = ibs.sideZ.inDiscards
				hs.SideZOutDiscards = ibs.sideZ.outDiscards
				hs.SideZCarrierTransitions = ibs.sideZ.carrierTransitions
			}
		}

		// Determine status using the same classification as the multi-link endpoint
		bs := bucketMap[key]
		if hs.Samples == 0 {
			hs.Status = "no_data"
		} else {
			hs.Status = classifyLinkStatus(hs.AvgLatencyUs, hs.AvgLossPct, committedRttUs)

			// If only one direction is reporting, the missing side likely
			// can't send probes. For completed buckets, treat as unhealthy.
			// For the collecting bucket, treat as no_data since we don't
			// have enough data to classify health yet.
			if hs.DrainStatus == "" && bs != nil && (bs.sideA == nil) != (bs.sideZ == nil) {
				if hs.Collecting {
					hs.Status = "no_data"
				} else if hs.Status == "healthy" || hs.Status == "degraded" {
					hs.Status = "unhealthy"
				}
			}
		}

		// Upgrade status based on interface issues (same thresholds as multi-link endpoint)
		const InterfaceUnhealthyThreshold = uint64(100)
		totalErrors := hs.SideAInErrors + hs.SideAOutErrors + hs.SideZInErrors + hs.SideZOutErrors
		totalDiscards := hs.SideAInDiscards + hs.SideAOutDiscards + hs.SideZInDiscards + hs.SideZOutDiscards
		totalCarrier := hs.SideACarrierTransitions + hs.SideZCarrierTransitions

		if totalErrors >= InterfaceUnhealthyThreshold || totalDiscards >= InterfaceUnhealthyThreshold || totalCarrier >= InterfaceUnhealthyThreshold {
			if hs.Status == "healthy" || hs.Status == "degraded" {
				hs.Status = "unhealthy"
			}
		} else if (totalErrors > 0 || totalDiscards > 0 || totalCarrier > 0) && hs.Status == "healthy" {
			hs.Status = "degraded"
		}

		// Check ISIS adjacency state for this bucket
		if resolveISISDown(key) {
			hs.ISISDown = true
		}

		hourStatuses = append(hourStatuses, hs)
	}

	return &SingleLinkHistoryResponse{
		PK:             linkPK,
		Code:           code,
		CommittedRttUs: committedRttUs,
		Hours:          hourStatuses,
		TimeRange:      timeRange,
		BucketMinutes:  bucketMinutes,
		BucketCount:    bucketCount,
	}, nil
}

// SingleDeviceHistoryResponse is the response for single device history endpoint
type SingleDeviceHistoryResponse struct {
	PK            string             `json:"pk"`
	Code          string             `json:"code"`
	DeviceType    string             `json:"device_type"`
	Contributor   string             `json:"contributor"`
	Metro         string             `json:"metro"`
	MaxUsers      int32              `json:"max_users"`
	Hours         []DeviceHourStatus `json:"hours"`
	IssueReasons  []string           `json:"issue_reasons"`
	TimeRange     string             `json:"time_range"`
	BucketMinutes int                `json:"bucket_minutes"`
	BucketCount   int                `json:"bucket_count"`
}

// GetSingleDeviceHistory returns the status history for a single device
func GetSingleDeviceHistory(w http.ResponseWriter, r *http.Request) {
	devicePK := chi.URLParam(r, "pk")
	if devicePK == "" {
		http.Error(w, "missing device pk", http.StatusBadRequest)
		return
	}

	// Parse time range parameter
	timeRange := r.URL.Query().Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}

	// Parse optional bucket count
	requestedBuckets := 24 // default
	if b := r.URL.Query().Get("buckets"); b != "" {
		if n, err := strconv.Atoi(b); err == nil && n >= 12 && n <= 10000 {
			requestedBuckets = n
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	resp, err := fetchSingleDeviceHistoryData(ctx, devicePK, timeRange, requestedBuckets)
	if err != nil {
		slog.Error("error fetching single device history", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if resp == nil {
		http.Error(w, "Device not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

func fetchSingleDeviceHistoryData(ctx context.Context, devicePK string, timeRange string, requestedBuckets int) (*SingleDeviceHistoryResponse, error) {
	// Calculate bucket size based on time range and requested buckets
	var totalMinutes int
	switch timeRange {
	case "1h":
		totalMinutes = 60
	case "3h":
		totalMinutes = 3 * 60
	case "6h":
		totalMinutes = 6 * 60
	case "12h":
		totalMinutes = 12 * 60
	case "3d":
		totalMinutes = 3 * 24 * 60
	case "7d":
		totalMinutes = 7 * 24 * 60
	default: // "24h"
		timeRange = "24h"
		totalMinutes = 24 * 60
	}

	bucketMinutes := snapBucketMinutes(totalMinutes / requestedBuckets)
	bucketCount := totalMinutes / bucketMinutes
	totalHours := totalMinutes / 60
	bucketDuration := time.Duration(bucketMinutes) * time.Minute
	now := time.Now().UTC()

	// Build the bucket interval expression
	var bucketInterval string
	if bucketMinutes >= 60 && bucketMinutes%60 == 0 {
		bucketInterval = fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %d HOUR, 'UTC')", bucketMinutes/60)
	} else {
		bucketInterval = fmt.Sprintf("toStartOfInterval(event_ts, INTERVAL %d MINUTE, 'UTC')", bucketMinutes)
	}

	// Get device metadata
	deviceQuery := `
		SELECT d.code, d.device_type, COALESCE(c.code, '') as contributor,
		       m.code as metro, d.max_users, d.status
		FROM dz_devices_current d
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		WHERE d.pk = ?
	`
	var code, deviceType, contributor, metro, status string
	var maxUsers int32
	err := envDB(ctx).QueryRow(ctx, deviceQuery, devicePK).Scan(&code, &deviceType, &contributor, &metro, &maxUsers, &status)
	if err != nil {
		return nil, nil // Device not found
	}

	// Check if device is currently drained
	isCurrentlyDrained := status == "soft-drained" || status == "hard-drained" || status == "suspended"
	issueReasons := make(map[string]bool)
	if isCurrentlyDrained {
		issueReasons["drained"] = true
	}

	// Get interface counters per bucket
	interfaceQuery := `
		SELECT
			` + bucketInterval + ` as bucket,
			toUInt64(SUM(greatest(0, in_errors_delta))) as in_errors,
			toUInt64(SUM(greatest(0, out_errors_delta))) as out_errors,
			toUInt64(SUM(greatest(0, in_fcs_errors_delta))) as in_fcs_errors,
			toUInt64(SUM(greatest(0, in_discards_delta))) as in_discards,
			toUInt64(SUM(greatest(0, out_discards_delta))) as out_discards,
			toUInt64(SUM(greatest(0, carrier_transitions_delta))) as carrier_transitions
		FROM fact_dz_device_interface_counters
		WHERE device_pk = ? AND event_ts > now() - INTERVAL ? HOUR
		GROUP BY bucket
		ORDER BY bucket
	`

	interfaceRows, err := envDB(ctx).Query(ctx, interfaceQuery, devicePK, totalHours)
	if err != nil {
		return nil, fmt.Errorf("device interface query error: %w", err)
	}
	defer interfaceRows.Close()

	type bucketStats struct {
		bucket             time.Time
		inErrors           uint64
		outErrors          uint64
		inFcsErrors        uint64
		inDiscards         uint64
		outDiscards        uint64
		carrierTransitions uint64
	}
	bucketMap := make(map[string]bucketStats)

	for interfaceRows.Next() {
		var stats bucketStats
		if err := interfaceRows.Scan(&stats.bucket, &stats.inErrors, &stats.outErrors, &stats.inFcsErrors, &stats.inDiscards, &stats.outDiscards, &stats.carrierTransitions); err != nil {
			return nil, fmt.Errorf("interface scan error: %w", err)
		}
		key := stats.bucket.UTC().Format(time.RFC3339)
		bucketMap[key] = stats

		// Track issue reasons
		totalErrors := stats.inErrors + stats.outErrors
		totalDiscards := stats.inDiscards + stats.outDiscards
		if totalErrors > 0 {
			issueReasons["interface_errors"] = true
		}
		if stats.inFcsErrors > 0 {
			issueReasons["fcs_errors"] = true
		}
		if totalDiscards > 0 {
			issueReasons["discards"] = true
		}
		if stats.carrierTransitions > 0 {
			issueReasons["carrier_transitions"] = true
		}
	}

	// Get latency probe presence per bucket for this device
	probeQuery := `
		SELECT
			` + bucketInterval + ` as bucket,
			count(*) as samples
		FROM fact_dz_device_link_latency
		WHERE origin_device_pk = ? AND event_ts > now() - INTERVAL ? HOUR
		GROUP BY bucket
		ORDER BY bucket
	`
	probeRows, err := envDB(ctx).Query(ctx, probeQuery, devicePK, totalHours)
	if err != nil {
		return nil, fmt.Errorf("device probe query error: %w", err)
	}
	defer probeRows.Close()

	deviceProbes := make(map[string]bool)
	for probeRows.Next() {
		var bucket time.Time
		var samples uint64
		if err := probeRows.Scan(&bucket, &samples); err != nil {
			return nil, fmt.Errorf("probe scan error: %w", err)
		}
		if samples > 0 {
			deviceProbes[bucket.UTC().Format(time.RFC3339)] = true
		}
	}

	// Get historical device status per bucket
	var historyBucketInterval string
	if bucketMinutes >= 60 && bucketMinutes%60 == 0 {
		historyBucketInterval = fmt.Sprintf("toStartOfInterval(snapshot_ts, INTERVAL %d HOUR, 'UTC')", bucketMinutes/60)
	} else {
		historyBucketInterval = fmt.Sprintf("toStartOfInterval(snapshot_ts, INTERVAL %d MINUTE, 'UTC')", bucketMinutes)
	}

	statusHistoryQuery := `
		SELECT
			` + historyBucketInterval + ` as bucket,
			argMax(status, snapshot_ts) as status
		FROM dim_dz_devices_history
		WHERE pk = ? AND snapshot_ts > now() - INTERVAL ? HOUR
		GROUP BY bucket
		ORDER BY bucket
	`

	statusRows, err := envDB(ctx).Query(ctx, statusHistoryQuery, devicePK, totalHours)
	if err != nil {
		slog.Error("device status history query error", "error", err)
		// Non-fatal - continue without historical status
	}

	deviceStatusHistory := make(map[string]string)
	if statusRows != nil {
		defer statusRows.Close()
		for statusRows.Next() {
			var deviceStatus string
			var bucket time.Time
			if err := statusRows.Scan(&bucket, &deviceStatus); err != nil {
				return nil, fmt.Errorf("device status history scan error: %w", err)
			}
			key := bucket.UTC().Format(time.RFC3339)
			deviceStatusHistory[key] = deviceStatus

			// Check if device was drained in history
			if deviceStatus == "soft-drained" || deviceStatus == "hard-drained" || deviceStatus == "suspended" {
				issueReasons["drained"] = true
			}
		}
	}

	// Build bucket status array including the current collecting bucket.
	var hourStatuses []DeviceHourStatus
	for i := bucketCount - 1; i >= 0; i-- {
		bucketStart := now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
		key := bucketStart.UTC().Format(time.RFC3339)
		isCollecting := i == 0

		// Check historical status for this bucket
		historicalStatus, hasHistory := deviceStatusHistory[key]
		wasDrained := hasHistory && (historicalStatus == "soft-drained" || historicalStatus == "hard-drained" || historicalStatus == "suspended")

		// If device was drained at this time, show as disabled
		if wasDrained {
			hourStatuses = append(hourStatuses, DeviceHourStatus{
				Hour:        key,
				Status:      "disabled",
				Collecting:  isCollecting,
				DrainStatus: historicalStatus,
				MaxUsers:    maxUsers,
			})
			continue
		}

		// Check if we have interface data for this bucket
		if stats, ok := bucketMap[key]; ok {
			deviceStatus := classifyDeviceStatus(stats.inErrors+stats.outErrors+stats.inFcsErrors, stats.inDiscards+stats.outDiscards, stats.carrierTransitions)

			// If device has interface data but no latency probes, mark unhealthy.
			noProbes := !isCollecting && !deviceProbes[key]
			if noProbes {
				if deviceStatus == "healthy" || deviceStatus == "degraded" {
					deviceStatus = "unhealthy"
				}
				issueReasons["no_probes"] = true
			}

			hourStatuses = append(hourStatuses, DeviceHourStatus{
				Hour:               key,
				Status:             deviceStatus,
				Collecting:         isCollecting,
				MaxUsers:           maxUsers,
				InErrors:           stats.inErrors,
				OutErrors:          stats.outErrors,
				InFcsErrors:        stats.inFcsErrors,
				InDiscards:         stats.inDiscards,
				OutDiscards:        stats.outDiscards,
				CarrierTransitions: stats.carrierTransitions,
				NoProbes:           noProbes,
			})
		} else {
			// No interface data for this bucket — show as no_data.
			hourStatuses = append(hourStatuses, DeviceHourStatus{
				Hour:       key,
				Status:     "no_data",
				Collecting: isCollecting,
				MaxUsers:   maxUsers,
			})
		}
	}

	// Convert issue reasons to slice
	var issueReasonsList []string
	for reason := range issueReasons {
		issueReasonsList = append(issueReasonsList, reason)
	}
	sort.Strings(issueReasonsList)

	return &SingleDeviceHistoryResponse{
		PK:            devicePK,
		Code:          code,
		DeviceType:    deviceType,
		Contributor:   contributor,
		Metro:         metro,
		MaxUsers:      maxUsers,
		Hours:         hourStatuses,
		IssueReasons:  issueReasonsList,
		TimeRange:     timeRange,
		BucketMinutes: bucketMinutes,
		BucketCount:   bucketCount,
	}, nil
}
