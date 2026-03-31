package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/health"
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

// Thresholds re-exported from the shared health package for local use.
const (
	LatencyWarningPct  = health.LatencyWarningPct
	LatencyCriticalPct = health.LatencyCriticalPct
	LossWarningPct     = health.LossWarningPct
	LossCriticalPct    = health.LossCriticalPct
	UtilWarningPct     = health.UtilWarningPct
	UtilCriticalPct    = health.UtilCriticalPct

	committedRttProvisioningNs = health.CommittedRttProvisioningNs
)

func (a *API) GetStatus(w http.ResponseWriter, r *http.Request) {
	// Try to serve from cache first (cache only holds mainnet data)
	if isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), "status"); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	// Cache miss - fetch fresh data
	w.Header().Set("X-Cache", "MISS")
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	resp := a.FetchStatusData(ctx)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// FetchStatusData performs the actual status data fetch from the database.
// This is called by both the cache refresh and direct requests.
func (a *API) FetchStatusData(ctx context.Context) *StatusResponse {
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
		if err := a.DB.Ping(pingCtx); err != nil {
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
			SELECT formatDateTime(max(bucket_ts), '%Y-%m-%dT%H:%i:%sZ', 'UTC')
			FROM link_rollup_5m
			WHERE bucket_ts > now() - INTERVAL 1 HOUR
		`
		row := a.envDB(ctx).QueryRow(ctx, query)
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
		row := a.envDB(ctx).QueryRow(ctx, query)
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
		row := a.envDB(ctx).QueryRow(ctx, query)
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
		row := a.envDB(ctx).QueryRow(ctx, query)
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
		row := a.envDB(ctx).QueryRow(ctx, query)
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
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.Users)
	})

	g.Go(func() error {
		query := `SELECT COUNT(*) FROM dz_devices_current`
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.Devices)
	})

	g.Go(func() error {
		query := `SELECT COUNT(*) FROM dz_links_current`
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.Links)
	})

	g.Go(func() error {
		query := `SELECT COUNT(DISTINCT pk) FROM dz_contributors_current`
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.Contributors)
	})

	g.Go(func() error {
		query := `SELECT COUNT(DISTINCT pk) FROM dz_metros_current`
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.Metros)
	})

	// Sum total bandwidth for all links
	g.Go(func() error {
		query := `SELECT COALESCE(SUM(bandwidth_bps), 0) FROM dz_links_current`
		row := a.envDB(ctx).QueryRow(ctx, query)
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
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&resp.Network.UserInboundBps)
	})

	// Device status breakdown
	g.Go(func() error {
		query := `SELECT status, COUNT(*) as cnt FROM dz_devices_current GROUP BY status`
		rows, err := a.envDB(ctx).Query(ctx, query)
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
		rows, err := a.envDB(ctx).Query(ctx, query, committedRttProvisioningNs)
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
			LEFT JOIN (
				SELECT link_pk,
					argMax(greatest(a_loss_pct, z_loss_pct) >= 100, bucket_ts) as is_down
				FROM link_rollup_5m FINAL
				WHERE bucket_ts >= now() - INTERVAL 10 MINUTE
				GROUP BY link_pk
			) h ON l.pk = h.link_pk
			LEFT JOIN (
				SELECT link_pk,
					sum(a_avg_rtt_us * a_samples + z_avg_rtt_us * z_samples) / greatest(sum(a_samples + z_samples), 1) as avg_rtt_us,
					sum(a_loss_pct * a_samples + z_loss_pct * z_samples) / greatest(sum(a_samples + z_samples), 1) as loss_percent
				FROM link_rollup_5m FINAL
				WHERE bucket_ts >= now() - INTERVAL 1 HOUR
				GROUP BY link_pk
			) lat ON l.pk = lat.link_pk
			-- Direct link traffic (where link_pk is populated)
			LEFT JOIN (
				SELECT link_pk,
					max(p95_in_bps) as in_bps,
					max(p95_out_bps) as out_bps
				FROM device_interface_rollup_5m
				WHERE bucket_ts >= now() - INTERVAL 24 HOUR
					AND link_pk != ''
				GROUP BY link_pk
			) traffic_direct ON l.pk = traffic_direct.link_pk
			-- Parent interface traffic (for sub-interfaces like PortChannel2000.10023)
			LEFT JOIN (
				SELECT device_pk, intf,
					max(p95_in_bps) as in_bps,
					max(p95_out_bps) as out_bps
				FROM device_interface_rollup_5m
				WHERE bucket_ts >= now() - INTERVAL 24 HOUR
				GROUP BY device_pk, intf
			) traffic_parent ON traffic_parent.device_pk = l.side_a_pk
				AND traffic_parent.intf = splitByChar('.', l.side_a_iface_name)[1]
				AND traffic_direct.link_pk IS NULL
			WHERE l.status = 'activated'
				AND l.isis_delay_override_ns != ?
				AND l.committed_rtt_ns != ?
		`
		rows, err := a.envDB(ctx).Query(ctx, query, delayOverrideSoftDrainedNs, committedRttProvisioningNs)
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
						r.bucket_ts as bucket,
						greatest(r.a_loss_pct, r.z_loss_pct) as loss_pct
					FROM link_rollup_5m r
					JOIN dz_links_current l ON r.link_pk = l.pk
					WHERE r.bucket_ts >= now() - INTERVAL 7 DAY
					  AND l.code IN (?)
					  AND (r.a_samples + r.z_samples) >= 3
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
			issueRows, err := a.envDB(ctx).Query(ctx, issueStartQuery, linkCodes, LossWarningPct, LossWarningPct, LossWarningPct, LossWarningPct)
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
					max(bucket_ts) as last_seen
				FROM link_rollup_5m
				WHERE bucket_ts >= now() - INTERVAL 30 DAY
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
		noDataRows, err := a.envDB(ctx).Query(ctx, noDataQuery, committedRttProvisioningNs)
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
				ifNotFinite(sum((a_avg_rtt_us * a_samples + z_avg_rtt_us * z_samples)) / greatest(sum(a_samples + z_samples), 1), 0) as avg_latency,
				ifNotFinite(max(greatest(a_p95_rtt_us, z_p95_rtt_us)), 0) as p95_latency,
				ifNotFinite(min(least(a_min_rtt_us, z_min_rtt_us)), 0) as min_latency,
				ifNotFinite(max(greatest(a_max_rtt_us, z_max_rtt_us)), 0) as max_latency,
				ifNotFinite(sum(a_loss_pct * a_samples + z_loss_pct * z_samples) / greatest(sum(a_samples + z_samples), 1), 0) as avg_loss,
				ifNotFinite(sum((a_avg_jitter_us * a_samples + z_avg_jitter_us * z_samples)) / greatest(sum(a_samples + z_samples), 1), 0) as avg_jitter
			FROM link_rollup_5m r
			JOIN dz_links_current l ON r.link_pk = l.pk
			WHERE r.bucket_ts >= now() - INTERVAL 3 HOUR
			  AND l.link_type = 'WAN'
		`
		row := a.envDB(ctx).QueryRow(ctx, query)
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
				COALESCE(SUM(avg_in_bps), 0) as total_in_bps,
				COALESCE(SUM(avg_out_bps), 0) as total_out_bps
			FROM device_interface_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL 5 MINUTE
			  AND link_pk != ''
		`
		row := a.envDB(ctx).QueryRow(ctx, query)
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
		rows, err := a.envDB(ctx).Query(ctx, query)
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
		rows, err := a.envDB(ctx).Query(ctx, query)
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
		rows, err := a.envDB(ctx).Query(ctx, query)
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
		rows, err := a.envDB(ctx).Query(ctx, query, committedRttProvisioningNs, committedRttProvisioningNs)
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
		rows, err := a.envDB(ctx).Query(ctx, query)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("status: failed to query ISIS device issues", "error", err)
			}
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
		rows, err := a.envDB(ctx).Query(ctx, query, committedRttProvisioningNs)
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("status: failed to query missing ISIS adjacencies", "error", err)
			}
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
			sinceRows, sinceErr := a.envDB(ctx).Query(ctx, sinceQuery, linkPKs)
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

func (a *API) GetLinkHistory(w http.ResponseWriter, r *http.Request) {
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

	// Try to serve from cache first (cache only holds mainnet data, skip for raw source)
	if isMainnet(r.Context()) && r.URL.Query().Get("source") == "" {
		cacheKey := "link_history:" + timeRange + ":" + strconv.Itoa(requestedBuckets)
		if data, err := a.readPageCache(r.Context(), cacheKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
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
	ctx, cancel := statusContext(r, timeout)
	defer cancel()

	filters := parseStatusFilterParam(r.URL.Query().Get("filter"))
	resp, err := a.FetchLinkHistoryData(ctx, timeRange, requestedBuckets, filters...)
	if err != nil {
		if dberror.IsTransient(err) {
			cancel()
			var retryCancel context.CancelFunc
			ctx, retryCancel = context.WithTimeout(r.Context(), timeout)
			defer retryCancel()
			resp, err = a.FetchLinkHistoryData(ctx, timeRange, requestedBuckets, filters...)
		}
	}
	if err != nil {
		slog.Warn("link history query failed", "error", err)
		http.Error(w, "Failed to fetch link history", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode response", "error", err)
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

// FetchLinkHistoryData delegates to the rollup-based implementation.
func (a *API) FetchLinkHistoryData(ctx context.Context, timeRange string, requestedBuckets int, filters ...statusFilter) (*LinkHistoryResponse, error) {
	return a.fetchLinkHistoryFromRollup(ctx, timeRange, requestedBuckets, filters...)
}

// statusContext returns a context with raw source flag set if ?source=raw is present.
func statusContext(r *http.Request, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx := r.Context()
	if r.URL.Query().Get("source") == "raw" {
		ctx = withRawSource(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func classifyLinkStatus(avgLatency, lossPct, committedRttUs float64) string {
	return health.ClassifyLinkStatus(avgLatency, lossPct, committedRttUs)
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

	// Check aggregate performance, but only if individual links are actually
	// degraded or unhealthy. The network average can exceed thresholds due to
	// a few links with minor loss that individually classify as healthy.
	if resp.Links.Degraded > 0 || resp.Links.Unhealthy > 0 || resp.Links.Down > 0 {
		if resp.Performance.AvgLossPercent >= LossCriticalPct {
			return "unhealthy"
		}
		if resp.Performance.AvgLossPercent >= LossWarningPct {
			return "degraded"
		}
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

func (a *API) GetDeviceHistory(w http.ResponseWriter, r *http.Request) {
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

	// Try to serve from cache first (cache only holds mainnet data, skip for raw source)
	if isMainnet(r.Context()) && r.URL.Query().Get("source") == "" {
		cacheKey := "device_history:" + timeRange + ":" + strconv.Itoa(requestedBuckets)
		if data, err := a.readPageCache(r.Context(), cacheKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	// Cache miss - fetch fresh data
	w.Header().Set("X-Cache", "MISS")
	ctx, cancel := statusContext(r, 20*time.Second)
	defer cancel()

	filters := parseStatusFilterParam(r.URL.Query().Get("filter"))
	resp, err := a.FetchDeviceHistoryData(ctx, timeRange, requestedBuckets, filters...)
	if err != nil {
		logError("fetchDeviceHistoryData error", "error", err)
		http.Error(w, "Failed to fetch device history", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// FetchDeviceHistoryData delegates to the rollup-based implementation.
func (a *API) FetchDeviceHistoryData(ctx context.Context, timeRange string, requestedBuckets int, filters ...statusFilter) (*DeviceHistoryResponse, error) {
	return a.fetchDeviceHistoryFromRollup(ctx, timeRange, requestedBuckets, filters...)
}

func classifyDeviceStatus(totalErrors, totalDiscards uint64, carrierTransitions uint64) string { //nolint:unused
	return health.ClassifyDeviceStatus(totalErrors, totalDiscards, carrierTransitions)
}

// InterfaceIssuesResponse is the response for interface issues endpoint
type InterfaceIssuesResponse struct {
	Issues    []InterfaceIssue `json:"issues"`
	TimeRange string           `json:"time_range"`
}

// GetInterfaceIssues returns interface issues for a given time range
func (a *API) GetInterfaceIssues(w http.ResponseWriter, r *http.Request) {
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

	ctx, cancel := statusContext(r, 15*time.Second)
	defer cancel()

	issues, err := a.fetchInterfaceIssuesData(ctx, duration)
	if err != nil {
		logError("error fetching interface issues", "error", err)
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

func (a *API) fetchInterfaceIssuesData(ctx context.Context, duration time.Duration) ([]InterfaceIssue, error) {
	return a.fetchInterfaceIssuesFromRollup(ctx, duration)
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
func (a *API) GetDeviceInterfaceHistory(w http.ResponseWriter, r *http.Request) {
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

	ctx, cancel := statusContext(r, 15*time.Second)
	defer cancel()

	resp, err := a.fetchDeviceInterfaceHistoryData(ctx, devicePK, timeRange, requestedBuckets)
	if err != nil {
		logError("error fetching device interface history", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (a *API) fetchDeviceInterfaceHistoryData(ctx context.Context, devicePK string, timeRange string, requestedBuckets int) (*DeviceInterfaceHistoryResponse, error) {
	return a.fetchDeviceInterfaceHistoryFromRollup(ctx, devicePK, timeRange, requestedBuckets)
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
func (a *API) GetSingleLinkHistory(w http.ResponseWriter, r *http.Request) {
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

	ctx, cancel := statusContext(r, 15*time.Second)
	defer cancel()

	resp, err := a.fetchSingleLinkHistoryData(ctx, linkPK, timeRange, requestedBuckets)
	if err != nil {
		logError("error fetching single link history", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if resp == nil {
		http.Error(w, "Link not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode response", "error", err)
	}
}

func (a *API) fetchSingleLinkHistoryData(ctx context.Context, linkPK string, timeRange string, requestedBuckets int) (*SingleLinkHistoryResponse, error) {
	return a.fetchSingleLinkHistoryFromRollup(ctx, linkPK, timeRange, requestedBuckets)
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
func (a *API) GetSingleDeviceHistory(w http.ResponseWriter, r *http.Request) {
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

	ctx, cancel := statusContext(r, 15*time.Second)
	defer cancel()

	resp, err := a.fetchSingleDeviceHistoryData(ctx, devicePK, timeRange, requestedBuckets)
	if err != nil {
		logError("error fetching single device history", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if resp == nil {
		http.Error(w, "Device not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode response", "error", err)
	}
}

func (a *API) fetchSingleDeviceHistoryData(ctx context.Context, devicePK string, timeRange string, requestedBuckets int) (*SingleDeviceHistoryResponse, error) {
	return a.fetchSingleDeviceHistoryFromRollup(ctx, devicePK, timeRange, requestedBuckets)
}
