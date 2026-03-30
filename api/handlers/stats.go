package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
	"golang.org/x/sync/errgroup"
)

type StatsResponse struct {
	ValidatorsOnDZ uint64  `json:"validators_on_dz"`
	TotalStakeSol  float64 `json:"total_stake_sol"`
	StakeSharePct  float64 `json:"stake_share_pct"`
	Users          uint64  `json:"users"`
	Devices        uint64  `json:"devices"`
	Links          uint64  `json:"links"`
	Contributors   uint64  `json:"contributors"`
	Metros         uint64  `json:"metros"`
	BandwidthBps   int64   `json:"bandwidth_bps"`
	UserInboundBps float64 `json:"user_inbound_bps"`
	FetchedAt      string  `json:"fetched_at"`
	Error          string  `json:"error,omitempty"`
}

func (a *API) GetStats(w http.ResponseWriter, r *http.Request) {
	// Try to derive stats from the status cache (cache only holds mainnet data)
	if isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), "status"); err == nil {
			var cached StatusResponse
			if json.Unmarshal(data, &cached) == nil {
				stats := StatsResponse{
					ValidatorsOnDZ: cached.Network.ValidatorsOnDZ,
					TotalStakeSol:  cached.Network.TotalStakeSol,
					StakeSharePct:  cached.Network.StakeSharePct,
					Users:          cached.Network.Users,
					Devices:        cached.Network.Devices,
					Links:          cached.Network.Links,
					Contributors:   cached.Network.Contributors,
					Metros:         cached.Network.Metros,
					BandwidthBps:   cached.Network.BandwidthBps,
					UserInboundBps: cached.Network.UserInboundBps,
					FetchedAt:      cached.Timestamp,
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Cache", "HIT")
				if err := json.NewEncoder(w).Encode(stats); err != nil {
					logError("failed to encode response", "error", err)
				}
				return
			}
		}
	}

	// Cache miss - fetch fresh data
	w.Header().Set("X-Cache", "MISS")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()

	stats := StatsResponse{
		FetchedAt: time.Now().UTC().Format(time.RFC3339),
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(10)

	// Count validators on DZ (Solana validators connected via dz_users)
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
		return row.Scan(&stats.ValidatorsOnDZ)
	})

	// Sum total stake for validators on DZ (in lamports, convert to SOL)
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
		return row.Scan(&stats.TotalStakeSol)
	})

	// Calculate stake share percentage (connected stake / total network stake * 100)
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
		return row.Scan(&stats.StakeSharePct)
	})

	// Count users
	g.Go(func() error {
		query := `SELECT COUNT(*) FROM dz_users_current`
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&stats.Users)
	})

	// Count devices
	g.Go(func() error {
		query := `SELECT COUNT(*) FROM dz_devices_current`
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&stats.Devices)
	})

	// Count links
	g.Go(func() error {
		query := `SELECT COUNT(*) FROM dz_links_current`
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&stats.Links)
	})

	// Count contributors
	g.Go(func() error {
		query := `SELECT COUNT(DISTINCT pk) FROM dz_contributors_current`
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&stats.Contributors)
	})

	// Count metros
	g.Go(func() error {
		query := `SELECT COUNT(DISTINCT pk) FROM dz_metros_current`
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&stats.Metros)
	})

	// Sum total bandwidth for all links
	g.Go(func() error {
		query := `SELECT COALESCE(SUM(bandwidth_bps), 0) FROM dz_links_current`
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&stats.BandwidthBps)
	})

	// Calculate total user inbound traffic rate (bps) over last hour
	g.Go(func() error {
		query := `
			SELECT COALESCE(SUM(avg_in_bps), 0)
			FROM device_interface_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL 15 MINUTE
			  AND user_tunnel_id IS NOT NULL
		`
		row := a.envDB(ctx).QueryRow(ctx, query)
		return row.Scan(&stats.UserInboundBps)
	})

	err := g.Wait()
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("stats query failed", "error", err)
		stats.Error = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		logError("failed to encode response", "error", err)
	}
}
