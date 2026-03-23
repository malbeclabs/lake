package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/metrics"
)

type ValidatorListItem struct {
	VotePubkey     string  `json:"vote_pubkey"`
	NodePubkey     string  `json:"node_pubkey"`
	StakeSol       float64 `json:"stake_sol"`
	StakeShare     float64 `json:"stake_share"`
	Commission     int64   `json:"commission"`
	OnDZ           bool    `json:"on_dz"`
	DeviceCode     string  `json:"device_code"`
	MetroCode      string  `json:"metro_code"`
	City           string  `json:"city"`
	Country        string  `json:"country"`
	InBps          float64 `json:"in_bps"`
	OutBps         float64 `json:"out_bps"`
	SkipRate       float64 `json:"skip_rate"`
	Version        string  `json:"version"`
	SoftwareClient string  `json:"software_client"`
}

type ValidatorListResponse struct {
	Items     []ValidatorListItem `json:"items"`
	Total     int                 `json:"total"`
	OnDZCount int                 `json:"on_dz_count"`
	Limit     int                 `json:"limit"`
	Offset    int                 `json:"offset"`
}

var validatorSortFields = map[string]string{
	"vote":       "v.vote_pubkey",
	"node":       "v.node_pubkey",
	"stake":      "v.activated_stake_lamports",
	"share":      "v.activated_stake_lamports",
	"commission": "COALESCE(v.commission_percentage, 0)",
	"dz":         "on_dz",
	"device":     "device_code",
	"city":       "city",
	"country":    "country",
	"in":         "in_bps",
	"out":        "out_bps",
	"skip":       "skip_rate",
	"version":    "version",
	"client":     "software_client",
}

var validatorFilterFields = map[string]FilterFieldConfig{
	"vote":       {Column: "vote_pubkey", Type: FieldTypeText},
	"node":       {Column: "node_pubkey", Type: FieldTypeText},
	"stake":      {Column: "stake_sol", Type: FieldTypeStake},
	"share":      {Column: "stake_share", Type: FieldTypeNumeric},
	"commission": {Column: "commission", Type: FieldTypeNumeric},
	"dz":         {Column: "on_dz", Type: FieldTypeBoolean},
	"device":     {Column: "device_code", Type: FieldTypeText},
	"city":       {Column: "city", Type: FieldTypeText},
	"country":    {Column: "country", Type: FieldTypeText},
	"in":         {Column: "in_bps", Type: FieldTypeBandwidth},
	"out":        {Column: "out_bps", Type: FieldTypeBandwidth},
	"skip":       {Column: "skip_rate", Type: FieldTypeNumeric},
	"version":    {Column: "version", Type: FieldTypeText},
	"client":     {Column: "software_client", Type: FieldTypeText},
}

func GetValidators(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "stake", validatorSortFields)
	filters := ParseFilters(r)
	start := time.Now()

	// Build filter clause
	filterClause, filterArgs := filters.BuildFilterClause(validatorFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}

	// Single query using window functions for counts to avoid repeating expensive CTEs.
	// NOTE: We avoid JOINing _current views (which use window functions) with each other
	// directly, as ClickHouse incorrectly correlates the window functions across views
	// in the same JOIN chain. Instead, we use IN for the on_dz boolean check and join
	// the DZ metadata (dz_ip_info) separately via gossip_ip after the gossip join.

	// Build sort clause using output column names
	sortFieldsForQuery := map[string]string{
		"vote":       "vote_pubkey",
		"node":       "node_pubkey",
		"stake":      "activated_stake_lamports",
		"share":      "activated_stake_lamports",
		"commission": "commission",
		"dz":         "on_dz",
		"device":     "device_code",
		"city":       "city",
		"country":    "country",
		"in":         "in_bps",
		"out":        "out_bps",
		"skip":       "skip_rate",
		"version":    "version",
		"client":     "software_client",
	}
	orderBy := sort.OrderByClause(sortFieldsForQuery)

	query := `
		WITH total_stake AS (
			SELECT sum(activated_stake_lamports) as total
			FROM solana_vote_accounts_current
			WHERE epoch_vote_account = 'true'
		),
		dz_ip_info AS (
			SELECT
				u.client_ip,
				any(u.pk) as user_pk,
				any(d.pk) as device_pk,
				any(d.code) as device_code,
				any(m.code) as metro_code
			FROM dz_users_current u
			JOIN dz_devices_current d ON u.device_pk = d.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			WHERE u.status = 'activated'
				AND u.client_ip IS NOT NULL
				AND u.client_ip != ''
			GROUP BY u.client_ip
		),
		traffic_rates AS (
			SELECT
				user_pk,
				SUM(avg_in_bps) as in_bps,
				SUM(avg_out_bps) as out_bps
			FROM device_interface_rollup_5m
			WHERE bucket_ts = (SELECT max(bucket_ts) FROM device_interface_rollup_5m)
				AND user_pk != ''
			GROUP BY user_pk
		),
		skip_rates AS (
			SELECT
				leader_identity_pubkey,
				ROUND(
					(MAX(leader_slots_assigned_cum) - MAX(blocks_produced_cum)) * 100.0
					/ NULLIF(MAX(leader_slots_assigned_cum), 0),
					2
				) as skip_rate
			FROM fact_solana_block_production
			WHERE event_ts > now() - INTERVAL 24 HOUR
			GROUP BY leader_identity_pubkey
			HAVING MAX(leader_slots_assigned_cum) > 0
		),
		validatorsapp_data AS (
			SELECT
				vote_account,
				software_client
			FROM validatorsapp_validators_current
		),
		validators_with_gossip AS (
			SELECT
				v.vote_pubkey,
				v.node_pubkey,
				v.activated_stake_lamports,
				v.activated_stake_lamports / 1e9 as stake_sol,
				CASE WHEN ts.total > 0
					THEN v.activated_stake_lamports * 100.0 / ts.total
					ELSE 0
				END as stake_share,
				COALESCE(v.commission_percentage, 0) as commission,
				g.gossip_ip,
				g.gossip_ip IN (SELECT client_ip FROM dz_ip_info) as on_dz,
				COALESCE(geo.city, '') as city,
				COALESCE(geo.country, '') as country,
				COALESCE(sr.skip_rate, 0) as skip_rate,
				COALESCE(g.version, '') as version,
				COALESCE(va.software_client, '') as software_client
			FROM solana_vote_accounts_current v
			CROSS JOIN total_stake ts
			LEFT JOIN solana_gossip_nodes_current g ON v.node_pubkey = g.pubkey
			LEFT JOIN geoip_records_current geo ON g.gossip_ip = geo.ip
			LEFT JOIN skip_rates sr ON v.node_pubkey = sr.leader_identity_pubkey
			LEFT JOIN validatorsapp_data va ON v.vote_pubkey = va.vote_account
			WHERE v.epoch_vote_account = 'true'
				AND v.activated_stake_lamports > 0
		),
		validators_data AS (
			SELECT
				vg.vote_pubkey,
				vg.node_pubkey,
				vg.activated_stake_lamports,
				vg.stake_sol,
				vg.stake_share,
				vg.commission,
				vg.on_dz,
				COALESCE(di.device_code, '') as device_code,
				COALESCE(di.metro_code, '') as metro_code,
				vg.city,
				vg.country,
				COALESCE(tr.in_bps, 0) as in_bps,
				COALESCE(tr.out_bps, 0) as out_bps,
				vg.skip_rate,
				vg.version,
				vg.software_client
			FROM validators_with_gossip vg
			LEFT JOIN dz_ip_info di ON vg.gossip_ip = di.client_ip
			LEFT JOIN traffic_rates tr ON di.user_pk = tr.user_pk
		)
		SELECT vote_pubkey, node_pubkey, stake_sol, stake_share, commission,
			on_dz, device_code, metro_code, city, country, in_bps, out_bps, skip_rate, version,
			software_client,
			count() OVER () as _total,
			countIf(on_dz = true) OVER () as _on_dz_count
		FROM validators_data
		WHERE 1=1` + whereFilter + `
		` + orderBy + `
		LIMIT ? OFFSET ?
	`

	queryArgs := append(filterArgs, pagination.Limit, pagination.Offset)
	rows, err := envDB(ctx).Query(ctx, query, queryArgs...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		slog.Error("validators query failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var validators []ValidatorListItem
	var total, onDZCount uint64
	for rows.Next() {
		var v ValidatorListItem
		if err := rows.Scan(
			&v.VotePubkey,
			&v.NodePubkey,
			&v.StakeSol,
			&v.StakeShare,
			&v.Commission,
			&v.OnDZ,
			&v.DeviceCode,
			&v.MetroCode,
			&v.City,
			&v.Country,
			&v.InBps,
			&v.OutBps,
			&v.SkipRate,
			&v.Version,
			&v.SoftwareClient,
			&total,
			&onDZCount,
		); err != nil {
			slog.Error("validators row scan failed", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		validators = append(validators, v)
	}

	if err := rows.Err(); err != nil {
		slog.Error("validators rows iteration failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null
	if validators == nil {
		validators = []ValidatorListItem{}
	}

	response := ValidatorListResponse{
		Items:     validators,
		Total:     int(total),
		OnDZCount: int(onDZCount),
		Limit:     pagination.Limit,
		Offset:    pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

type ValidatorDetail struct {
	VotePubkey      string  `json:"vote_pubkey"`
	NodePubkey      string  `json:"node_pubkey"`
	StakeSol        float64 `json:"stake_sol"`
	StakeShare      float64 `json:"stake_share"`
	Commission      int64   `json:"commission"`
	OnDZ            bool    `json:"on_dz"`
	DevicePK        string  `json:"device_pk"`
	DeviceCode      string  `json:"device_code"`
	MetroPK         string  `json:"metro_pk"`
	MetroCode       string  `json:"metro_code"`
	City            string  `json:"city"`
	Country         string  `json:"country"`
	GossipIP        string  `json:"gossip_ip"`
	GossipPort      int32   `json:"gossip_port"`
	InBps           float64 `json:"in_bps"`
	OutBps          float64 `json:"out_bps"`
	SkipRate        float64 `json:"skip_rate"`
	Version         string  `json:"version"`
	SoftwareClient  string  `json:"software_client"`
	SoftwareVersion string  `json:"software_version"`
}

func GetValidator(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	votePubkey := chi.URLParam(r, "vote_pubkey")
	if votePubkey == "" {
		http.Error(w, "missing vote_pubkey", http.StatusBadRequest)
		return
	}

	start := time.Now()
	query := `
		WITH total_stake AS (
			SELECT sum(activated_stake_lamports) as total
			FROM solana_vote_accounts_current
			WHERE epoch_vote_account = 'true'
		),
		dz_ip_info AS (
			SELECT
				u.client_ip,
				any(u.pk) as user_pk,
				any(d.pk) as device_pk,
				any(d.code) as device_code,
				any(d.metro_pk) as metro_pk,
				any(m.code) as metro_code
			FROM dz_users_current u
			JOIN dz_devices_current d ON u.device_pk = d.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			WHERE u.status = 'activated'
				AND u.client_ip IS NOT NULL
				AND u.client_ip != ''
			GROUP BY u.client_ip
		),
		traffic_rates AS (
			SELECT
				user_pk,
				SUM(avg_in_bps) as in_bps,
				SUM(avg_out_bps) as out_bps
			FROM device_interface_rollup_5m
			WHERE bucket_ts = (SELECT max(bucket_ts) FROM device_interface_rollup_5m)
				AND user_pk != ''
			GROUP BY user_pk
		),
		skip_rates AS (
			SELECT
				leader_identity_pubkey,
				ROUND(
					(MAX(leader_slots_assigned_cum) - MAX(blocks_produced_cum)) * 100.0
					/ NULLIF(MAX(leader_slots_assigned_cum), 0),
					2
				) as skip_rate
			FROM fact_solana_block_production
			WHERE event_ts > now() - INTERVAL 24 HOUR
			GROUP BY leader_identity_pubkey
			HAVING MAX(leader_slots_assigned_cum) > 0
		),
		validatorsapp_data AS (
			SELECT
				vote_account,
				software_client,
				software_version
			FROM validatorsapp_validators_current
		)
		SELECT
			v.vote_pubkey,
			v.node_pubkey,
			v.activated_stake_lamports / 1e9 as stake_sol,
			CASE WHEN ts.total > 0
				THEN v.activated_stake_lamports * 100.0 / ts.total
				ELSE 0
			END as stake_share,
			COALESCE(v.commission_percentage, 0) as commission,
			g.gossip_ip IN (SELECT client_ip FROM dz_ip_info) as on_dz,
			COALESCE(di.device_pk, '') as device_pk,
			COALESCE(di.device_code, '') as device_code,
			COALESCE(di.metro_pk, '') as metro_pk,
			COALESCE(di.metro_code, '') as metro_code,
			COALESCE(geo.city, '') as city,
			COALESCE(geo.country, '') as country,
			COALESCE(g.gossip_ip, '') as gossip_ip,
			COALESCE(g.gossip_port, 0) as gossip_port,
			COALESCE(tr.in_bps, 0) as in_bps,
			COALESCE(tr.out_bps, 0) as out_bps,
			COALESCE(sr.skip_rate, 0) as skip_rate,
			COALESCE(g.version, '') as version,
			COALESCE(va.software_client, '') as software_client,
			COALESCE(va.software_version, '') as software_version
		FROM solana_vote_accounts_current v
		CROSS JOIN total_stake ts
		LEFT JOIN solana_gossip_nodes_current g ON v.node_pubkey = g.pubkey
		LEFT JOIN geoip_records_current geo ON g.gossip_ip = geo.ip
		LEFT JOIN dz_ip_info di ON g.gossip_ip = di.client_ip
		LEFT JOIN traffic_rates tr ON di.user_pk = tr.user_pk
		LEFT JOIN skip_rates sr ON v.node_pubkey = sr.leader_identity_pubkey
		LEFT JOIN validatorsapp_data va ON v.vote_pubkey = va.vote_account
		WHERE v.vote_pubkey = ?
	`

	var validator ValidatorDetail
	err := envDB(ctx).QueryRow(ctx, query, votePubkey).Scan(
		&validator.VotePubkey,
		&validator.NodePubkey,
		&validator.StakeSol,
		&validator.StakeShare,
		&validator.Commission,
		&validator.OnDZ,
		&validator.DevicePK,
		&validator.DeviceCode,
		&validator.MetroPK,
		&validator.MetroCode,
		&validator.City,
		&validator.Country,
		&validator.GossipIP,
		&validator.GossipPort,
		&validator.InBps,
		&validator.OutBps,
		&validator.SkipRate,
		&validator.Version,
		&validator.SoftwareClient,
		&validator.SoftwareVersion,
	)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		slog.Error("validator query failed", "error", err, "vote_pubkey", votePubkey)
		http.Error(w, "validator not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(validator); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}
