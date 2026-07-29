package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

// validatorsQuerySortFields maps sort keys to the output column names used in the
// ORDER BY of the validators listing query (distinct from validatorSortFields,
// which references view columns for other query shapes).
var validatorsQuerySortFields = map[string]string{
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

// ValidatorsPageCacheKey is the page-cache key for the default validators listing
// (no filters, sorted by stake desc, first page). The worker refreshes it; the
// handler serves it for the matching request shape. Exported so the worker entry
// and this handler share one definition (like MulticastHealthSummariesCacheKey).
const ValidatorsPageCacheKey = "validators"

func (a *API) GetValidators(w http.ResponseWriter, r *http.Request) {
	// DefaultLimit (not a literal) so the parse default and the page-cache gate's
	// isDefaultValidatorsRequest comparison can't drift apart.
	pagination := ParsePagination(r, DefaultLimit)
	sort := ParseSort(r, "stake", validatorSortFields)
	filters := ParseFilters(r)

	// The default shape (first page, stake desc, no filters) is polled continuously
	// and served from the page cache on mainnet. Other shapes bypass the cache.
	if isMainnet(r.Context()) && isDefaultValidatorsRequest(pagination, sort, filters) {
		if data, err := a.readPageCache(r.Context(), ValidatorsPageCacheKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}
	w.Header().Set("X-Cache", "MISS")

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	filterClause, filterArgs := filters.BuildFilterClause(validatorFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}
	orderBy := sort.OrderByClause(validatorsQuerySortFields)

	resp, err := a.fetchValidatorsPage(ctx, whereFilter, filterArgs, orderBy, pagination.Limit, pagination.Offset)
	if err != nil {
		logError("validators query failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// isDefaultValidatorsRequest reports whether the parsed request matches the shape
// the page cache holds: first page (default limit, offset 0), stake desc, no
// filters. Comparing parsed values (not the raw URL) so both a bare request and
// an explicit "?limit=100&offset=0&sort_by=stake&sort_dir=desc" match.
func isDefaultValidatorsRequest(p PaginationParams, s SortParams, f MultiFilterParams) bool {
	return p.Limit == DefaultLimit && p.Offset == 0 &&
		s.Field == "stake" && s.Direction == "desc" &&
		f.IsEmpty()
}

// FetchValidatorsData computes the default validators listing (the page-cached
// shape) for the page-cache worker. It runs against the mainnet connection, like
// the other worker fetch functions.
func (a *API) FetchValidatorsData(ctx context.Context) (*ValidatorListResponse, error) {
	orderBy := SortParams{Field: "stake", Direction: "desc"}.OrderByClause(validatorsQuerySortFields)
	return a.fetchValidatorsPage(ctx, "", nil, orderBy, DefaultLimit, 0)
}

// fetchValidatorsPage runs the validators listing query for the given filter,
// sort, and pagination, returning the assembled response. Shared by the live
// handler and the page-cache worker.
func (a *API) fetchValidatorsPage(ctx context.Context, whereFilter string, filterArgs []any, orderBy string, limit, offset int) (*ValidatorListResponse, error) {
	start := time.Now()

	// Single query using window functions for counts to avoid repeating expensive CTEs.
	// NOTE: We avoid JOINing _current views (which use window functions) with each other
	// directly, as ClickHouse incorrectly correlates the window functions across views
	// in the same JOIN chain. Instead, we use IN for the on_dz boolean check and join
	// the DZ metadata (dz_ip_info) separately via gossip_ip after the gossip join.
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

	queryArgs := append(append([]any{}, filterArgs...), limit, offset)
	rows, err := a.envDB(ctx).Query(ctx, query, queryArgs...)
	metrics.RecordClickHouseQuery("validators", time.Since(start), err)
	if err != nil {
		return nil, err
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
			return nil, fmt.Errorf("validators row scan: %w", err)
		}
		validators = append(validators, v)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("validators rows iteration: %w", err)
	}

	// Return empty array instead of null
	if validators == nil {
		validators = []ValidatorListItem{}
	}

	return &ValidatorListResponse{
		Items:     validators,
		Total:     int(total),
		OnDZCount: int(onDZCount),
		Limit:     limit,
		Offset:    offset,
	}, nil
}

type ValidatorMetadataRow struct {
	IP              string
	ActiveStake     int64
	VoteAccount     string
	SoftwareClient  string
	SoftwareVersion string
}

// FetchValidatorsMetadata returns active validator metadata ordered by
// active_stake DESC.
func (a *API) FetchValidatorsMetadata(ctx context.Context) ([]ValidatorMetadataRow, error) {
	start := time.Now()
	query := `
		SELECT
			COALESCE(v.ip, '') as ip,
			v.active_stake,
			v.vote_account,
			COALESCE(v.software_client, '') as software_client,
			COALESCE(v.software_version, '') as software_version
		FROM validatorsapp_validators_current v
		WHERE v.is_active = 1
		ORDER BY v.active_stake DESC
	`

	rows, err := a.envDB(ctx).Query(ctx, query)
	metrics.RecordClickHouseQuery("validators", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []ValidatorMetadataRow
	for rows.Next() {
		var item ValidatorMetadataRow
		if err := rows.Scan(&item.IP, &item.ActiveStake, &item.VoteAccount, &item.SoftwareClient, &item.SoftwareVersion); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
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

func (a *API) GetValidator(w http.ResponseWriter, r *http.Request) {
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
				AND u.client_ip = (
					SELECT gossip_ip FROM solana_gossip_nodes_current
					WHERE pubkey = (SELECT node_pubkey FROM solana_vote_accounts_current WHERE vote_pubkey = ?)
				)
			GROUP BY u.client_ip
		),
		traffic_rates AS (
			SELECT
				user_pk,
				SUM(avg_in_bps) as in_bps,
				SUM(avg_out_bps) as out_bps
			FROM device_interface_rollup_5m
			WHERE bucket_ts = (SELECT max(bucket_ts) FROM device_interface_rollup_5m)
				AND user_pk IN (SELECT user_pk FROM dz_ip_info)
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
				AND leader_identity_pubkey = (SELECT node_pubkey FROM solana_vote_accounts_current WHERE vote_pubkey = ?)
			GROUP BY leader_identity_pubkey
			HAVING MAX(leader_slots_assigned_cum) > 0
		),
		validatorsapp_data AS (
			SELECT
				vote_account,
				software_client,
				software_version
			FROM validatorsapp_validators_current
			WHERE vote_account = ?
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
	err := a.envDB(ctx).QueryRow(ctx, query, votePubkey, votePubkey, votePubkey, votePubkey).Scan(
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
	metrics.RecordClickHouseQuery("validators", duration, err)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "validator not found", http.StatusNotFound)
			return
		}
		logError("validator query failed", "error", err, "vote_pubkey", votePubkey)
		http.Error(w, "validator not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(validator); err != nil {
		logError("failed to encode response", "error", err)
	}
}
