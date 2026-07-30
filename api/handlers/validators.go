package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
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

// ValidatorsPageCacheKey is the page-cache key for the unfiltered, stake-desc
// validators listing. The worker refreshes the *complete* set; the handler slices
// the requested page out of it. Exported so the worker entry and this handler
// share one definition (like MulticastHealthSummariesCacheKey).
//
// Deliberately not the old "validators" key, which held only the first page: the
// worker runs inside the api pod, so during a rolling update a new pod would write
// the complete set under a key that old pods still serve verbatim — returning every
// row and ignoring the request's limit. A new key means both versions simply miss
// and serve live for the rollout window.
const ValidatorsPageCacheKey = "validators:all"

// validatorsCacheMaxRows caps the row count the page-cache entry holds. At ~390
// bytes of JSON per row the current ~1,300-row validator set is ~490 KB, so this
// is ~4× headroom at ~1.9 MB worst case. Exceeding it disables the cache rather
// than serving a truncated page — see sliceCachedValidators.
const validatorsCacheMaxRows = 5000

func (a *API) GetValidators(w http.ResponseWriter, r *http.Request) {
	pagination := ParsePagination(r, DefaultLimit)
	sort := ParseSort(r, "stake", validatorSortFields)
	filters := ParseFilters(r)

	// The unfiltered stake-desc listing is polled continuously (by the UI at
	// limit=100 and by an external consumer at limit=900). The cache holds the
	// complete set, so any page of it can be served without a ClickHouse query.
	// Filtered or differently-sorted shapes bypass the cache.
	if isMainnet(r.Context()) && isCacheableValidatorsRequest(sort, filters) {
		if data, err := a.readPageCache(r.Context(), ValidatorsPageCacheKey); err == nil {
			if page, ok := sliceCachedValidators(data, pagination); ok {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Cache", "HIT")
				// Let the polling clients self-throttle to the refresh cadence.
				w.Header().Set("Cache-Control", "public, max-age=60")
				// The response body depends on the env header, which is not in the URL
				// (EnvMiddleware reads X-DZ-Env first, ?env= only as a fallback). Without
				// this, a cache would replay this mainnet payload for a devnet request to
				// the same URL — starting with the browser's own cache when the UI's env
				// selector switches.
				w.Header().Add("Vary", "X-DZ-Env")
				if err := json.NewEncoder(w).Encode(page); err != nil {
					logError("failed to encode response", "error", err)
				}
				return
			}
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
		// Fixed message, not err.Error(): this endpoint is unauthenticated, and a raw
		// driver error leaks the ClickHouse host and user. The detail is in the log.
		http.Error(w, "validators query failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// isCacheableValidatorsRequest reports whether the parsed request can be served
// from the page-cache entry: unfiltered and sorted by stake desc. Pagination is
// deliberately not part of the gate — the entry holds the complete set, so any
// limit/offset is serviceable. Comparing parsed values (not the raw URL) so both a
// bare request and an explicit "?sort_by=stake&sort_dir=desc" match.
func isCacheableValidatorsRequest(s SortParams, f MultiFilterParams) bool {
	return s.Field == "stake" && s.Direction == "desc" && f.IsEmpty()
}

// sliceCachedValidators returns the requested page of a cached validators listing,
// or false when the cached payload is not the complete set and therefore can't
// answer an arbitrary page.
//
// Completeness is decided by Total (count() OVER () over the whole set, so it
// reports the true row count even when the SELECT was capped) against the number
// of items actually stored. That check covers the row cap being exceeded and any
// future truncation, both of which fall through to the live query.
//
// An empty set is also treated as unusable rather than as a trivially complete
// one: a single refresh that caught the view mid-reload would otherwise pin an
// empty listing as an authoritative answer for a whole refresh cycle. Same reason
// scalarCache.set refuses to cache a zero.
func sliceCachedValidators(data []byte, p PaginationParams) (*ValidatorListResponse, bool) {
	var cached ValidatorListResponse
	if err := json.Unmarshal(data, &cached); err != nil {
		return nil, false
	}
	if cached.Total == 0 || cached.Total > len(cached.Items) {
		return nil, false
	}

	// Both bounds are clamped to the set, so a page past the end is an empty (but
	// non-nil, hence still JSON "[]") slice rather than an out-of-range panic.
	start := min(p.Offset, len(cached.Items))
	end := min(start+p.Limit, len(cached.Items))

	return &ValidatorListResponse{
		Items: cached.Items[start:end],
		// Whole-set aggregates, independent of the page being served.
		Total:     cached.Total,
		OnDZCount: cached.OnDZCount,
		Limit:     p.Limit,
		Offset:    p.Offset,
	}, true
}

// FetchValidatorsData computes the complete unfiltered stake-desc validators
// listing for the page-cache worker, from which the handler slices whatever page a
// client asks for. It runs against the mainnet connection, like the other worker
// fetch functions.
func (a *API) FetchValidatorsData(ctx context.Context) (*ValidatorListResponse, error) {
	orderBy := SortParams{Field: "stake", Direction: "desc"}.OrderByClause(validatorsQuerySortFields)
	resp, err := a.fetchValidatorsPage(ctx, "", nil, orderBy, validatorsCacheMaxRows, 0)
	if err != nil {
		return nil, err
	}
	if resp.Total > len(resp.Items) {
		// The cache entry is now incomplete, so the handler will serve every request
		// live until the cap is raised. Degraded fallback, not a terminal failure —
		// WARN, never ERROR (this would otherwise page on-call for Solana growth).
		logWarn("validators page cache row cap exceeded; listing requests will bypass the cache",
			"cap", validatorsCacheMaxRows, "total", resp.Total)
		// Drop the rows before they're written: Total already fails the handler's
		// completeness check, so storing multiple MB every refresh only to unmarshal
		// and discard it per request is strictly worse than caching nothing.
		resp.Items = []ValidatorListItem{}
	}
	return resp, nil
}

// fetchValidatorsPage runs the validators listing query for the given filter,
// sort, and pagination, returning the assembled response. Shared by the live
// handler and the page-cache worker.
func (a *API) fetchValidatorsPage(ctx context.Context, whereFilter string, filterArgs []any, orderBy string, limit, offset int) (*ValidatorListResponse, error) {
	// The stake_share denominator comes from the TTL-cached scalar instead of a
	// per-request CTE that re-scanned the vote-accounts window-function view.
	totalStake, err := a.cachedEpochVoteTotalStake(ctx)
	if err != nil {
		return nil, fmt.Errorf("validators total stake: %w", err)
	}
	totalStakeLit := strconv.FormatInt(totalStake, 10)

	start := time.Now()

	// Single query using window functions for counts to avoid repeating expensive CTEs.
	// NOTE: We avoid JOINing _current views (which use window functions) with each other
	// directly, as ClickHouse incorrectly correlates the window functions across views
	// in the same JOIN chain. Instead, we use IN for the on_dz boolean check and join
	// the DZ metadata (dz_ip_info) separately via gossip_ip after the gossip join.
	query := `
		WITH dz_ip_info AS (
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
				CASE WHEN ` + totalStakeLit + ` > 0
					THEN v.activated_stake_lamports * 100.0 / ` + totalStakeLit + `
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

	// TTL-cached denominator instead of a per-request total_stake CTE (see
	// cachedEpochVoteTotalStake).
	totalStake, err := a.cachedEpochVoteTotalStake(ctx)
	if err != nil {
		logError("validator total stake query failed", "error", err, "vote_pubkey", votePubkey)
		http.Error(w, "validator lookup failed", http.StatusInternalServerError)
		return
	}
	totalStakeLit := strconv.FormatInt(totalStake, 10)

	start := time.Now()
	query := `
		WITH dz_ip_info AS (
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
			CASE WHEN ` + totalStakeLit + ` > 0
				THEN v.activated_stake_lamports * 100.0 / ` + totalStakeLit + `
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
		LEFT JOIN solana_gossip_nodes_current g ON v.node_pubkey = g.pubkey
		LEFT JOIN geoip_records_current geo ON g.gossip_ip = geo.ip
		LEFT JOIN dz_ip_info di ON g.gossip_ip = di.client_ip
		LEFT JOIN traffic_rates tr ON di.user_pk = tr.user_pk
		LEFT JOIN skip_rates sr ON v.node_pubkey = sr.leader_identity_pubkey
		LEFT JOIN validatorsapp_data va ON v.vote_pubkey = va.vote_account
		WHERE v.vote_pubkey = ?
	`

	var validator ValidatorDetail
	err = a.envDB(ctx).QueryRow(ctx, query, votePubkey, votePubkey, votePubkey, votePubkey).Scan(
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
