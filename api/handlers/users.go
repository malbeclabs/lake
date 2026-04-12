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

type UserListItem struct {
	PK          string  `json:"pk"`
	OwnerPubkey string  `json:"owner_pubkey"`
	Status      string  `json:"status"`
	Kind        string  `json:"kind"`
	DzIP        string  `json:"dz_ip"`
	ClientIP    string  `json:"client_ip"`
	DevicePK    string  `json:"device_pk"`
	DeviceCode  string  `json:"device_code"`
	MetroCode   string  `json:"metro_code"`
	MetroName   string  `json:"metro_name"`
	TenantPK    string  `json:"tenant_pk"`
	TenantCode  string  `json:"tenant_code"`
	InBps       float64 `json:"in_bps"`
	OutBps      float64 `json:"out_bps"`
	IsDeleted   bool    `json:"is_deleted"`
}

var userSortFields = map[string]string{
	"owner":    "owner_pubkey",
	"kind":     "kind",
	"dzip":     "dz_ip",
	"clientip": "client_ip",
	"device":   "device_code",
	"metro":    "metro_name",
	"tenant":   "tenant_code",
	"status":   "status",
	"in":       "in_bps",
	"out":      "out_bps",
}

var userFilterFields = map[string]FilterFieldConfig{
	"owner":    {Column: "owner_pubkey", Type: FieldTypeText},
	"kind":     {Column: "kind", Type: FieldTypeText},
	"dzip":     {Column: "dz_ip", Type: FieldTypeText},
	"clientip": {Column: "client_ip", Type: FieldTypeText},
	"device":   {Column: "device_code", Type: FieldTypeText},
	"metro":    {Column: "metro_name", Type: FieldTypeText},
	"tenant":   {Column: "tenant_code", Type: FieldTypeText},
	"status":   {Column: "status", Type: FieldTypeText},
	"in":       {Column: "in_bps", Type: FieldTypeBandwidth},
	"out":      {Column: "out_bps", Type: FieldTypeBandwidth},
}

func (a *API) GetUsers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "owner", userSortFields)
	filters := ParseFilters(r)
	includeDeleted := r.URL.Query().Get("include_deleted") == "true"
	start := time.Now()

	filterClause, filterArgs := filters.BuildFilterClause(userFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}

	orderBy := sort.OrderByClause(userSortFields)
	if orderBy == "" {
		orderBy = "ORDER BY owner_pubkey ASC"
	}

	var sourceCTE string
	var isDeletedExpr string
	var fromTable string
	if includeDeleted {
		sourceCTE = `all_users AS (
			SELECT
				argMax(pk, (snapshot_ts, ingested_at, op_id)) as pk,
				argMax(owner_pubkey, (snapshot_ts, ingested_at, op_id)) as owner_pubkey,
				argMax(status, (snapshot_ts, ingested_at, op_id)) as status,
				argMax(kind, (snapshot_ts, ingested_at, op_id)) as kind,
				argMax(dz_ip, (snapshot_ts, ingested_at, op_id)) as dz_ip,
				argMax(client_ip, (snapshot_ts, ingested_at, op_id)) as client_ip,
				argMax(device_pk, (snapshot_ts, ingested_at, op_id)) as device_pk,
				argMax(tenant_pk, (snapshot_ts, ingested_at, op_id)) as tenant_pk,
				argMax(is_deleted, (snapshot_ts, ingested_at, op_id)) as is_deleted
			FROM dim_dz_users_history
			GROUP BY entity_id
			HAVING pk != ''
		),`
		isDeletedExpr = "u.is_deleted = 1 as is_deleted"
		fromTable = "all_users"
	} else {
		sourceCTE = ""
		isDeletedExpr = "false as is_deleted"
		fromTable = "dz_users_current"
	}

	query := `
		WITH ` + sourceCTE + `
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
		users_data AS (
			SELECT
				u.pk as pk,
				COALESCE(u.owner_pubkey, '') as owner_pubkey,
				u.status as status,
				COALESCE(u.kind, '') as kind,
				COALESCE(u.dz_ip, '') as dz_ip,
				COALESCE(u.client_ip, '') as client_ip,
				COALESCE(u.device_pk, '') as device_pk,
				COALESCE(d.code, '') as device_code,
				COALESCE(m.code, '') as metro_code,
				COALESCE(m.name, '') as metro_name,
				COALESCE(u.tenant_pk, '') as tenant_pk,
				COALESCE(t.code, '') as tenant_code,
				COALESCE(tr.in_bps, 0) as in_bps,
				COALESCE(tr.out_bps, 0) as out_bps,
				` + isDeletedExpr + `
			FROM ` + fromTable + ` u
			LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			LEFT JOIN dz_tenants_current t ON u.tenant_pk = t.pk
			LEFT JOIN traffic_rates tr ON u.pk = tr.user_pk
		)
		SELECT
			pk, owner_pubkey, status, kind, dz_ip, client_ip, device_pk,
			device_code, metro_code, metro_name, tenant_pk, tenant_code,
			in_bps, out_bps, is_deleted,
			count() OVER () as _total
		FROM users_data
		WHERE 1=1` + whereFilter + `
		` + orderBy + `
		LIMIT ? OFFSET ?`

	queryArgs := append(filterArgs, pagination.Limit, pagination.Offset)
	rows, err := a.envDB(ctx).Query(ctx, query, queryArgs...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("users query failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var users []UserListItem
	var total uint64
	for rows.Next() {
		var u UserListItem
		if err := rows.Scan(
			&u.PK,
			&u.OwnerPubkey,
			&u.Status,
			&u.Kind,
			&u.DzIP,
			&u.ClientIP,
			&u.DevicePK,
			&u.DeviceCode,
			&u.MetroCode,
			&u.MetroName,
			&u.TenantPK,
			&u.TenantCode,
			&u.InBps,
			&u.OutBps,
			&u.IsDeleted,
			&total,
		); err != nil {
			logError("users row scan failed", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		users = append(users, u)
	}

	if err := rows.Err(); err != nil {
		logError("users rows iteration failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if users == nil {
		users = []UserListItem{}
	}

	response := PaginatedResponse[UserListItem]{
		Items:  users,
		Total:  int(total),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}

type UserDetail struct {
	PK              string  `json:"pk"`
	OwnerPubkey     string  `json:"owner_pubkey"`
	Status          string  `json:"status"`
	Kind            string  `json:"kind"`
	DzIP            string  `json:"dz_ip"`
	ClientIP        string  `json:"client_ip"`
	TunnelID        int32   `json:"tunnel_id"`
	DevicePK        string  `json:"device_pk"`
	DeviceCode      string  `json:"device_code"`
	MetroPK         string  `json:"metro_pk"`
	MetroCode       string  `json:"metro_code"`
	MetroName       string  `json:"metro_name"`
	ContributorPK   string  `json:"contributor_pk"`
	ContributorCode string  `json:"contributor_code"`
	TenantPK        string  `json:"tenant_pk"`
	TenantCode      string  `json:"tenant_code"`
	InBps           float64 `json:"in_bps"`
	OutBps          float64 `json:"out_bps"`
	IsValidator     bool    `json:"is_validator"`
	NodePubkey      string  `json:"node_pubkey"`
	VotePubkey      string  `json:"vote_pubkey"`
	StakeSol        float64 `json:"stake_sol"`
	StakeWeightPct  float64 `json:"stake_weight_pct"`
	IsDeleted       bool    `json:"is_deleted"`
}

func (a *API) GetUser(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing user pk", http.StatusBadRequest)
		return
	}

	start := time.Now()
	query := `
		WITH latest_user AS (
			SELECT
				argMax(pk, (snapshot_ts, ingested_at, op_id)) as pk,
				argMax(owner_pubkey, (snapshot_ts, ingested_at, op_id)) as owner_pubkey,
				argMax(status, (snapshot_ts, ingested_at, op_id)) as status,
				argMax(kind, (snapshot_ts, ingested_at, op_id)) as kind,
				argMax(dz_ip, (snapshot_ts, ingested_at, op_id)) as dz_ip,
				argMax(client_ip, (snapshot_ts, ingested_at, op_id)) as client_ip,
				argMax(tunnel_id, (snapshot_ts, ingested_at, op_id)) as tunnel_id,
				argMax(device_pk, (snapshot_ts, ingested_at, op_id)) as device_pk,
				argMax(tenant_pk, (snapshot_ts, ingested_at, op_id)) as tenant_pk,
				argMax(is_deleted, (snapshot_ts, ingested_at, op_id)) as is_deleted
			FROM (SELECT * FROM dim_dz_users_history WHERE pk = ?)
			GROUP BY entity_id
			HAVING pk != ''
			LIMIT 1
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
		solana_info AS (
			SELECT
				g.gossip_ip,
				g.pubkey as node_pubkey,
				v.vote_pubkey,
				COALESCE(v.activated_stake_lamports / 1e9, 0) as stake_sol,
				COALESCE(v.activated_stake_lamports, 0) as stake_lamports
			FROM solana_gossip_nodes_current g
			LEFT JOIN solana_vote_accounts_current v ON g.pubkey = v.node_pubkey AND v.epoch_vote_account = 'true'
		),
		total_stake AS (
			SELECT COALESCE(SUM(activated_stake_lamports), 0) as total_lamports
			FROM solana_vote_accounts_current
			WHERE epoch_vote_account = 'true'
		)
		SELECT
			u.pk,
			COALESCE(u.owner_pubkey, '') as owner_pubkey,
			u.status,
			COALESCE(u.kind, '') as kind,
			COALESCE(u.dz_ip, '') as dz_ip,
			COALESCE(u.client_ip, '') as client_ip,
			COALESCE(u.tunnel_id, 0) as tunnel_id,
			COALESCE(u.device_pk, '') as device_pk,
			COALESCE(d.code, '') as device_code,
			COALESCE(d.metro_pk, '') as metro_pk,
			COALESCE(m.code, '') as metro_code,
			COALESCE(m.name, '') as metro_name,
			COALESCE(d.contributor_pk, '') as contributor_pk,
			COALESCE(c.code, '') as contributor_code,
			COALESCE(u.tenant_pk, '') as tenant_pk,
			COALESCE(t.code, '') as tenant_code,
			COALESCE(tr.in_bps, 0) as in_bps,
			COALESCE(tr.out_bps, 0) as out_bps,
			si.vote_pubkey IS NOT NULL AND si.vote_pubkey != '' as is_validator,
			COALESCE(si.node_pubkey, '') as node_pubkey,
			COALESCE(si.vote_pubkey, '') as vote_pubkey,
			COALESCE(si.stake_sol, 0) as stake_sol,
			CASE WHEN ts.total_lamports > 0 THEN si.stake_lamports * 100.0 / ts.total_lamports ELSE 0 END as stake_weight_pct,
			u.is_deleted = 1 as is_deleted
		FROM latest_user u
		LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		LEFT JOIN dz_tenants_current t ON u.tenant_pk = t.pk
		LEFT JOIN traffic_rates tr ON u.pk = tr.user_pk
		LEFT JOIN solana_info si ON u.client_ip = si.gossip_ip
		CROSS JOIN total_stake ts
	`

	var user UserDetail
	err := a.envDB(ctx).QueryRow(ctx, query, pk).Scan(
		&user.PK,
		&user.OwnerPubkey,
		&user.Status,
		&user.Kind,
		&user.DzIP,
		&user.ClientIP,
		&user.TunnelID,
		&user.DevicePK,
		&user.DeviceCode,
		&user.MetroPK,
		&user.MetroCode,
		&user.MetroName,
		&user.ContributorPK,
		&user.ContributorCode,
		&user.TenantPK,
		&user.TenantCode,
		&user.InBps,
		&user.OutBps,
		&user.IsValidator,
		&user.NodePubkey,
		&user.VotePubkey,
		&user.StakeSol,
		&user.StakeWeightPct,
		&user.IsDeleted,
	)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "user not found", http.StatusNotFound)
			return
		}
		logError("user query failed", "error", err, "pk", pk)
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(user); err != nil {
		logError("failed to encode response", "error", err)
	}
}

type UserTrafficPoint struct {
	Time     string  `json:"time"`
	TunnelID int64   `json:"tunnel_id"`
	InBps    float64 `json:"in_bps"`
	OutBps   float64 `json:"out_bps"`
	InPps    float64 `json:"in_pps"`
	OutPps   float64 `json:"out_pps"`
}

func (a *API) GetUserTraffic(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing user pk", http.StatusBadRequest)
		return
	}

	// Use the shared time filter: raw fact table for sub-5m, rollup for >= 5m
	timeFilter, bucketInterval, useRaw := trafficTimeFilter(r)

	// Parse aggregation mode
	agg := r.URL.Query().Get("agg")
	if agg == "" {
		agg = "max"
	}

	start := time.Now()
	var query string

	if useRaw {
		// Raw fact table path: compute rates from deltas
		// Note: device in_octets = user outbound (tx), device out_octets = user inbound (rx)
		var aggFunc string
		switch agg {
		case "avg":
			aggFunc = "AVG"
		case "min":
			aggFunc = "MIN"
		case "p50":
			aggFunc = "quantile(0.5)"
		case "p90":
			aggFunc = "quantile(0.9)"
		case "p95":
			aggFunc = "quantile(0.95)"
		case "p99":
			aggFunc = "quantile(0.99)"
		default:
			aggFunc = "MAX"
		}

		query = fmt.Sprintf(`
			WITH user_info AS (
				SELECT tunnel_id, device_pk
				FROM dz_users_current
				WHERE pk = ?
			)
			SELECT
				formatDateTime(toStartOfInterval(event_ts, INTERVAL %s), '%%Y-%%m-%%dT%%H:%%i:%%sZ') as time,
				user_tunnel_id as tunnel_id,
				%s(CASE WHEN delta_duration > 0 THEN out_octets_delta * 8 / delta_duration ELSE 0 END) as in_bps,
				%s(CASE WHEN delta_duration > 0 THEN in_octets_delta * 8 / delta_duration ELSE 0 END) as out_bps,
				%s(CASE WHEN delta_duration > 0 THEN out_pkts_delta / delta_duration ELSE 0 END) as in_pps,
				%s(CASE WHEN delta_duration > 0 THEN in_pkts_delta / delta_duration ELSE 0 END) as out_pps
			FROM fact_dz_device_interface_counters
			WHERE %s
				AND user_tunnel_id IN (SELECT tunnel_id FROM user_info)
				AND device_pk IN (SELECT device_pk FROM user_info)
				AND delta_duration > 0
				AND (in_octets_delta >= 0 OR out_octets_delta >= 0)
			GROUP BY time, tunnel_id
			ORDER BY time, tunnel_id
		`, bucketInterval, aggFunc, aggFunc, aggFunc, aggFunc, timeFilter)
	} else {
		// Rollup path: use pre-computed columns
		// Note: device in = user outbound (tx), device out = user inbound (rx)
		aggPrefix := "max"
		switch agg {
		case "avg":
			aggPrefix = "avg"
		case "min":
			aggPrefix = "min"
		case "p50":
			aggPrefix = "p50"
		case "p90":
			aggPrefix = "p90"
		case "p95":
			aggPrefix = "p95"
		case "p99":
			aggPrefix = "p99"
		}

		rollupAggFunc := "MAX"
		switch agg {
		case "avg":
			rollupAggFunc = "AVG"
		case "min":
			rollupAggFunc = "MIN"
		}

		query = fmt.Sprintf(`
			WITH user_info AS (
				SELECT tunnel_id, device_pk
				FROM dz_users_current
				WHERE pk = ?
			)
			SELECT
				formatDateTime(toStartOfInterval(bucket_ts, INTERVAL %s), '%%Y-%%m-%%dT%%H:%%i:%%sZ') as time,
				user_tunnel_id as tunnel_id,
				%s(%s_out_bps) as in_bps,
				%s(%s_in_bps) as out_bps,
				%s(%s_out_pps) as in_pps,
				%s(%s_in_pps) as out_pps
			FROM device_interface_rollup_5m
			WHERE %s
				AND user_tunnel_id IN (SELECT tunnel_id FROM user_info)
				AND device_pk IN (SELECT device_pk FROM user_info)
			GROUP BY time, tunnel_id
			ORDER BY time, tunnel_id
		`, bucketInterval,
			rollupAggFunc, aggPrefix,
			rollupAggFunc, aggPrefix,
			rollupAggFunc, aggPrefix,
			rollupAggFunc, aggPrefix,
			timeFilter)
	}

	rows, err := a.envDB(ctx).Query(ctx, query, pk)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("user traffic query failed", "error", err, "pk", pk)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var points []UserTrafficPoint
	for rows.Next() {
		var p UserTrafficPoint
		if err := rows.Scan(&p.Time, &p.TunnelID, &p.InBps, &p.OutBps, &p.InPps, &p.OutPps); err != nil {
			logError("user traffic row scan failed", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		points = append(points, p)
	}

	if err := rows.Err(); err != nil {
		logError("user traffic rows iteration failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if points == nil {
		points = []UserTrafficPoint{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(points); err != nil {
		logError("failed to encode response", "error", err)
	}
}

type UserMulticastGroup struct {
	GroupPK         string `json:"group_pk"`
	GroupCode       string `json:"group_code"`
	MulticastIP     string `json:"multicast_ip"`
	Mode            string `json:"mode"`
	Status          string `json:"status"`
	PublisherCount  uint64 `json:"publisher_count"`
	SubscriberCount uint64 `json:"subscriber_count"`
}

func (a *API) GetUserMulticastGroups(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing user pk", http.StatusBadRequest)
		return
	}

	start := time.Now()
	query := `
		WITH user_groups AS (
			SELECT
				arrayJoin(JSONExtract(u.publishers, 'Array(String)')) as group_pk,
				'P' as mode
			FROM dz_users_current u
			WHERE u.pk = ? AND JSONLength(u.publishers) > 0
			UNION ALL
			SELECT
				arrayJoin(JSONExtract(u.subscribers, 'Array(String)')) as group_pk,
				'S' as mode
			FROM dz_users_current u
			WHERE u.pk = ? AND JSONLength(u.subscribers) > 0
		),
		user_modes AS (
			SELECT
				group_pk,
				CASE
					WHEN countIf(mode = 'P') > 0 AND countIf(mode = 'S') > 0 THEN 'P+S'
					WHEN countIf(mode = 'P') > 0 THEN 'P'
					ELSE 'S'
				END as mode
			FROM user_groups
			GROUP BY group_pk
		),
		group_counts AS (
			SELECT
				group_pk,
				countIf(mode = 'P') as pub_count,
				countIf(mode = 'S') as sub_count
			FROM (
				SELECT arrayJoin(JSONExtract(u.publishers, 'Array(String)')) as group_pk, 'P' as mode
				FROM dz_users_current u
				WHERE u.status = 'activated' AND u.kind = 'multicast' AND JSONLength(u.publishers) > 0
				UNION ALL
				SELECT arrayJoin(JSONExtract(u.subscribers, 'Array(String)')) as group_pk, 'S' as mode
				FROM dz_users_current u
				WHERE u.status = 'activated' AND u.kind = 'multicast' AND JSONLength(u.subscribers) > 0
			)
			GROUP BY group_pk
		)
		SELECT
			g.pk,
			g.code,
			COALESCE(g.multicast_ip, '') as multicast_ip,
			um.mode,
			g.status,
			COALESCE(gc.pub_count, 0) as publisher_count,
			COALESCE(gc.sub_count, 0) as subscriber_count
		FROM user_modes um
		JOIN dz_multicast_groups_current g ON um.group_pk = g.pk
		LEFT JOIN group_counts gc ON g.pk = gc.group_pk
		ORDER BY g.code
	`

	rows, err := a.envDB(ctx).Query(ctx, query, pk, pk)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("user multicast groups query failed", "error", err, "pk", pk)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var groups []UserMulticastGroup
	for rows.Next() {
		var g UserMulticastGroup
		if err := rows.Scan(&g.GroupPK, &g.GroupCode, &g.MulticastIP, &g.Mode, &g.Status, &g.PublisherCount, &g.SubscriberCount); err != nil {
			logError("user multicast groups row scan failed", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		groups = append(groups, g)
	}

	if err := rows.Err(); err != nil {
		logError("user multicast groups rows iteration failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if groups == nil {
		groups = []UserMulticastGroup{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(groups); err != nil {
		logError("failed to encode response", "error", err)
	}
}
