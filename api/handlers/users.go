package handlers

import (
	"context"
	"encoding/json"
	"log"
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
	InBps       float64 `json:"in_bps"`
	OutBps      float64 `json:"out_bps"`
}

func GetUsers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	start := time.Now()

	// Get total count
	countQuery := `SELECT count(*) FROM dz_users_current`
	var total uint64
	if err := envDB(ctx).QueryRow(ctx, countQuery).Scan(&total); err != nil {
		log.Printf("Users count error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	query := `
		WITH traffic_rates AS (
			SELECT
				user_tunnel_id,
				device_pk,
				CASE WHEN SUM(delta_duration) > 0
					THEN SUM(out_octets_delta) * 8 / SUM(delta_duration)
					ELSE 0
				END as in_bps,
				CASE WHEN SUM(delta_duration) > 0
					THEN SUM(in_octets_delta) * 8 / SUM(delta_duration)
					ELSE 0
				END as out_bps
			FROM fact_dz_device_interface_counters
			WHERE event_ts > now() - INTERVAL 5 MINUTE
				AND user_tunnel_id IS NOT NULL
				AND delta_duration > 0
				AND (in_octets_delta >= 0 OR out_octets_delta >= 0)
			GROUP BY user_tunnel_id, device_pk
		)
		SELECT
			u.pk,
			COALESCE(u.owner_pubkey, '') as owner_pubkey,
			u.status,
			COALESCE(u.kind, '') as kind,
			COALESCE(u.dz_ip, '') as dz_ip,
			COALESCE(u.client_ip, '') as client_ip,
			COALESCE(u.device_pk, '') as device_pk,
			COALESCE(d.code, '') as device_code,
			COALESCE(m.code, '') as metro_code,
			COALESCE(m.name, '') as metro_name,
			COALESCE(tr.in_bps, 0) as in_bps,
			COALESCE(tr.out_bps, 0) as out_bps
		FROM dz_users_current u
		LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN traffic_rates tr ON u.tunnel_id = tr.user_tunnel_id AND u.device_pk = tr.device_pk
		ORDER BY u.owner_pubkey
		LIMIT ? OFFSET ?
	`

	rows, err := envDB(ctx).Query(ctx, query, pagination.Limit, pagination.Offset)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("Users query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var users []UserListItem
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
			&u.InBps,
			&u.OutBps,
		); err != nil {
			log.Printf("Users scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		users = append(users, u)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Users rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null
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
		log.Printf("JSON encoding error: %v", err)
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
	InBps           float64 `json:"in_bps"`
	OutBps          float64 `json:"out_bps"`
	IsValidator     bool    `json:"is_validator"`
	NodePubkey      string  `json:"node_pubkey"`
	VotePubkey      string  `json:"vote_pubkey"`
	StakeSol        float64 `json:"stake_sol"`
	StakeWeightPct  float64 `json:"stake_weight_pct"`
}

func GetUser(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing user pk", http.StatusBadRequest)
		return
	}

	start := time.Now()
	query := `
		WITH traffic_rates AS (
			SELECT
				user_tunnel_id,
				device_pk,
				CASE WHEN SUM(delta_duration) > 0
					THEN SUM(out_octets_delta) * 8 / SUM(delta_duration)
					ELSE 0
				END as in_bps,
				CASE WHEN SUM(delta_duration) > 0
					THEN SUM(in_octets_delta) * 8 / SUM(delta_duration)
					ELSE 0
				END as out_bps
			FROM fact_dz_device_interface_counters
			WHERE event_ts > now() - INTERVAL 5 MINUTE
				AND user_tunnel_id IS NOT NULL
				AND delta_duration > 0
				AND (in_octets_delta >= 0 OR out_octets_delta >= 0)
			GROUP BY user_tunnel_id, device_pk
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
			COALESCE(tr.in_bps, 0) as in_bps,
			COALESCE(tr.out_bps, 0) as out_bps,
			si.vote_pubkey IS NOT NULL AND si.vote_pubkey != '' as is_validator,
			COALESCE(si.node_pubkey, '') as node_pubkey,
			COALESCE(si.vote_pubkey, '') as vote_pubkey,
			COALESCE(si.stake_sol, 0) as stake_sol,
			CASE WHEN ts.total_lamports > 0 THEN si.stake_lamports * 100.0 / ts.total_lamports ELSE 0 END as stake_weight_pct
		FROM dz_users_current u
		LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		LEFT JOIN traffic_rates tr ON u.tunnel_id = tr.user_tunnel_id AND u.device_pk = tr.device_pk
		LEFT JOIN solana_info si ON u.client_ip = si.gossip_ip
		CROSS JOIN total_stake ts
		WHERE u.pk = ?
	`

	var user UserDetail
	err := envDB(ctx).QueryRow(ctx, query, pk).Scan(
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
		&user.InBps,
		&user.OutBps,
		&user.IsValidator,
		&user.NodePubkey,
		&user.VotePubkey,
		&user.StakeSol,
		&user.StakeWeightPct,
	)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("User query error: %v", err)
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(user); err != nil {
		log.Printf("JSON encoding error: %v", err)
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

func GetUserTraffic(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing user pk", http.StatusBadRequest)
		return
	}

	timeRange := r.URL.Query().Get("time_range")
	if timeRange == "" {
		timeRange = "1h"
	}

	// Default bucket sizes per time range
	var interval, lookback string
	switch timeRange {
	case "1h":
		interval, lookback = "30", "1 HOUR"
	case "6h":
		interval, lookback = "120", "6 HOUR"
	case "12h":
		interval, lookback = "300", "12 HOUR"
	case "24h":
		interval, lookback = "600", "24 HOUR"
	default:
		interval, lookback = "30", "1 HOUR"
	}

	// Allow explicit bucket override (in seconds)
	if bucket := r.URL.Query().Get("bucket"); bucket != "" && bucket != "auto" {
		switch bucket {
		case "2", "10", "30", "60", "120", "300", "600":
			interval = bucket
		}
	}

	start := time.Now()
	query := `
		WITH user_info AS (
			SELECT tunnel_id, device_pk
			FROM dz_users_current
			WHERE pk = ?
		)
		SELECT
			formatDateTime(toStartOfInterval(event_ts, INTERVAL ` + interval + ` SECOND), '%Y-%m-%dT%H:%i:%s') as time,
			user_tunnel_id as tunnel_id,
			CASE WHEN SUM(delta_duration) > 0
				THEN SUM(out_octets_delta) * 8 / SUM(delta_duration)
				ELSE 0
			END as in_bps,
			CASE WHEN SUM(delta_duration) > 0
				THEN SUM(in_octets_delta) * 8 / SUM(delta_duration)
				ELSE 0
			END as out_bps,
			CASE WHEN SUM(delta_duration) > 0
				THEN SUM(out_pkts_delta) / SUM(delta_duration)
				ELSE 0
			END as in_pps,
			CASE WHEN SUM(delta_duration) > 0
				THEN SUM(in_pkts_delta) / SUM(delta_duration)
				ELSE 0
			END as out_pps
		FROM fact_dz_device_interface_counters
		WHERE event_ts > now() - INTERVAL ` + lookback + `
			AND user_tunnel_id IN (SELECT tunnel_id FROM user_info)
			AND device_pk IN (SELECT device_pk FROM user_info)
			AND delta_duration > 0
			AND (in_octets_delta >= 0 OR out_octets_delta >= 0)
		GROUP BY time, tunnel_id
		ORDER BY time, tunnel_id
	`

	rows, err := envDB(ctx).Query(ctx, query, pk)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("UserTraffic query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var points []UserTrafficPoint
	for rows.Next() {
		var p UserTrafficPoint
		if err := rows.Scan(&p.Time, &p.TunnelID, &p.InBps, &p.OutBps, &p.InPps, &p.OutPps); err != nil {
			log.Printf("UserTraffic scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		points = append(points, p)
	}

	if err := rows.Err(); err != nil {
		log.Printf("UserTraffic rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if points == nil {
		points = []UserTrafficPoint{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(points); err != nil {
		log.Printf("JSON encoding error: %v", err)
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

func GetUserMulticastGroups(w http.ResponseWriter, r *http.Request) {
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

	rows, err := envDB(ctx).Query(ctx, query, pk, pk)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("UserMulticastGroups query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var groups []UserMulticastGroup
	for rows.Next() {
		var g UserMulticastGroup
		if err := rows.Scan(&g.GroupPK, &g.GroupCode, &g.MulticastIP, &g.Mode, &g.Status, &g.PublisherCount, &g.SubscriberCount); err != nil {
			log.Printf("UserMulticastGroups scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		groups = append(groups, g)
	}

	if err := rows.Err(); err != nil {
		log.Printf("UserMulticastGroups rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if groups == nil {
		groups = []UserMulticastGroup{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(groups); err != nil {
		log.Printf("JSON encoding error: %v", err)
	}
}
