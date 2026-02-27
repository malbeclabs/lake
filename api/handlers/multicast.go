package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/metrics"
)

type MulticastGroupListItem struct {
	PK              string `json:"pk"`
	Code            string `json:"code"`
	MulticastIP     string `json:"multicast_ip"`
	MaxBandwidth    uint64 `json:"max_bandwidth"`
	Status          string `json:"status"`
	PublisherCount  uint32 `json:"publisher_count"`
	SubscriberCount uint32 `json:"subscriber_count"`
}

func GetMulticastGroups(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	start := time.Now()

	query := `
		SELECT
			pk,
			COALESCE(code, '') as code,
			COALESCE(multicast_ip, '') as multicast_ip,
			COALESCE(max_bandwidth, 0) as max_bandwidth,
			COALESCE(status, '') as status,
			COALESCE(publisher_count, 0) as publisher_count,
			COALESCE(subscriber_count, 0) as subscriber_count
		FROM dz_multicast_groups_current
		WHERE status = 'activated'
		ORDER BY code
	`

	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("MulticastGroups query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var groups []MulticastGroupListItem
	for rows.Next() {
		var g MulticastGroupListItem
		if err := rows.Scan(
			&g.PK,
			&g.Code,
			&g.MulticastIP,
			&g.MaxBandwidth,
			&g.Status,
			&g.PublisherCount,
			&g.SubscriberCount,
		); err != nil {
			log.Printf("MulticastGroups scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		groups = append(groups, g)
	}

	if err := rows.Err(); err != nil {
		log.Printf("MulticastGroups rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null
	if groups == nil {
		groups = []MulticastGroupListItem{}
	}

	// Compute real pub/sub counts from dz_users_current since the table columns are often 0
	if len(groups) > 0 {
		groupPKs := make([]string, len(groups))
		groupByPK := make(map[string]int, len(groups))
		for i, g := range groups {
			groupPKs[i] = g.PK
			groupByPK[g.PK] = i
		}

		countsQuery := `
			SELECT
				group_pk,
				countIf(mode = 'P' OR mode = 'P+S') as pub_count,
				countIf(mode = 'S' OR mode = 'P+S') as sub_count
			FROM (
				SELECT
					arrayJoin(JSONExtract(u.publishers, 'Array(String)')) as group_pk,
					'P' as mode
				FROM dz_users_current u
				WHERE u.status = 'activated' AND u.kind = 'multicast'
					AND JSONLength(u.publishers) > 0
				UNION ALL
				SELECT
					arrayJoin(JSONExtract(u.subscribers, 'Array(String)')) as group_pk,
					'S' as mode
				FROM dz_users_current u
				WHERE u.status = 'activated' AND u.kind = 'multicast'
					AND JSONLength(u.subscribers) > 0
			)
			WHERE group_pk IN (?)
			GROUP BY group_pk
		`

		countRows, err := envDB(ctx).Query(ctx, countsQuery, groupPKs)
		if err != nil {
			log.Printf("MulticastGroups counts query error (non-fatal): %v", err)
		} else {
			defer countRows.Close()
			for countRows.Next() {
				var gpk string
				var pubCount, subCount uint64
				if err := countRows.Scan(&gpk, &pubCount, &subCount); err != nil {
					log.Printf("MulticastGroups counts scan error: %v", err)
					continue
				}
				if idx, ok := groupByPK[gpk]; ok {
					groups[idx].PublisherCount = uint32(pubCount)
					groups[idx].SubscriberCount = uint32(subCount)
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(groups); err != nil {
		log.Printf("JSON encoding error: %v", err)
	}
}

type MulticastMember struct {
	UserPK         string  `json:"user_pk"`
	Mode           string  `json:"mode"` // "P", "S", or "P+S"
	DevicePK       string  `json:"device_pk"`
	DeviceCode     string  `json:"device_code"`
	MetroPK        string  `json:"metro_pk"`
	MetroCode      string  `json:"metro_code"`
	MetroName      string  `json:"metro_name"`
	ClientIP       string  `json:"client_ip"`
	DZIP           string  `json:"dz_ip"`
	Status         string  `json:"status"`
	OwnerPubkey    string  `json:"owner_pubkey"`
	TunnelID       int32   `json:"tunnel_id"`
	TrafficBps     float64 `json:"traffic_bps"`      // traffic rate in bits per second
	TrafficPps     float64 `json:"traffic_pps"`      // traffic rate in packets per second
	IsLeader       bool    `json:"is_leader"`        // true if currently the Solana leader
	NodePubkey     string  `json:"node_pubkey"`      // validator's node identity pubkey
	VotePubkey     string  `json:"vote_pubkey"`      // validator's vote account pubkey
	StakeSol       float64 `json:"stake_sol"`        // activated stake in SOL
	LastLeaderSlot *int64  `json:"last_leader_slot"` // most recent past leader slot
	NextLeaderSlot *int64  `json:"next_leader_slot"` // next upcoming leader slot
	CurrentSlot    int64   `json:"current_slot"`     // current cluster slot
}

type MulticastGroupDetail struct {
	PK              string `json:"pk"`
	Code            string `json:"code"`
	MulticastIP     string `json:"multicast_ip"`
	MaxBandwidth    uint64 `json:"max_bandwidth"`
	Status          string `json:"status"`
	PublisherCount  uint32 `json:"publisher_count"`
	SubscriberCount uint32 `json:"subscriber_count"`
}

func GetMulticastGroup(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing multicast group pk", http.StatusBadRequest)
		return
	}

	start := time.Now()

	groupQuery := `
		SELECT
			pk,
			COALESCE(code, '') as code,
			COALESCE(multicast_ip, '') as multicast_ip,
			COALESCE(max_bandwidth, 0) as max_bandwidth,
			COALESCE(status, '') as status,
			COALESCE(publisher_count, 0) as publisher_count,
			COALESCE(subscriber_count, 0) as subscriber_count
		FROM dz_multicast_groups_current
		WHERE pk = ? OR code = ?
	`

	var group MulticastGroupDetail
	err := envDB(ctx).QueryRow(ctx, groupQuery, pkOrCode, pkOrCode).Scan(
		&group.PK,
		&group.Code,
		&group.MulticastIP,
		&group.MaxBandwidth,
		&group.Status,
		&group.PublisherCount,
		&group.SubscriberCount,
	)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("MulticastGroup query error: %v", err)
		http.Error(w, "multicast group not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(group); err != nil {
		log.Printf("JSON encoding error: %v", err)
	}
}

// MulticastMembersResponse is the paginated response for multicast group members
type MulticastMembersResponse struct {
	Items           []MulticastMember `json:"items"`
	Total           int               `json:"total"`
	PublisherCount  int               `json:"publisher_count"`
	SubscriberCount int               `json:"subscriber_count"`
	Limit           int               `json:"limit"`
	Offset          int               `json:"offset"`
}

var multicastMemberSortFields = map[string]string{
	"owner_pubkey":    "owner_pubkey",
	"node_pubkey":     "node_pubkey",
	"device_code":     "device_code",
	"metro_name":      "metro_name",
	"dz_ip":           "dz_ip",
	"tunnel_id":       "tunnel_id",
	"stake_sol":       "stake_sol",
	"leader_schedule": "next_leader_slot",
}

var multicastMemberFilterFields = map[string]FilterFieldConfig{
	"device": {Column: "device_code", Type: FieldTypeText},
	"metro":  {Column: "metro_name", Type: FieldTypeText},
	"owner":  {Column: "owner_pubkey", Type: FieldTypeText},
	"all":    {Column: "", Type: FieldTypeText}, // handled specially in BuildFilterClause
}

func GetMulticastGroupMembers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing multicast group pk", http.StatusBadRequest)
		return
	}

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "stake_sol", multicastMemberSortFields)
	filter := ParseFilter(r)
	tab := r.URL.Query().Get("tab")
	if tab != "publishers" && tab != "subscribers" {
		tab = "publishers"
	}

	start := time.Now()

	// Resolve group PK
	var groupPK string
	err := envDB(ctx).QueryRow(ctx,
		`SELECT pk FROM dz_multicast_groups_current WHERE pk = ? OR code = ?`, pkOrCode, pkOrCode).Scan(&groupPK)
	if err != nil {
		log.Printf("MulticastGroupMembers group query error: %v", err)
		http.Error(w, "multicast group not found", http.StatusNotFound)
		return
	}

	// Build base CTE with all joins integrated
	baseCTE := `
		WITH current_slot_info AS (
			SELECT max(cluster_slot) as slot
			FROM fact_solana_vote_account_activity
			WHERE event_ts >= now() - INTERVAL 2 MINUTE
		),
		epoch_info AS (
			SELECT
				toUInt64(cs.slot) as abs_slot,
				ls.epoch as epoch,
				toUInt64(ls.epoch) * 432000 as epoch_start,
				toUInt64(cs.slot) - (toUInt64(ls.epoch) * 432000) as slot_in_epoch
			FROM solana_leader_schedule_current ls
			CROSS JOIN current_slot_info cs
			LIMIT 1
		),
		members_base AS (
			SELECT
				u.pk as user_pk,
				CASE
					WHEN has(JSONExtract(u.publishers, 'Array(String)'), ?) AND has(JSONExtract(u.subscribers, 'Array(String)'), ?) THEN 'P+S'
					WHEN has(JSONExtract(u.publishers, 'Array(String)'), ?) THEN 'P'
					ELSE 'S'
				END as mode,
				has(JSONExtract(u.publishers, 'Array(String)'), ?) as is_publisher,
				has(JSONExtract(u.subscribers, 'Array(String)'), ?) as is_subscriber,
				COALESCE(u.device_pk, '') as device_pk,
				COALESCE(d.code, '') as device_code,
				COALESCE(d.metro_pk, '') as metro_pk,
				COALESCE(m.code, '') as metro_code,
				COALESCE(m.name, '') as metro_name,
				COALESCE(u.client_ip, '') as client_ip,
				COALESCE(u.dz_ip, '') as dz_ip,
				u.status as status,
				COALESCE(u.owner_pubkey, '') as owner_pubkey,
				COALESCE(u.tunnel_id, 0) as tunnel_id,
				COALESCE(g.pubkey, '') as node_pubkey,
				COALESCE(v.vote_pubkey, '') as vote_pubkey,
				COALESCE(v.activated_stake_lamports, 0) / 1e9 as stake_sol,
				COALESCE(ls.next_slot, 0) as next_leader_slot
			FROM dz_users_current u
			LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			LEFT JOIN solana_gossip_nodes_current g ON u.client_ip = g.gossip_ip AND u.client_ip != ''
			LEFT JOIN solana_vote_accounts_current v ON g.pubkey = v.node_pubkey AND v.epoch_vote_account = 'true'
			LEFT JOIN (
				SELECT
					node_pubkey,
					if(empty(arrayFilter(x -> x > ei.slot_in_epoch, JSONExtract(slots, 'Array(UInt64)'))), 0,
						ei.epoch_start + arrayMin(arrayFilter(x -> x > ei.slot_in_epoch, JSONExtract(slots, 'Array(UInt64)'))
					)) as next_slot
				FROM solana_leader_schedule_current
				CROSS JOIN epoch_info ei
			) ls ON g.pubkey = ls.node_pubkey
			WHERE u.status = 'activated'
				AND u.kind = 'multicast'
				AND (
					has(JSONExtract(u.publishers, 'Array(String)'), ?)
					OR has(JSONExtract(u.subscribers, 'Array(String)'), ?)
				)
		)
	`

	// Group PK appears 7 times in the base CTE
	baseArgs := []any{groupPK, groupPK, groupPK, groupPK, groupPK, groupPK, groupPK}

	// Tab filter: publishers tab shows P and P+S, subscribers tab shows S and P+S
	tabFilter := ""
	if tab == "publishers" {
		tabFilter = " AND is_publisher = 1"
	} else {
		tabFilter = " AND is_subscriber = 1"
	}

	// Build filter clause
	filterClause, filterArgs := filter.BuildFilterClause(multicastMemberFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}

	// Count queries: total for current tab+filter, and counts for both tabs
	countQuery := baseCTE + `SELECT count(*) FROM members_base WHERE 1=1` + tabFilter + whereFilter
	countArgs := append(append([]any{}, baseArgs...), filterArgs...)

	pubCountQuery := baseCTE + `SELECT count(*) FROM members_base WHERE is_publisher = 1` + whereFilter
	pubCountArgs := append(append([]any{}, baseArgs...), filterArgs...)

	subCountQuery := baseCTE + `SELECT count(*) FROM members_base WHERE is_subscriber = 1` + whereFilter
	subCountArgs := append(append([]any{}, baseArgs...), filterArgs...)

	var total, pubCount, subCount uint64
	if err := envDB(ctx).QueryRow(ctx, countQuery, countArgs...).Scan(&total); err != nil {
		log.Printf("MulticastGroupMembers count error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := envDB(ctx).QueryRow(ctx, pubCountQuery, pubCountArgs...).Scan(&pubCount); err != nil {
		log.Printf("MulticastGroupMembers pub count error (non-fatal): %v", err)
	}
	if err := envDB(ctx).QueryRow(ctx, subCountQuery, subCountArgs...).Scan(&subCount); err != nil {
		log.Printf("MulticastGroupMembers sub count error (non-fatal): %v", err)
	}

	// Main data query with sorting and pagination
	orderBy := sort.OrderByClause(multicastMemberSortFields)
	dataQuery := baseCTE + `
		SELECT user_pk, mode, device_pk, device_code, metro_pk, metro_code, metro_name,
			client_ip, dz_ip, status, owner_pubkey, tunnel_id, node_pubkey, vote_pubkey, stake_sol, next_leader_slot
		FROM members_base
		WHERE 1=1` + tabFilter + whereFilter + `
		` + orderBy + `
		LIMIT ? OFFSET ?
	`

	dataArgs := append(append([]any{}, baseArgs...), filterArgs...)
	dataArgs = append(dataArgs, pagination.Limit, pagination.Offset)

	rows, err := envDB(ctx).Query(ctx, dataQuery, dataArgs...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("MulticastGroupMembers data query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var members []MulticastMember
	for rows.Next() {
		var m MulticastMember
		var nextLeaderSlot uint64
		if err := rows.Scan(
			&m.UserPK, &m.Mode, &m.DevicePK, &m.DeviceCode,
			&m.MetroPK, &m.MetroCode, &m.MetroName,
			&m.ClientIP, &m.DZIP, &m.Status, &m.OwnerPubkey,
			&m.TunnelID, &m.NodePubkey, &m.VotePubkey, &m.StakeSol,
			&nextLeaderSlot,
		); err != nil {
			log.Printf("MulticastGroupMembers scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if nextLeaderSlot > 0 {
			s := int64(nextLeaderSlot)
			m.NextLeaderSlot = &s
		}
		members = append(members, m)
	}

	if err := rows.Err(); err != nil {
		log.Printf("MulticastGroupMembers rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if members == nil {
		members = []MulticastMember{}
	}

	// Enrich paginated members with traffic rates
	if len(members) > 0 {
		type tunnelKey struct {
			devicePK string
			tunnelID int64
		}
		tunnelToMembers := make(map[tunnelKey][]int)
		for i, m := range members {
			if m.TunnelID > 0 {
				key := tunnelKey{m.DevicePK, int64(m.TunnelID)}
				tunnelToMembers[key] = append(tunnelToMembers[key], i)
			}
		}

		trafficQuery := `
			SELECT
				device_pk,
				user_tunnel_id,
				(sum(coalesce(in_octets_delta, 0)) * 8.0) / sum(delta_duration) as in_bps,
				(sum(coalesce(out_octets_delta, 0)) * 8.0) / sum(delta_duration) as out_bps,
				sum(coalesce(in_pkts_delta, 0)) / sum(delta_duration) as in_pps,
				sum(coalesce(out_pkts_delta, 0)) / sum(delta_duration) as out_pps
			FROM fact_dz_device_interface_counters
			WHERE event_ts >= now() - INTERVAL 5 MINUTE
				AND user_tunnel_id > 0
				AND delta_duration > 0
			GROUP BY device_pk, user_tunnel_id
		`

		trafficRows, err := envDB(ctx).Query(ctx, trafficQuery)
		if err != nil {
			log.Printf("MulticastGroupMembers traffic query error (non-fatal): %v", err)
		} else {
			defer trafficRows.Close()
			for trafficRows.Next() {
				var devicePK string
				var tunnelID int64
				var inBps, outBps, inPps, outPps float64
				if err := trafficRows.Scan(&devicePK, &tunnelID, &inBps, &outBps, &inPps, &outPps); err != nil {
					log.Printf("MulticastGroupMembers traffic scan error: %v", err)
					continue
				}
				key := tunnelKey{devicePK, tunnelID}
				if indices, ok := tunnelToMembers[key]; ok {
					for _, idx := range indices {
						if members[idx].Mode == "P" || members[idx].Mode == "P+S" {
							members[idx].TrafficBps = inBps
							members[idx].TrafficPps = inPps
						} else {
							members[idx].TrafficBps = outBps
							members[idx].TrafficPps = outPps
						}
					}
				}
			}
		}
	}

	// Enrich publishers with leader schedule timing (last_leader_slot, is_leader, current_slot)
	if len(members) > 0 {
		clientIPToMembers := make(map[string][]int)
		for i, m := range members {
			if (m.Mode == "P" || m.Mode == "P+S") && m.ClientIP != "" {
				clientIPToMembers[m.ClientIP] = append(clientIPToMembers[m.ClientIP], i)
			}
		}

		if len(clientIPToMembers) > 0 {
			clientIPs := make([]string, 0, len(clientIPToMembers))
			for ip := range clientIPToMembers {
				clientIPs = append(clientIPs, ip)
			}

			leaderQuery := `
				WITH current AS (
					SELECT max(cluster_slot) as slot
					FROM fact_solana_vote_account_activity
					WHERE event_ts >= now() - INTERVAL 2 MINUTE
				),
				epoch_info AS (
					SELECT
						toUInt64(current.slot) as abs_slot,
						ls.epoch as epoch,
						toUInt64(ls.epoch) * 432000 as epoch_start,
						toUInt64(current.slot) - (toUInt64(ls.epoch) * 432000) as slot_in_epoch
					FROM solana_leader_schedule_current ls
					CROSS JOIN current
					LIMIT 1
				)
				SELECT
					g.gossip_ip as client_ip,
					ei.abs_slot as current_slot,
					has(JSONExtract(ls.slots, 'Array(UInt64)'), ei.slot_in_epoch) as is_leader,
					if(empty(arrayFilter(x -> x <= ei.slot_in_epoch, JSONExtract(ls.slots, 'Array(UInt64)'))), 0,
						ei.epoch_start + arrayMax(arrayFilter(x -> x <= ei.slot_in_epoch, JSONExtract(ls.slots, 'Array(UInt64)')))) as last_leader_slot
				FROM solana_leader_schedule_current ls
				JOIN solana_gossip_nodes_current g ON g.pubkey = ls.node_pubkey
				CROSS JOIN epoch_info ei
				WHERE g.gossip_ip IN (?)
			`

			leaderRows, err := envDB(ctx).Query(ctx, leaderQuery, clientIPs)
			if err != nil {
				log.Printf("MulticastGroupMembers leader query error (non-fatal): %v", err)
			} else {
				defer leaderRows.Close()
				for leaderRows.Next() {
					var clientIP string
					var currentSlot uint64
					var isLeader uint8
					var lastSlot uint64
					if err := leaderRows.Scan(&clientIP, &currentSlot, &isLeader, &lastSlot); err != nil {
						log.Printf("MulticastGroupMembers leader scan error: %v", err)
						continue
					}
					if indices, ok := clientIPToMembers[clientIP]; ok {
						for _, idx := range indices {
							members[idx].IsLeader = isLeader != 0
							members[idx].CurrentSlot = int64(currentSlot)
							if lastSlot > 0 {
								s := int64(lastSlot)
								members[idx].LastLeaderSlot = &s
							}
						}
					}
				}
			}
		}
	}

	response := MulticastMembersResponse{
		Items:           members,
		Total:           int(total),
		PublisherCount:  int(pubCount),
		SubscriberCount: int(subCount),
		Limit:           pagination.Limit,
		Offset:          pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("JSON encoding error: %v", err)
	}
}

type MulticastTrafficPoint struct {
	Time     string  `json:"time"`
	DevicePK string  `json:"device_pk"`
	TunnelID int64   `json:"tunnel_id"`
	Mode     string  `json:"mode"` // "P" or "S"
	InBps    float64 `json:"in_bps"`
	OutBps   float64 `json:"out_bps"`
	InPps    float64 `json:"in_pps"`
	OutPps   float64 `json:"out_pps"`
}

func GetMulticastGroupTraffic(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing multicast group pk", http.StatusBadRequest)
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
		interval, lookback = "10", "1 HOUR"
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

	// Resolve pk from pk or code
	var groupPK string
	err := envDB(ctx).QueryRow(ctx,
		`SELECT pk FROM dz_multicast_groups_current WHERE pk = ? OR code = ?`, pkOrCode, pkOrCode).Scan(&groupPK)
	if err != nil {
		log.Printf("MulticastGroupTraffic group query error: %v", err)
		http.Error(w, "multicast group not found", http.StatusNotFound)
		return
	}

	// Get members with their device_pk, tunnel_id, and mode
	membersQuery := `
		SELECT
			COALESCE(u.device_pk, '') as device_pk,
			COALESCE(u.tunnel_id, 0) as tunnel_id,
			CASE
				WHEN has(JSONExtract(u.publishers, 'Array(String)'), ?) AND has(JSONExtract(u.subscribers, 'Array(String)'), ?) THEN 'P'
				WHEN has(JSONExtract(u.publishers, 'Array(String)'), ?) THEN 'P'
				ELSE 'S'
			END as mode
		FROM dz_users_current u
		WHERE u.status = 'activated'
			AND u.kind = 'multicast'
			AND (
				has(JSONExtract(u.publishers, 'Array(String)'), ?)
				OR has(JSONExtract(u.subscribers, 'Array(String)'), ?)
			)
	`

	memberRows, err := envDB(ctx).Query(ctx, membersQuery, groupPK, groupPK, groupPK, groupPK, groupPK)
	if err != nil {
		log.Printf("MulticastGroupTraffic members query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer memberRows.Close()

	type memberInfo struct {
		devicePK string
		tunnelID int32
		mode     string
	}
	var members []memberInfo
	tunnelIDs := make([]int64, 0)

	for memberRows.Next() {
		var m memberInfo
		if err := memberRows.Scan(&m.devicePK, &m.tunnelID, &m.mode); err != nil {
			log.Printf("MulticastGroupTraffic members scan error: %v", err)
			continue
		}
		if m.tunnelID > 0 {
			members = append(members, m)
			tunnelIDs = append(tunnelIDs, int64(m.tunnelID))
		}
	}

	if len(tunnelIDs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
		return
	}

	// Build lookup: (device_pk, tunnel_id) -> mode
	type tunnelKey struct {
		devicePK string
		tunnelID int64
	}
	tunnelMode := make(map[tunnelKey]string)
	devicePKs := make([]string, 0, len(members))
	for _, m := range members {
		tunnelMode[tunnelKey{m.devicePK, int64(m.tunnelID)}] = m.mode
		devicePKs = append(devicePKs, m.devicePK)
	}

	// Query traffic time series — filter by device_pk and tunnel_id independently,
	// then post-filter to exact (device_pk, tunnel_id) pairs to avoid cross-matches
	trafficQuery := `
		SELECT
			formatDateTime(toStartOfInterval(event_ts, INTERVAL ` + interval + ` SECOND), '%Y-%m-%dT%H:%i:%s') as time,
			device_pk,
			user_tunnel_id as tunnel_id,
			CASE WHEN SUM(delta_duration) > 0
				THEN SUM(in_octets_delta) * 8 / SUM(delta_duration)
				ELSE 0
			END as in_bps,
			CASE WHEN SUM(delta_duration) > 0
				THEN SUM(out_octets_delta) * 8 / SUM(delta_duration)
				ELSE 0
			END as out_bps,
			CASE WHEN SUM(delta_duration) > 0
				THEN SUM(in_pkts_delta) / SUM(delta_duration)
				ELSE 0
			END as in_pps,
			CASE WHEN SUM(delta_duration) > 0
				THEN SUM(out_pkts_delta) / SUM(delta_duration)
				ELSE 0
			END as out_pps
		FROM fact_dz_device_interface_counters
		WHERE event_ts > now() - INTERVAL ` + lookback + `
			AND user_tunnel_id IN (?)
			AND device_pk IN (?)
			AND delta_duration > 0
			AND (in_octets_delta >= 0 OR out_octets_delta >= 0)
		GROUP BY time, device_pk, tunnel_id
		ORDER BY time, device_pk, tunnel_id
	`

	trafficRows, err := envDB(ctx).Query(ctx, trafficQuery, tunnelIDs, devicePKs)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("MulticastGroupTraffic traffic query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer trafficRows.Close()

	var points []MulticastTrafficPoint
	for trafficRows.Next() {
		var p MulticastTrafficPoint
		if err := trafficRows.Scan(&p.Time, &p.DevicePK, &p.TunnelID, &p.InBps, &p.OutBps, &p.InPps, &p.OutPps); err != nil {
			log.Printf("MulticastGroupTraffic traffic scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Only include rows for exact (device_pk, tunnel_id) member pairs
		key := tunnelKey{p.DevicePK, p.TunnelID}
		mode, ok := tunnelMode[key]
		if !ok {
			continue
		}
		p.Mode = mode
		points = append(points, p)
	}

	if err := trafficRows.Err(); err != nil {
		log.Printf("MulticastGroupTraffic rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if points == nil {
		points = []MulticastTrafficPoint{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(points); err != nil {
		log.Printf("JSON encoding error: %v", err)
	}
}

type MulticastMemberCountPoint struct {
	Time            string `json:"time"`
	PublisherCount  int64  `json:"publisher_count"`
	SubscriberCount int64  `json:"subscriber_count"`
}

func GetMulticastGroupMemberCounts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing multicast group pk", http.StatusBadRequest)
		return
	}

	timeRange := r.URL.Query().Get("time_range")
	if timeRange == "" {
		timeRange = "7d"
	}

	var lookback string
	switch timeRange {
	case "1h":
		lookback = "1 HOUR"
	case "6h":
		lookback = "6 HOUR"
	case "12h":
		lookback = "12 HOUR"
	case "24h":
		lookback = "24 HOUR"
	case "7d":
		lookback = "7 DAY"
	case "30d":
		lookback = "30 DAY"
	default:
		lookback = "24 HOUR"
	}

	start := time.Now()

	// Resolve group PK
	var groupPK string
	err := envDB(ctx).QueryRow(ctx,
		`SELECT pk FROM dz_multicast_groups_current WHERE pk = ? OR code = ?`, pkOrCode, pkOrCode).Scan(&groupPK)
	if err != nil {
		log.Printf("MulticastGroupMemberCounts group query error: %v", err)
		http.Error(w, "multicast group not found", http.StatusNotFound)
		return
	}

	// Reconstruct member counts over time from SCD user history.
	// 1. For each user change, compute whether they're a pub/sub of this group
	// 2. Use lag() to compute deltas (joined/left)
	// 3. Running sum of deltas gives the count at each point in time
	// The running sum is computed from all history for correctness. We return
	// the last point before the window (as a baseline) plus all points within it.
	query := `
		WITH
			user_changes AS (
				SELECT
					snapshot_ts as ts,
					pk as user_pk,
					CASE WHEN status = 'activated' AND is_deleted = 0
						THEN has(JSONExtract(publishers, 'Array(String)'), ?)
						ELSE 0
					END as is_pub,
					CASE WHEN status = 'activated' AND is_deleted = 0
						THEN has(JSONExtract(subscribers, 'Array(String)'), ?)
						ELSE 0
					END as is_sub,
					lagInFrame(CASE WHEN status = 'activated' AND is_deleted = 0
						THEN has(JSONExtract(publishers, 'Array(String)'), ?)
						ELSE 0
					END, 1, 0) OVER (PARTITION BY pk ORDER BY snapshot_ts, ingested_at, op_id) as prev_pub,
					lagInFrame(CASE WHEN status = 'activated' AND is_deleted = 0
						THEN has(JSONExtract(subscribers, 'Array(String)'), ?)
						ELSE 0
					END, 1, 0) OVER (PARTITION BY pk ORDER BY snapshot_ts, ingested_at, op_id) as prev_sub
				FROM dim_dz_users_history
				WHERE kind = 'multicast'
			),
			deltas AS (
				SELECT ts, toInt32(is_pub) - toInt32(prev_pub) as pub_delta, toInt32(is_sub) - toInt32(prev_sub) as sub_delta
				FROM user_changes
				WHERE is_pub != prev_pub OR is_sub != prev_sub
			),
			agg_deltas AS (
				SELECT ts, sum(pub_delta) as pub_delta, sum(sub_delta) as sub_delta
				FROM deltas GROUP BY ts
			),
			running AS (
				SELECT ts,
					sum(pub_delta) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as publisher_count,
					sum(sub_delta) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as subscriber_count
				FROM agg_deltas
			),
			cutoff AS (
				SELECT now() - INTERVAL ` + lookback + ` as t
			),
			-- Include the last point before the window as baseline, plus all points in the window
			filtered AS (
				SELECT ts, publisher_count, subscriber_count,
					row_number() OVER (ORDER BY ts DESC) as rn_before
				FROM running, cutoff
				WHERE ts <= cutoff.t
			)
		SELECT time, publisher_count, subscriber_count FROM (
			SELECT formatDateTime(ts, '%Y-%m-%dT%H:%i:%s') as time, publisher_count, subscriber_count
			FROM filtered WHERE rn_before = 1
			UNION ALL
			SELECT formatDateTime(ts, '%Y-%m-%dT%H:%i:%s') as time, publisher_count, subscriber_count
			FROM running
			WHERE ts > now() - INTERVAL ` + lookback + `
		) ORDER BY time
	`

	rows, err := envDB(ctx).Query(ctx, query, groupPK, groupPK, groupPK, groupPK)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("MulticastGroupMemberCounts query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var points []MulticastMemberCountPoint
	for rows.Next() {
		var p MulticastMemberCountPoint
		if err := rows.Scan(&p.Time, &p.PublisherCount, &p.SubscriberCount); err != nil {
			log.Printf("MulticastGroupMemberCounts scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		points = append(points, p)
	}

	if points == nil {
		points = []MulticastMemberCountPoint{}
	}

	// Ensure the chart X-axis starts at the window start time.
	// If the first point (baseline) is before the window, clamp it to the window start.
	// If it's after the window start, prepend a point at the window start with the baseline values.
	if len(points) > 0 {
		var windowDuration time.Duration
		switch timeRange {
		case "1h":
			windowDuration = time.Hour
		case "6h":
			windowDuration = 6 * time.Hour
		case "12h":
			windowDuration = 12 * time.Hour
		case "24h":
			windowDuration = 24 * time.Hour
		case "7d":
			windowDuration = 7 * 24 * time.Hour
		case "30d":
			windowDuration = 30 * 24 * time.Hour
		default:
			windowDuration = 24 * time.Hour
		}
		windowStart := time.Now().UTC().Add(-windowDuration).Format("2006-01-02T15:04:05")
		if points[0].Time < windowStart {
			// Baseline is before the window — clamp its timestamp to window start
			points[0].Time = windowStart
		} else if points[0].Time > windowStart {
			// First data point is after window start — prepend a point at window start
			points = append([]MulticastMemberCountPoint{{
				Time:            windowStart,
				PublisherCount:  points[0].PublisherCount,
				SubscriberCount: points[0].SubscriberCount,
			}}, points...)
		}
	}

	// Append a "now" point with the last known counts so the chart extends to current time
	if len(points) > 0 {
		last := points[len(points)-1]
		nowStr := time.Now().UTC().Format("2006-01-02T15:04:05")
		if last.Time != nowStr {
			points = append(points, MulticastMemberCountPoint{
				Time:            nowStr,
				PublisherCount:  last.PublisherCount,
				SubscriberCount: last.SubscriberCount,
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(points); err != nil {
		log.Printf("JSON encoding error: %v", err)
	}
}

// MulticastTreeHop represents a single hop in a multicast tree path
type MulticastTreeHop struct {
	DevicePK   string `json:"devicePK"`
	DeviceCode string `json:"deviceCode"`
	DeviceType string `json:"deviceType"`
	EdgeMetric int    `json:"edgeMetric,omitempty"` // metric to reach this hop from previous
}

// MulticastTreePath represents a path from publisher to subscriber
type MulticastTreePath struct {
	PublisherDevicePK    string             `json:"publisherDevicePK"`
	PublisherDeviceCode  string             `json:"publisherDeviceCode"`
	SubscriberDevicePK   string             `json:"subscriberDevicePK"`
	SubscriberDeviceCode string             `json:"subscriberDeviceCode"`
	Path                 []MulticastTreeHop `json:"path"`
	TotalMetric          int                `json:"totalMetric"`
	HopCount             int                `json:"hopCount"`
}

// MulticastTreeResponse is the response for multicast tree paths endpoint
type MulticastTreeResponse struct {
	GroupCode       string              `json:"groupCode"`
	GroupPK         string              `json:"groupPK"`
	PublisherCount  int                 `json:"publisherCount"`
	SubscriberCount int                 `json:"subscriberCount"`
	Paths           []MulticastTreePath `json:"paths"`
	Error           string              `json:"error,omitempty"`
}

// GetMulticastTreePaths computes paths from all publishers to all subscribers in a multicast group
func GetMulticastTreePaths(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		writeJSON(w, MulticastTreeResponse{Error: "missing multicast group pk"})
		return
	}

	start := time.Now()
	response := MulticastTreeResponse{
		Paths: []MulticastTreePath{},
	}

	// First get group info and members from ClickHouse (accept pk or code)
	groupQuery := `
		SELECT pk, COALESCE(code, '') FROM dz_multicast_groups_current WHERE pk = ? OR code = ?
	`
	err := envDB(ctx).QueryRow(ctx, groupQuery, pkOrCode, pkOrCode).Scan(&response.GroupPK, &response.GroupCode)
	if err != nil {
		log.Printf("MulticastTreePaths group query error: %v", err)
		response.Error = "multicast group not found"
		writeJSON(w, response)
		return
	}

	// Get publishers and subscribers
	membersQuery := `
		SELECT
			CASE
				WHEN has(JSONExtract(u.publishers, 'Array(String)'), ?) AND has(JSONExtract(u.subscribers, 'Array(String)'), ?) THEN 'P+S'
				WHEN has(JSONExtract(u.publishers, 'Array(String)'), ?) THEN 'P'
				ELSE 'S'
			END as mode,
			COALESCE(u.device_pk, '') as device_pk,
			COALESCE(d.code, '') as device_code
		FROM dz_users_current u
		LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
		WHERE u.status = 'activated'
			AND u.kind = 'multicast'
			AND (
				has(JSONExtract(u.publishers, 'Array(String)'), ?)
				OR has(JSONExtract(u.subscribers, 'Array(String)'), ?)
			)
	`

	rows, err := envDB(ctx).Query(ctx, membersQuery, response.GroupPK, response.GroupPK, response.GroupPK, response.GroupPK, response.GroupPK)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("MulticastTreePaths members query error: %v", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}
	defer rows.Close()

	type deviceInfo struct {
		PK   string
		Code string
	}
	var publishers, subscribers []deviceInfo
	publisherSet := make(map[string]bool)
	subscriberSet := make(map[string]bool)

	for rows.Next() {
		var mode, devicePK, deviceCode string
		if err := rows.Scan(&mode, &devicePK, &deviceCode); err != nil {
			log.Printf("MulticastTreePaths members scan error: %v", err)
			continue
		}
		if devicePK == "" {
			continue // Skip members without device assignment
		}

		// Publishers: P or P+S
		if (mode == "P" || mode == "P+S") && !publisherSet[devicePK] {
			publishers = append(publishers, deviceInfo{PK: devicePK, Code: deviceCode})
			publisherSet[devicePK] = true
		}
		// Subscribers: S or P+S
		if (mode == "S" || mode == "P+S") && !subscriberSet[devicePK] {
			subscribers = append(subscribers, deviceInfo{PK: devicePK, Code: deviceCode})
			subscriberSet[devicePK] = true
		}
	}

	response.PublisherCount = len(publishers)
	response.SubscriberCount = len(subscribers)

	if len(publishers) == 0 || len(subscribers) == 0 {
		response.Error = "no publishers or subscribers found with device assignments"
		writeJSON(w, response)
		return
	}

	// Find paths from each publisher to each subscriber using Neo4j
	type pathResult struct {
		path MulticastTreePath
		err  error
	}

	var wg sync.WaitGroup
	resultChan := make(chan pathResult, len(publishers)*len(subscribers))
	sem := make(chan struct{}, 10) // Limit concurrent queries

	for _, pub := range publishers {
		for _, sub := range subscribers {
			if pub.PK == sub.PK {
				continue // Skip self-paths
			}
			wg.Add(1)
			go func(pubPK, pubCode, subPK, subCode string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				queryCtx, queryCancel := context.WithTimeout(ctx, 5*time.Second)
				defer queryCancel()

				session := config.Neo4jSession(queryCtx)
				defer session.Close(queryCtx)

				// Use Dijkstra to find lowest latency path from publisher to subscriber
				cypher := `
					MATCH (a:Device {pk: $from_pk}), (b:Device {pk: $to_pk})
					CALL apoc.algo.dijkstra(a, b, 'ISIS_ADJACENT>', 'metric') YIELD path, weight
					WITH path, toInteger(weight) AS totalMetric
					RETURN [n IN nodes(path) | {
						pk: n.pk,
						code: n.code,
						device_type: n.device_type
					}] AS devices,
					[r IN relationships(path) | r.metric] AS edgeMetrics,
					totalMetric
					LIMIT 1
				`

				result, err := session.Run(queryCtx, cypher, map[string]any{
					"from_pk": pubPK,
					"to_pk":   subPK,
				})
				if err != nil {
					resultChan <- pathResult{err: err}
					return
				}

				record, err := result.Single(queryCtx)
				if err != nil {
					// No path found - not an error, just skip
					return
				}

				// Parse the path
				devicesVal, _ := record.Get("devices")
				edgeMetricsVal, _ := record.Get("edgeMetrics")
				totalMetric, _ := record.Get("totalMetric")

				var hops []MulticastTreeHop
				if deviceList, ok := devicesVal.([]any); ok {
					var metrics []int
					if metricList, ok := edgeMetricsVal.([]any); ok {
						for _, m := range metricList {
							if v, ok := m.(int64); ok {
								metrics = append(metrics, int(v))
							}
						}
					}

					for i, d := range deviceList {
						if dm, ok := d.(map[string]any); ok {
							hop := MulticastTreeHop{
								DevicePK:   asString(dm["pk"]),
								DeviceCode: asString(dm["code"]),
								DeviceType: asString(dm["device_type"]),
							}
							// Edge metric is from previous hop to this hop
							if i > 0 && i-1 < len(metrics) {
								hop.EdgeMetric = metrics[i-1]
							}
							hops = append(hops, hop)
						}
					}
				}

				if len(hops) > 0 {
					treePath := MulticastTreePath{
						PublisherDevicePK:    pubPK,
						PublisherDeviceCode:  pubCode,
						SubscriberDevicePK:   subPK,
						SubscriberDeviceCode: subCode,
						Path:                 hops,
						HopCount:             len(hops) - 1,
					}
					if tm, ok := totalMetric.(int64); ok {
						treePath.TotalMetric = int(tm)
					}
					resultChan <- pathResult{path: treePath}
				}
			}(pub.PK, pub.Code, sub.PK, sub.Code)
		}
	}

	// Wait for all goroutines and close channel
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	for result := range resultChan {
		if result.err != nil {
			log.Printf("MulticastTreePaths path query error: %v", result.err)
			continue
		}
		response.Paths = append(response.Paths, result.path)
	}

	log.Printf("MulticastTreePaths: %d paths found in %v", len(response.Paths), time.Since(start))
	writeJSON(w, response)
}
