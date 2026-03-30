package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
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

func (a *API) GetMulticastGroups(w http.ResponseWriter, r *http.Request) {
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

	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("multicast groups query error", "error", err)
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
			logError("multicast groups scan error", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		groups = append(groups, g)
	}

	if err := rows.Err(); err != nil {
		logError("multicast groups rows error", "error", err)
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

		countRows, err := a.envDB(ctx).Query(ctx, countsQuery, groupPKs)
		if err != nil {
			slog.Warn("multicast groups counts query error", "error", err)
		} else {
			defer countRows.Close()
			for countRows.Next() {
				var gpk string
				var pubCount, subCount uint64
				if err := countRows.Scan(&gpk, &pubCount, &subCount); err != nil {
					logError("multicast groups counts scan error", "error", err)
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
		logError("failed to encode response", "error", err)
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

func (a *API) GetMulticastGroup(w http.ResponseWriter, r *http.Request) {
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
	err := a.envDB(ctx).QueryRow(ctx, groupQuery, pkOrCode, pkOrCode).Scan(
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
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "multicast group not found", http.StatusNotFound)
			return
		}
		logError("multicast group query error", "error", err)
		http.Error(w, "multicast group not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(group); err != nil {
		logError("failed to encode response", "error", err)
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

func (a *API) GetMulticastGroupMembers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing multicast group pk", http.StatusBadRequest)
		return
	}

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "stake_sol", multicastMemberSortFields)
	filters := ParseFilters(r)
	tab := r.URL.Query().Get("tab")
	if tab != "publishers" && tab != "subscribers" {
		tab = "publishers"
	}

	start := time.Now()

	// Resolve group PK
	var groupPK string
	err := a.envDB(ctx).QueryRow(ctx,
		`SELECT pk FROM dz_multicast_groups_current WHERE pk = ? OR code = ?`, pkOrCode, pkOrCode).Scan(&groupPK)
	if err != nil {
		logError("multicast group members group query error", "error", err)
		http.Error(w, "multicast group not found", http.StatusNotFound)
		return
	}

	// Tab filter: publishers tab shows P and P+S, subscribers tab shows S and P+S
	tabFilter := ""
	if tab == "publishers" {
		tabFilter = " AND is_publisher = 1"
	} else {
		tabFilter = " AND is_subscriber = 1"
	}

	// Build filter clause
	filterClause, filterArgs := filters.BuildFilterClause(multicastMemberFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}

	// Lightweight CTE for counts — no gossip/vote/leader joins needed
	countCTE := `
		WITH members_base AS (
			SELECT
				has(JSONExtract(u.publishers, 'Array(String)'), ?) as is_publisher,
				has(JSONExtract(u.subscribers, 'Array(String)'), ?) as is_subscriber,
				COALESCE(d.code, '') as device_code,
				COALESCE(m.name, '') as metro_name,
				COALESCE(u.owner_pubkey, '') as owner_pubkey
			FROM dz_users_current u
			LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			WHERE u.status = 'activated'
				AND u.kind = 'multicast'
				AND (
					has(JSONExtract(u.publishers, 'Array(String)'), ?)
					OR has(JSONExtract(u.subscribers, 'Array(String)'), ?)
				)
		)
	`
	countBaseArgs := []any{groupPK, groupPK, groupPK, groupPK}

	// Single count query returning all three counts
	countQuery := countCTE + `
		SELECT
			countIf(1=1` + tabFilter + whereFilter + `) as total,
			countIf(is_publisher = 1` + whereFilter + `) as pub_count,
			countIf(is_subscriber = 1` + whereFilter + `) as sub_count
		FROM members_base WHERE 1=1
	`
	// Filter args appear 3 times in the count query (once per countIf)
	countArgs := append([]any{}, countBaseArgs...)
	countArgs = append(countArgs, filterArgs...)
	countArgs = append(countArgs, filterArgs...)
	countArgs = append(countArgs, filterArgs...)

	// Full CTE for data query — includes gossip/vote/leader joins for sorting
	dataCTE := `
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
	dataBaseArgs := []any{groupPK, groupPK, groupPK, groupPK, groupPK, groupPK, groupPK}

	orderBy := sort.OrderByClause(multicastMemberSortFields)
	dataQuery := dataCTE + `
		SELECT user_pk, mode, device_pk, device_code, metro_pk, metro_code, metro_name,
			client_ip, dz_ip, status, owner_pubkey, tunnel_id, node_pubkey, vote_pubkey, stake_sol, next_leader_slot
		FROM members_base
		WHERE 1=1` + tabFilter + whereFilter + `
		` + orderBy + `
		LIMIT ? OFFSET ?
	`
	dataArgs := append(append([]any{}, dataBaseArgs...), filterArgs...)
	dataArgs = append(dataArgs, pagination.Limit, pagination.Offset)

	// Run count query and data query in parallel
	type countResult struct {
		total, pubCount, subCount uint64
		err                       error
	}
	type dataResult struct {
		members []MulticastMember
		err     error
	}

	countCh := make(chan countResult, 1)
	dataCh := make(chan dataResult, 1)

	go func() {
		var r countResult
		r.err = a.envDB(ctx).QueryRow(ctx, countQuery, countArgs...).Scan(&r.total, &r.pubCount, &r.subCount)
		countCh <- r
	}()

	go func() {
		rows, err := a.envDB(ctx).Query(ctx, dataQuery, dataArgs...)
		if err != nil {
			dataCh <- dataResult{err: err}
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
				dataCh <- dataResult{err: err}
				return
			}
			if nextLeaderSlot > 0 {
				s := int64(nextLeaderSlot)
				m.NextLeaderSlot = &s
			}
			members = append(members, m)
		}
		if err := rows.Err(); err != nil {
			dataCh <- dataResult{err: err}
			return
		}
		dataCh <- dataResult{members: members}
	}()

	cr := <-countCh
	dr := <-dataCh
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, cr.err)

	if cr.err != nil {
		slog.Warn("multicast group members count query failed", "error", cr.err)
		http.Error(w, cr.err.Error(), http.StatusInternalServerError)
		return
	}
	if dr.err != nil {
		slog.Warn("multicast group members data query failed", "error", dr.err)
		http.Error(w, dr.err.Error(), http.StatusInternalServerError)
		return
	}

	members := dr.members
	if members == nil {
		members = []MulticastMember{}
	}

	// Enrich paginated members with traffic rates and leader schedule in parallel
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

		clientIPToMembers := make(map[string][]int)
		for i, m := range members {
			if (m.Mode == "P" || m.Mode == "P+S") && m.ClientIP != "" {
				clientIPToMembers[m.ClientIP] = append(clientIPToMembers[m.ClientIP], i)
			}
		}

		type trafficResult struct {
			data map[tunnelKey]struct{ inBps, outBps, inPps, outPps float64 }
			err  error
		}
		type leaderResult struct {
			data map[string]struct {
				currentSlot uint64
				isLeader    bool
				lastSlot    uint64
			}
			err error
		}

		trafficCh := make(chan trafficResult, 1)
		leaderCh := make(chan leaderResult, 1)

		go func() {
			tr := trafficResult{data: make(map[tunnelKey]struct{ inBps, outBps, inPps, outPps float64 })}
			trafficQuery := `
				SELECT
					device_pk,
					user_tunnel_id,
					avg(avg_in_bps) as in_bps,
					avg(avg_out_bps) as out_bps,
					avg(avg_in_pps) as in_pps,
					avg(avg_out_pps) as out_pps
				FROM device_interface_rollup_5m
				WHERE bucket_ts >= now() - INTERVAL 15 MINUTE
					AND user_tunnel_id > 0
				GROUP BY device_pk, user_tunnel_id
			`
			rows, err := a.envDB(ctx).Query(ctx, trafficQuery)
			if err != nil {
				tr.err = err
				trafficCh <- tr
				return
			}
			defer rows.Close()
			for rows.Next() {
				var devicePK string
				var tunnelID int64
				var inBps, outBps, inPps, outPps float64
				if err := rows.Scan(&devicePK, &tunnelID, &inBps, &outBps, &inPps, &outPps); err != nil {
					continue
				}
				key := tunnelKey{devicePK, tunnelID}
				tr.data[key] = struct{ inBps, outBps, inPps, outPps float64 }{inBps, outBps, inPps, outPps}
			}
			trafficCh <- tr
		}()

		go func() {
			lr := leaderResult{data: make(map[string]struct {
				currentSlot uint64
				isLeader    bool
				lastSlot    uint64
			})}
			if len(clientIPToMembers) == 0 {
				leaderCh <- lr
				return
			}
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
			rows, err := a.envDB(ctx).Query(ctx, leaderQuery, clientIPs)
			if err != nil {
				lr.err = err
				leaderCh <- lr
				return
			}
			defer rows.Close()
			for rows.Next() {
				var clientIP string
				var currentSlot uint64
				var isLeader uint8
				var lastSlot uint64
				if err := rows.Scan(&clientIP, &currentSlot, &isLeader, &lastSlot); err != nil {
					continue
				}
				lr.data[clientIP] = struct {
					currentSlot uint64
					isLeader    bool
					lastSlot    uint64
				}{currentSlot, isLeader != 0, lastSlot}
			}
			leaderCh <- lr
		}()

		tr := <-trafficCh
		lr := <-leaderCh

		if tr.err != nil {
			slog.Warn("multicast group members traffic query error", "error", tr.err)
		} else {
			for key, vals := range tr.data {
				if indices, ok := tunnelToMembers[key]; ok {
					for _, idx := range indices {
						if members[idx].Mode == "P" || members[idx].Mode == "P+S" {
							members[idx].TrafficBps = vals.inBps
							members[idx].TrafficPps = vals.inPps
						} else {
							members[idx].TrafficBps = vals.outBps
							members[idx].TrafficPps = vals.outPps
						}
					}
				}
			}
		}

		if lr.err != nil {
			slog.Warn("multicast group members leader query error", "error", lr.err)
		} else {
			for clientIP, vals := range lr.data {
				if indices, ok := clientIPToMembers[clientIP]; ok {
					for _, idx := range indices {
						members[idx].IsLeader = vals.isLeader
						members[idx].CurrentSlot = int64(vals.currentSlot)
						if vals.lastSlot > 0 {
							s := int64(vals.lastSlot)
							members[idx].LastLeaderSlot = &s
						}
					}
				}
			}
		}
	}

	response := MulticastMembersResponse{
		Items:           members,
		Total:           int(cr.total),
		PublisherCount:  int(cr.pubCount),
		SubscriberCount: int(cr.subCount),
		Limit:           pagination.Limit,
		Offset:          pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
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

func (a *API) GetMulticastGroupTraffic(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing multicast group pk", http.StatusBadRequest)
		return
	}

	// Use shared time filter with raw/rollup routing
	timeFilter, bucketInterval, useRaw := trafficTimeFilter(r)

	start := time.Now()

	// Resolve pk from pk or code
	var groupPK string
	err := a.envDB(ctx).QueryRow(ctx,
		`SELECT pk FROM dz_multicast_groups_current WHERE pk = ? OR code = ?`, pkOrCode, pkOrCode).Scan(&groupPK)
	if err != nil {
		logError("multicast group traffic group query error", "error", err)
		http.Error(w, "multicast group not found", http.StatusNotFound)
		return
	}

	// Get members with their pk, device_pk, tunnel_id, and mode
	membersQuery := `
		SELECT
			u.pk,
			COALESCE(u.device_pk, '') as device_pk,
			COALESCE(u.tunnel_id, 0) as tunnel_id,
			CASE
				WHEN has(JSONExtract(u.publishers, 'Array(String)'), ?) AND has(JSONExtract(u.subscribers, 'Array(String)'), ?) THEN 'P+S'
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

	memberRows, err := a.envDB(ctx).Query(ctx, membersQuery, groupPK, groupPK, groupPK, groupPK, groupPK)
	if err != nil {
		logError("multicast group traffic members query error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer memberRows.Close()

	type memberInfo struct {
		userPK   string
		devicePK string
		tunnelID int32
		mode     string
	}
	var members []memberInfo
	tunnelIDs := make([]int64, 0)
	userPKs := make([]string, 0)

	for memberRows.Next() {
		var m memberInfo
		if err := memberRows.Scan(&m.userPK, &m.devicePK, &m.tunnelID, &m.mode); err != nil {
			logError("multicast group traffic members scan error", "error", err)
			continue
		}
		if m.tunnelID > 0 {
			members = append(members, m)
			tunnelIDs = append(tunnelIDs, int64(m.tunnelID))
			userPKs = append(userPKs, m.userPK)
		}
	}

	if len(members) == 0 {
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
	var trafficQuery string
	if useRaw {
		trafficQuery = fmt.Sprintf(`
			SELECT
				formatDateTime(toStartOfInterval(event_ts, INTERVAL %s), '%%Y-%%m-%%dT%%H:%%i:%%sZ') as time,
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
			WHERE %s
				AND user_tunnel_id IN (?)
				AND device_pk IN (?)
				AND delta_duration > 0
				AND (in_octets_delta >= 0 OR out_octets_delta >= 0)
			GROUP BY time, device_pk, tunnel_id
			ORDER BY time, device_pk, tunnel_id
		`, bucketInterval, timeFilter)
	} else {
		trafficQuery = fmt.Sprintf(`
			SELECT
				formatDateTime(toStartOfInterval(bucket_ts, INTERVAL %s), '%%Y-%%m-%%dT%%H:%%i:%%sZ') as time,
				device_pk,
				user_tunnel_id as tunnel_id,
				MAX(max_in_bps) as in_bps,
				MAX(max_out_bps) as out_bps,
				MAX(max_in_pps) as in_pps,
				MAX(max_out_pps) as out_pps
			FROM device_interface_rollup_5m
			WHERE %s
				AND user_pk IN (?)
				AND device_pk IN (?)
			GROUP BY time, device_pk, tunnel_id
			ORDER BY time, device_pk, tunnel_id
		`, bucketInterval, timeFilter)
	}

	var filterIDs any
	if useRaw {
		filterIDs = tunnelIDs
	} else {
		filterIDs = userPKs
	}
	trafficRows, err := a.envDB(ctx).Query(ctx, trafficQuery, filterIDs, devicePKs)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("multicast group traffic query error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer trafficRows.Close()

	var points []MulticastTrafficPoint
	for trafficRows.Next() {
		var p MulticastTrafficPoint
		if err := trafficRows.Scan(&p.Time, &p.DevicePK, &p.TunnelID, &p.InBps, &p.OutBps, &p.InPps, &p.OutPps); err != nil {
			logError("multicast group traffic scan error", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Only include rows for exact (device_pk, tunnel_id) member pairs
		key := tunnelKey{p.DevicePK, p.TunnelID}
		mode, ok := tunnelMode[key]
		if !ok {
			continue
		}
		if mode == "P+S" {
			// Emit two records so the user appears in both publisher and subscriber views
			pubPoint := p
			pubPoint.Mode = "P"
			points = append(points, pubPoint)
			subPoint := p
			subPoint.Mode = "S"
			points = append(points, subPoint)
		} else {
			p.Mode = mode
			points = append(points, p)
		}
	}

	if err := trafficRows.Err(); err != nil {
		logError("multicast group traffic rows error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if points == nil {
		points = []MulticastTrafficPoint{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(points); err != nil {
		logError("failed to encode response", "error", err)
	}
}

type MulticastMemberCountPoint struct {
	Time            string `json:"time"`
	PublisherCount  int64  `json:"publisher_count"`
	SubscriberCount int64  `json:"subscriber_count"`
}

func (a *API) GetMulticastGroupMemberCounts(w http.ResponseWriter, r *http.Request) {
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
	err := a.envDB(ctx).QueryRow(ctx,
		`SELECT pk FROM dz_multicast_groups_current WHERE pk = ? OR code = ?`, pkOrCode, pkOrCode).Scan(&groupPK)
	if err != nil {
		logError("multicast group member counts group query error", "error", err)
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
			SELECT formatDateTime(ts, '%Y-%m-%dT%H:%i:%sZ') as time, publisher_count, subscriber_count
			FROM filtered WHERE rn_before = 1
			UNION ALL
			SELECT formatDateTime(ts, '%Y-%m-%dT%H:%i:%sZ') as time, publisher_count, subscriber_count
			FROM running
			WHERE ts > now() - INTERVAL ` + lookback + `
		) ORDER BY time
	`

	rows, err := a.envDB(ctx).Query(ctx, query, groupPK, groupPK, groupPK, groupPK)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("multicast group member counts query error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var points []MulticastMemberCountPoint
	for rows.Next() {
		var p MulticastMemberCountPoint
		if err := rows.Scan(&p.Time, &p.PublisherCount, &p.SubscriberCount); err != nil {
			logError("multicast group member counts scan error", "error", err)
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
		logError("failed to encode response", "error", err)
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
func (a *API) GetMulticastTreePaths(w http.ResponseWriter, r *http.Request) {
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
	err := a.envDB(ctx).QueryRow(ctx, groupQuery, pkOrCode, pkOrCode).Scan(&response.GroupPK, &response.GroupCode)
	if err != nil {
		logError("multicast tree paths group query error", "error", err)
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

	rows, err := a.envDB(ctx).Query(ctx, membersQuery, response.GroupPK, response.GroupPK, response.GroupPK, response.GroupPK, response.GroupPK)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("multicast tree paths members query error", "error", err)
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
			logError("multicast tree paths members scan error", "error", err)
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

	// Optional publisher filter: ?publishers=devicePK1,devicePK2
	if publishersParam := r.URL.Query().Get("publishers"); publishersParam != "" {
		allowedPKs := make(map[string]bool)
		for _, pk := range strings.Split(publishersParam, ",") {
			pk = strings.TrimSpace(pk)
			if pk != "" {
				allowedPKs[pk] = true
			}
		}
		if len(allowedPKs) > 0 {
			filtered := publishers[:0]
			for _, pub := range publishers {
				if allowedPKs[pub.PK] {
					filtered = append(filtered, pub)
				}
			}
			publishers = filtered
		}
	}

	if len(publishers) == 0 || len(subscribers) == 0 {
		response.Error = "no publishers or subscribers found with device assignments"
		writeJSON(w, response)
		return
	}

	// Load in-memory topology graph with committed latency for path finding
	g, err := a.loadTopologyGraph(ctx)
	if err != nil {
		response.Error = fmt.Sprintf("failed to load topology graph: %v", err)
		writeJSON(w, response)
		return
	}

	// Find paths from each publisher to each subscriber using in-memory Dijkstra
	type pathResult struct {
		path MulticastTreePath
		err  error
	}

	var wg sync.WaitGroup
	resultChan := make(chan pathResult, len(publishers)*len(subscribers))

	for _, pub := range publishers {
		for _, sub := range subscribers {
			if pub.PK == sub.PK {
				continue // Skip self-paths
			}
			wg.Add(1)
			go func(pubPK, pubCode, subPK, subCode string) {
				defer wg.Done()

				p := dijkstra(g, pubPK, subPK, nil, nil)
				if p == nil {
					return
				}

				var hops []MulticastTreeHop
				for i, nodePK := range p.Nodes {
					info := g.Nodes[nodePK]
					hop := MulticastTreeHop{
						DevicePK:   info.PK,
						DeviceCode: info.Code,
						DeviceType: info.DeviceType,
					}
					if i > 0 {
						hop.EdgeMetric = int(edgeMetric(g, p.Nodes[i-1], nodePK))
					}
					hops = append(hops, hop)
				}

				if len(hops) > 0 {
					resultChan <- pathResult{path: MulticastTreePath{
						PublisherDevicePK:    pubPK,
						PublisherDeviceCode:  pubCode,
						SubscriberDevicePK:   subPK,
						SubscriberDeviceCode: subCode,
						Path:                 hops,
						HopCount:             len(hops) - 1,
						TotalMetric:          int(p.TotalMetric),
					}}
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
			logError("multicast tree paths query error", "error", result.err)
			continue
		}
		response.Paths = append(response.Paths, result.path)
	}

	slog.Info("multicast tree paths completed", "paths", len(response.Paths), "duration", time.Since(start))
	writeJSON(w, response)
}

// MulticastAggSegment represents a unique link segment used by one or more publishers
type MulticastAggSegment struct {
	FromPK       string   `json:"fromPK"`
	ToPK         string   `json:"toPK"`
	PublisherPKs []string `json:"publisherPKs"`
}

// MulticastTreeSegmentsResponse is the response for the aggregated tree segments endpoint
type MulticastTreeSegmentsResponse struct {
	GroupCode       string                `json:"groupCode"`
	GroupPK         string                `json:"groupPK"`
	PublisherCount  int                   `json:"publisherCount"`
	SubscriberCount int                   `json:"subscriberCount"`
	Segments        []MulticastAggSegment `json:"segments"`
	Error           string                `json:"error,omitempty"`
}

// GetMulticastTreeSegments computes aggregated segments from publisher→subscriber paths.
// Instead of returning full hop-by-hop paths, it returns unique (fromPK, toPK) pairs
// with the set of publishers that traverse each segment. Uses batched Dijkstra queries
// (one per publisher) instead of one per (publisher, subscriber) pair.
func (a *API) GetMulticastTreeSegments(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		writeJSON(w, MulticastTreeSegmentsResponse{
			Segments: []MulticastAggSegment{},
			Error:    "missing multicast group pk",
		})
		return
	}

	start := time.Now()
	response := MulticastTreeSegmentsResponse{
		Segments: []MulticastAggSegment{},
	}

	// Resolve group info
	groupQuery := `
		SELECT pk, COALESCE(code, '') FROM dz_multicast_groups_current WHERE pk = ? OR code = ?
	`
	err := a.envDB(ctx).QueryRow(ctx, groupQuery, pkOrCode, pkOrCode).Scan(&response.GroupPK, &response.GroupCode)
	if err != nil {
		logError("multicast tree segments group query error", "error", err)
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
			COALESCE(u.device_pk, '') as device_pk
		FROM dz_users_current u
		WHERE u.status = 'activated'
			AND u.kind = 'multicast'
			AND (
				has(JSONExtract(u.publishers, 'Array(String)'), ?)
				OR has(JSONExtract(u.subscribers, 'Array(String)'), ?)
			)
	`

	rows, err := a.envDB(ctx).Query(ctx, membersQuery, response.GroupPK, response.GroupPK, response.GroupPK, response.GroupPK, response.GroupPK)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("multicast tree segments members query error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}
	defer rows.Close()

	publisherSet := make(map[string]bool)
	subscriberSet := make(map[string]bool)
	var publisherPKs, subscriberPKs []string

	for rows.Next() {
		var mode, devicePK string
		if err := rows.Scan(&mode, &devicePK); err != nil {
			logError("multicast tree segments members scan error", "error", err)
			continue
		}
		if devicePK == "" {
			continue
		}
		if (mode == "P" || mode == "P+S") && !publisherSet[devicePK] {
			publisherPKs = append(publisherPKs, devicePK)
			publisherSet[devicePK] = true
		}
		if (mode == "S" || mode == "P+S") && !subscriberSet[devicePK] {
			subscriberPKs = append(subscriberPKs, devicePK)
			subscriberSet[devicePK] = true
		}
	}

	response.PublisherCount = len(publisherPKs)
	response.SubscriberCount = len(subscriberPKs)

	// Optional publisher filter
	if publishersParam := r.URL.Query().Get("publishers"); publishersParam != "" {
		allowedPKs := make(map[string]bool)
		for _, pk := range strings.Split(publishersParam, ",") {
			pk = strings.TrimSpace(pk)
			if pk != "" {
				allowedPKs[pk] = true
			}
		}
		if len(allowedPKs) > 0 {
			filtered := publisherPKs[:0]
			for _, pk := range publisherPKs {
				if allowedPKs[pk] {
					filtered = append(filtered, pk)
				}
			}
			publisherPKs = filtered
		}
	}

	if len(publisherPKs) == 0 || len(subscriberPKs) == 0 {
		response.Error = "no publishers or subscribers found with device assignments"
		writeJSON(w, response)
		return
	}

	// Load in-memory topology graph with committed latency for path finding
	g, err := a.loadTopologyGraph(ctx)
	if err != nil {
		response.Error = fmt.Sprintf("failed to load topology graph: %v", err)
		writeJSON(w, response)
		return
	}

	// For each publisher, find paths to all subscribers using in-memory Dijkstra
	type publisherResult struct {
		publisherPK string
		// directed key (fromPK|toPK) -> true
		segments map[string]bool
		err      error
	}

	var wg sync.WaitGroup
	resultChan := make(chan publisherResult, len(publisherPKs))

	for _, pubPK := range publisherPKs {
		wg.Add(1)
		go func(pubPK string) {
			defer wg.Done()

			segments := make(map[string]bool)
			for _, subPK := range subscriberPKs {
				p := dijkstra(g, pubPK, subPK, nil, nil)
				if p == nil {
					continue
				}
				for i := 0; i < len(p.Nodes)-1; i++ {
					// Directed key: preserves path direction (publisher→subscriber).
					// A link traversed in both directions gets two separate segments.
					key := p.Nodes[i] + "|" + p.Nodes[i+1]
					segments[key] = true
				}
			}

			resultChan <- publisherResult{publisherPK: pubPK, segments: segments}
		}(pubPK)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Aggregate: directed segment key (fromPK|toPK) -> set of publisher PKs.
	// A link traversed in both directions produces two separate entries.
	segmentPublishers := make(map[string]map[string]bool)

	for result := range resultChan {
		if result.err != nil {
			logError("multicast tree segments path query error", "publisher", result.publisherPK, "error", result.err)
			continue
		}
		for key := range result.segments {
			if segmentPublishers[key] == nil {
				segmentPublishers[key] = make(map[string]bool)
			}
			segmentPublishers[key][result.publisherPK] = true
		}
	}

	// Build response segments
	for key, pubSet := range segmentPublishers {
		parts := strings.SplitN(key, "|", 2)
		pubs := make([]string, 0, len(pubSet))
		for pk := range pubSet {
			pubs = append(pubs, pk)
		}
		response.Segments = append(response.Segments, MulticastAggSegment{
			FromPK:       parts[0],
			ToPK:         parts[1],
			PublisherPKs: pubs,
		})
	}

	slog.Info("multicast tree segments completed", "segments", len(response.Segments), "publishers", len(publisherPKs), "duration", time.Since(start))
	writeJSON(w, response)
}
