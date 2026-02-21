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
	IsLeader       bool    `json:"is_leader"`         // true if currently the Solana leader
	NodePubkey     string  `json:"node_pubkey"`       // validator's node identity pubkey
	VotePubkey     string  `json:"vote_pubkey"`       // validator's vote account pubkey
	StakeSol       float64 `json:"stake_sol"`         // activated stake in SOL
	LastLeaderSlot *int64  `json:"last_leader_slot"`  // most recent past leader slot
	NextLeaderSlot *int64  `json:"next_leader_slot"`  // next upcoming leader slot
	CurrentSlot    int64   `json:"current_slot"`      // current cluster slot
}

type MulticastGroupDetail struct {
	PK              string            `json:"pk"`
	Code            string            `json:"code"`
	MulticastIP     string            `json:"multicast_ip"`
	MaxBandwidth    uint64            `json:"max_bandwidth"`
	Status          string            `json:"status"`
	PublisherCount  uint32            `json:"publisher_count"`
	SubscriberCount uint32            `json:"subscriber_count"`
	Members         []MulticastMember `json:"members"`
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

	// First get the group details (accept pk or code)
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
	if err != nil {
		log.Printf("MulticastGroup query error: %v", err)
		http.Error(w, "multicast group not found", http.StatusNotFound)
		return
	}

	// Now get all members (users who publish or subscribe to this group)
	// Users have publishers and subscribers columns as JSON arrays of group PKs
	membersQuery := `
		SELECT
			u.pk as user_pk,
			CASE
				WHEN has(JSONExtract(u.publishers, 'Array(String)'), ?) AND has(JSONExtract(u.subscribers, 'Array(String)'), ?) THEN 'P+S'
				WHEN has(JSONExtract(u.publishers, 'Array(String)'), ?) THEN 'P'
				ELSE 'S'
			END as mode,
			COALESCE(u.device_pk, '') as device_pk,
			COALESCE(d.code, '') as device_code,
			COALESCE(d.metro_pk, '') as metro_pk,
			COALESCE(m.code, '') as metro_code,
			COALESCE(m.name, '') as metro_name,
			COALESCE(u.client_ip, '') as client_ip,
			COALESCE(u.dz_ip, '') as dz_ip,
			u.status,
			COALESCE(u.owner_pubkey, '') as owner_pubkey,
			COALESCE(u.tunnel_id, 0) as tunnel_id
		FROM dz_users_current u
		LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		WHERE u.status = 'activated'
			AND u.kind = 'multicast'
			AND (
				has(JSONExtract(u.publishers, 'Array(String)'), ?)
				OR has(JSONExtract(u.subscribers, 'Array(String)'), ?)
			)
		ORDER BY mode DESC, d.code
	`

	rows, err := envDB(ctx).Query(ctx, membersQuery, group.PK, group.PK, group.PK, group.PK, group.PK)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("MulticastGroup members query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var members []MulticastMember
	for rows.Next() {
		var m MulticastMember
		if err := rows.Scan(
			&m.UserPK,
			&m.Mode,
			&m.DevicePK,
			&m.DeviceCode,
			&m.MetroPK,
			&m.MetroCode,
			&m.MetroName,
			&m.ClientIP,
			&m.DZIP,
			&m.Status,
			&m.OwnerPubkey,
			&m.TunnelID,
		); err != nil {
			log.Printf("MulticastGroup members scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		members = append(members, m)
	}

	if err := rows.Err(); err != nil {
		log.Printf("MulticastGroup members rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null
	if members == nil {
		members = []MulticastMember{}
	}

	// Enrich members with traffic rates from interface counters
	if len(members) > 0 {
		// Build a map of (device_pk, tunnel_id) -> member index for matching
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

		// Get latest traffic rate per user tunnel from the last 5 minutes
		// Return in/out separately so we can show the relevant direction per member mode:
		// Publishers send into the device (in_octets), subscribers receive from the device (out_octets)
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
			log.Printf("MulticastGroup traffic query error (non-fatal): %v", err)
		} else {
			defer trafficRows.Close()
			for trafficRows.Next() {
				var devicePK string
				var tunnelID int64
				var inBps, outBps, inPps, outPps float64
				if err := trafficRows.Scan(&devicePK, &tunnelID, &inBps, &outBps, &inPps, &outPps); err != nil {
					log.Printf("MulticastGroup traffic scan error: %v", err)
					continue
				}
				key := tunnelKey{devicePK, tunnelID}
				if indices, ok := tunnelToMembers[key]; ok {
					for _, idx := range indices {
						// Publishers: traffic into the device (in = validator sending shreds)
						// Subscribers: traffic out of the device (out = device forwarding to subscriber)
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

	// Enrich publishers with validator identity and leader schedule data
	if len(members) > 0 {
		// Collect client IPs of publishers (gossip_ip matches client_ip, not dz_ip)
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

			// First, resolve node_pubkey + vote/stake for all publishers via gossip + vote accounts
			// This works even for validators without leader slots in the current epoch
			gossipQuery := `
				SELECT
					g.gossip_ip,
					g.pubkey,
					COALESCE(v.vote_pubkey, '') as vote_pubkey,
					COALESCE(v.activated_stake_lamports, 0) / 1e9 as stake_sol
				FROM solana_gossip_nodes_current g
				LEFT JOIN solana_vote_accounts_current v ON g.pubkey = v.node_pubkey AND v.epoch_vote_account = 'true'
				WHERE g.gossip_ip IN (?)
			`
			gossipRows, err := envDB(ctx).Query(ctx, gossipQuery, clientIPs)
			if err != nil {
				log.Printf("MulticastGroup gossip query error (non-fatal): %v", err)
			} else {
				defer gossipRows.Close()
				for gossipRows.Next() {
					var gossipIP, pubkey, votePubkey string
					var stakeSol float64
					if err := gossipRows.Scan(&gossipIP, &pubkey, &votePubkey, &stakeSol); err != nil {
						log.Printf("MulticastGroup gossip scan error: %v", err)
						continue
					}
					if indices, ok := clientIPToMembers[gossipIP]; ok {
						for _, idx := range indices {
							members[idx].NodePubkey = pubkey
							members[idx].VotePubkey = votePubkey
							members[idx].StakeSol = stakeSol
						}
					}
				}
			}

			// Then layer on leader schedule timing for those with leader slots
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
					ls.node_pubkey,
					ei.abs_slot as current_slot,
					has(JSONExtract(ls.slots, 'Array(UInt64)'), ei.slot_in_epoch) as is_leader,
					if(empty(arrayFilter(x -> x <= ei.slot_in_epoch, JSONExtract(ls.slots, 'Array(UInt64)'))), 0,
						ei.epoch_start + arrayMax(arrayFilter(x -> x <= ei.slot_in_epoch, JSONExtract(ls.slots, 'Array(UInt64)')))) as last_leader_slot,
					if(empty(arrayFilter(x -> x > ei.slot_in_epoch, JSONExtract(ls.slots, 'Array(UInt64)'))), 0,
						ei.epoch_start + arrayMin(arrayFilter(x -> x > ei.slot_in_epoch, JSONExtract(ls.slots, 'Array(UInt64)')))) as next_leader_slot
				FROM solana_leader_schedule_current ls
				JOIN solana_gossip_nodes_current g ON g.pubkey = ls.node_pubkey
				CROSS JOIN epoch_info ei
				WHERE g.gossip_ip IN (?)
			`

			leaderRows, err := envDB(ctx).Query(ctx, leaderQuery, clientIPs)
			if err != nil {
				log.Printf("MulticastGroup leader query error (non-fatal): %v", err)
			} else {
				defer leaderRows.Close()
				for leaderRows.Next() {
					var clientIP, nodePubkey string
					var currentSlot uint64
					var isLeader uint8
					var lastSlot, nextSlot uint64
					if err := leaderRows.Scan(&clientIP, &nodePubkey, &currentSlot, &isLeader, &lastSlot, &nextSlot); err != nil {
						log.Printf("MulticastGroup leader scan error: %v", err)
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
							if nextSlot > 0 {
								s := int64(nextSlot)
								members[idx].NextLeaderSlot = &s
							}
						}
					}
				}
			}
		}
	}

	group.Members = members

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(group); err != nil {
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

	// Query traffic time series — filter to member edge devices only
	// to avoid including transit device traffic for the same tunnel IDs
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
		// Look up mode from member info
		key := tunnelKey{p.DevicePK, p.TunnelID}
		if mode, ok := tunnelMode[key]; ok {
			p.Mode = mode
		}
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
