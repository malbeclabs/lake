package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

type MetroListItem struct {
	PK                        string  `json:"pk"`
	Code                      string  `json:"code"`
	Name                      string  `json:"name"`
	Latitude                  float64 `json:"latitude"`
	Longitude                 float64 `json:"longitude"`
	DeviceCount               uint64  `json:"device_count"`
	UserCount                 uint64  `json:"user_count"`
	UnicastUsersCount         uint64  `json:"unicast_users_count"`
	MulticastSubscribersCount uint64  `json:"multicast_subscribers_count"`
	MulticastPublishersCount  uint64  `json:"multicast_publishers_count"`
	MaxUsers                  int64   `json:"max_users"`
	MaxUnicastUsers              uint64  `json:"max_unicast_users"`
	MaxMulticastSubscribers      uint64  `json:"max_multicast_subscribers"`
	MaxMulticastPublishers       uint64  `json:"max_multicast_publishers"`
	RawMaxUnicastUsers           uint64  `json:"raw_max_unicast_users"`
	RawMaxMulticastSubscribers   uint64  `json:"raw_max_multicast_subscribers"`
	RawMaxMulticastPublishers    uint64  `json:"raw_max_multicast_publishers"`
}

var metroSortFields = map[string]string{
	"code":      "code",
	"name":      "name",
	"latitude":  "latitude",
	"longitude": "longitude",
	"devices":   "device_count",
	"users":     "user_count",
}

var metroFilterFields = map[string]FilterFieldConfig{
	"code":    {Column: "code", Type: FieldTypeText},
	"name":    {Column: "name", Type: FieldTypeText},
	"devices": {Column: "device_count", Type: FieldTypeNumeric},
	"users":   {Column: "user_count", Type: FieldTypeNumeric},
}

func (a *API) GetMetros(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "code", metroSortFields)
	filters := ParseFilters(r)
	start := time.Now()

	filterClause, filterArgs := filters.BuildFilterClause(metroFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}
	orderBy := sort.OrderByClause(metroSortFields)

	query := `
		WITH device_counts AS (
			SELECT metro_pk, count(*) as device_count
			FROM dz_devices_current
			WHERE metro_pk IS NOT NULL
			GROUP BY metro_pk
		),
		user_counts AS (
			SELECT d.metro_pk, count(*) as user_count
			FROM dz_users_current u
			JOIN dz_devices_current d ON u.device_pk = d.pk
			WHERE u.status = 'activated' AND d.metro_pk IS NOT NULL
			GROUP BY d.metro_pk
		),
		-- NOTE: keep in sync with the identical CTE in GetMetro below
		onchain_user_counts AS (
			SELECT
				metro_pk,
				SUM(unicast_users_count) as unicast_users_count,
				SUM(multicast_subscribers_count) as multicast_subscribers_count,
				SUM(multicast_publishers_count) as multicast_publishers_count,
				SUM(max_users) as max_users,
				SUM(eff_max_unicast) as max_unicast_users,
				SUM(eff_max_subs) as max_multicast_subscribers,
				SUM(eff_max_pubs) as max_multicast_publishers,
				SUM(raw_max_unicast) as raw_max_unicast_users,
				SUM(raw_max_subs) as raw_max_multicast_subscribers,
				SUM(raw_max_pubs) as raw_max_multicast_publishers
			FROM (
				SELECT
					metro_pk,
					unicast_users_count,
					multicast_subscribers_count,
					multicast_publishers_count,
					max_users,
					toUInt64(max_unicast_users) as raw_max_unicast,
					toUInt64(max_multicast_subscribers) as raw_max_subs,
					toUInt64(max_multicast_publishers) as raw_max_pubs,
					if(max_unicast_users > 0, toUInt64(max_unicast_users), toUInt64(greatest(0, toInt64(max_users) - toInt64(max_multicast_subscribers) - toInt64(max_multicast_publishers)))) as eff_max_unicast,
					if(max_multicast_subscribers > 0, toUInt64(max_multicast_subscribers), toUInt64(greatest(0, toInt64(max_users) - toInt64(max_unicast_users) - toInt64(max_multicast_publishers)))) as eff_max_subs,
					if(max_multicast_publishers > 0, toUInt64(max_multicast_publishers), toUInt64(greatest(0, toInt64(max_users) - toInt64(max_unicast_users) - toInt64(max_multicast_subscribers)))) as eff_max_pubs
				FROM dz_devices_current
				WHERE metro_pk IS NOT NULL
			)
			GROUP BY metro_pk
		),
		metros_data AS (
			SELECT
				m.pk as pk,
				m.code as code,
				COALESCE(m.name, '') as name,
				COALESCE(m.latitude, 0) as latitude,
				COALESCE(m.longitude, 0) as longitude,
				COALESCE(dc.device_count, 0) as device_count,
				COALESCE(uc.user_count, 0) as user_count,
				COALESCE(ouc.unicast_users_count, 0) as unicast_users_count,
				COALESCE(ouc.multicast_subscribers_count, 0) as multicast_subscribers_count,
				COALESCE(ouc.multicast_publishers_count, 0) as multicast_publishers_count,
				COALESCE(ouc.max_users, 0) as max_users,
				COALESCE(ouc.max_unicast_users, 0) as max_unicast_users,
				COALESCE(ouc.max_multicast_subscribers, 0) as max_multicast_subscribers,
				COALESCE(ouc.max_multicast_publishers, 0) as max_multicast_publishers,
				COALESCE(ouc.raw_max_unicast_users, 0) as raw_max_unicast_users,
				COALESCE(ouc.raw_max_multicast_subscribers, 0) as raw_max_multicast_subscribers,
				COALESCE(ouc.raw_max_multicast_publishers, 0) as raw_max_multicast_publishers
			FROM dz_metros_current m
			LEFT JOIN device_counts dc ON m.pk = dc.metro_pk
			LEFT JOIN user_counts uc ON m.pk = uc.metro_pk
			LEFT JOIN onchain_user_counts ouc ON m.pk = ouc.metro_pk
		)
		SELECT pk, code, name, latitude, longitude, device_count, user_count, unicast_users_count, multicast_subscribers_count, multicast_publishers_count, max_users, max_unicast_users, max_multicast_subscribers, max_multicast_publishers, raw_max_unicast_users, raw_max_multicast_subscribers, raw_max_multicast_publishers, count() OVER () as _total
		FROM metros_data
		WHERE 1=1` + whereFilter + " " + orderBy + `
		LIMIT ? OFFSET ?
	`

	var args []any
	args = append(args, filterArgs...)
	args = append(args, pagination.Limit, pagination.Offset)

	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("metros query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var metros []MetroListItem
	var total uint64
	for rows.Next() {
		var m MetroListItem
		if err := rows.Scan(
			&m.PK,
			&m.Code,
			&m.Name,
			&m.Latitude,
			&m.Longitude,
			&m.DeviceCount,
			&m.UserCount,
			&m.UnicastUsersCount,
			&m.MulticastSubscribersCount,
			&m.MulticastPublishersCount,
			&m.MaxUsers,
			&m.MaxUnicastUsers,
			&m.MaxMulticastSubscribers,
			&m.MaxMulticastPublishers,
			&m.RawMaxUnicastUsers,
			&m.RawMaxMulticastSubscribers,
			&m.RawMaxMulticastPublishers,
			&total,
		); err != nil {
			logError("metros row scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		metros = append(metros, m)
	}

	if err := rows.Err(); err != nil {
		logError("metros rows iteration failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null
	if metros == nil {
		metros = []MetroListItem{}
	}

	response := PaginatedResponse[MetroListItem]{
		Items:  metros,
		Total:  int(total),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}

type MetroDetail struct {
	PK                        string  `json:"pk"`
	Code                      string  `json:"code"`
	Name                      string  `json:"name"`
	Latitude                  float64 `json:"latitude"`
	Longitude                 float64 `json:"longitude"`
	DeviceCount               uint64  `json:"device_count"`
	UserCount                 uint64  `json:"user_count"`
	UnicastUsersCount         uint64  `json:"unicast_users_count"`
	MulticastSubscribersCount uint64  `json:"multicast_subscribers_count"`
	MulticastPublishersCount  uint64  `json:"multicast_publishers_count"`
	MaxUsers                  int64   `json:"max_users"`
	MaxUnicastUsers              uint64  `json:"max_unicast_users"`
	MaxMulticastSubscribers      uint64  `json:"max_multicast_subscribers"`
	MaxMulticastPublishers       uint64  `json:"max_multicast_publishers"`
	RawMaxUnicastUsers           uint64  `json:"raw_max_unicast_users"`
	RawMaxMulticastSubscribers   uint64  `json:"raw_max_multicast_subscribers"`
	RawMaxMulticastPublishers    uint64  `json:"raw_max_multicast_publishers"`
	ValidatorCount               uint64  `json:"validator_count"`
	StakeSol                  float64 `json:"stake_sol"`
	InBps                     float64 `json:"in_bps"`
	OutBps                    float64 `json:"out_bps"`
}

func (a *API) GetMetro(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing metro pk", http.StatusBadRequest)
		return
	}

	start := time.Now()
	query := `
		WITH device_counts AS (
			SELECT metro_pk, count(*) as device_count
			FROM dz_devices_current
			WHERE metro_pk = ?
			GROUP BY metro_pk
		),
		user_counts AS (
			SELECT d.metro_pk, count(*) as user_count
			FROM dz_users_current u
			JOIN dz_devices_current d ON u.device_pk = d.pk
			WHERE u.status = 'activated' AND d.metro_pk = ?
			GROUP BY d.metro_pk
		),
		-- NOTE: keep in sync with the identical CTE in GetMetros above
		onchain_user_counts AS (
			SELECT
				metro_pk,
				SUM(unicast_users_count) as unicast_users_count,
				SUM(multicast_subscribers_count) as multicast_subscribers_count,
				SUM(multicast_publishers_count) as multicast_publishers_count,
				SUM(max_users) as max_users,
				SUM(eff_max_unicast) as max_unicast_users,
				SUM(eff_max_subs) as max_multicast_subscribers,
				SUM(eff_max_pubs) as max_multicast_publishers,
				SUM(raw_max_unicast) as raw_max_unicast_users,
				SUM(raw_max_subs) as raw_max_multicast_subscribers,
				SUM(raw_max_pubs) as raw_max_multicast_publishers
			FROM (
				SELECT
					metro_pk,
					unicast_users_count,
					multicast_subscribers_count,
					multicast_publishers_count,
					max_users,
					toUInt64(max_unicast_users) as raw_max_unicast,
					toUInt64(max_multicast_subscribers) as raw_max_subs,
					toUInt64(max_multicast_publishers) as raw_max_pubs,
					if(max_unicast_users > 0, toUInt64(max_unicast_users), toUInt64(greatest(0, toInt64(max_users) - toInt64(max_multicast_subscribers) - toInt64(max_multicast_publishers)))) as eff_max_unicast,
					if(max_multicast_subscribers > 0, toUInt64(max_multicast_subscribers), toUInt64(greatest(0, toInt64(max_users) - toInt64(max_unicast_users) - toInt64(max_multicast_publishers)))) as eff_max_subs,
					if(max_multicast_publishers > 0, toUInt64(max_multicast_publishers), toUInt64(greatest(0, toInt64(max_users) - toInt64(max_unicast_users) - toInt64(max_multicast_subscribers)))) as eff_max_pubs
				FROM dz_devices_current
				WHERE metro_pk IS NOT NULL
			)
			GROUP BY metro_pk
		),
		validator_stats AS (
			SELECT
				d.metro_pk,
				count(DISTINCT v.vote_pubkey) as validator_count,
				sum(v.activated_stake_lamports) / 1e9 as stake_sol
			FROM dz_users_current u
			JOIN dz_devices_current d ON u.device_pk = d.pk
			JOIN solana_gossip_nodes_current g ON u.client_ip = g.gossip_ip
			JOIN solana_vote_accounts_current v ON g.pubkey = v.node_pubkey
			WHERE u.status = 'activated' AND u.client_ip != '' AND v.epoch_vote_account = 'true'
				AND d.metro_pk = ?
			GROUP BY d.metro_pk
		),
		traffic_rates AS (
			SELECT
				d.metro_pk,
				SUM(f.avg_in_bps) as in_bps,
				SUM(f.avg_out_bps) as out_bps
			FROM device_interface_rollup_5m f
			JOIN dz_devices_current d ON f.device_pk = d.pk
			WHERE f.bucket_ts >= now() - INTERVAL 15 MINUTE
				AND f.user_tunnel_id IS NULL
				AND f.link_pk = ''
				AND d.metro_pk = ?
			GROUP BY d.metro_pk
		)
		SELECT
			m.pk,
			m.code,
			COALESCE(m.name, '') as name,
			COALESCE(m.latitude, 0) as latitude,
			COALESCE(m.longitude, 0) as longitude,
			COALESCE(dc.device_count, 0) as device_count,
			COALESCE(uc.user_count, 0) as user_count,
			COALESCE(ouc.unicast_users_count, 0) as unicast_users_count,
			COALESCE(ouc.multicast_subscribers_count, 0) as multicast_subscribers_count,
			COALESCE(ouc.multicast_publishers_count, 0) as multicast_publishers_count,
			COALESCE(ouc.max_users, 0) as max_users,
			COALESCE(ouc.max_unicast_users, 0) as max_unicast_users,
			COALESCE(ouc.max_multicast_subscribers, 0) as max_multicast_subscribers,
			COALESCE(ouc.max_multicast_publishers, 0) as max_multicast_publishers,
			COALESCE(ouc.raw_max_unicast_users, 0) as raw_max_unicast_users,
			COALESCE(ouc.raw_max_multicast_subscribers, 0) as raw_max_multicast_subscribers,
			COALESCE(ouc.raw_max_multicast_publishers, 0) as raw_max_multicast_publishers,
			COALESCE(vs.validator_count, 0) as validator_count,
			COALESCE(vs.stake_sol, 0) as stake_sol,
			COALESCE(tr.in_bps, 0) as in_bps,
			COALESCE(tr.out_bps, 0) as out_bps
		FROM dz_metros_current m
		LEFT JOIN device_counts dc ON m.pk = dc.metro_pk
		LEFT JOIN user_counts uc ON m.pk = uc.metro_pk
		LEFT JOIN onchain_user_counts ouc ON m.pk = ouc.metro_pk
		LEFT JOIN validator_stats vs ON m.pk = vs.metro_pk
		LEFT JOIN traffic_rates tr ON m.pk = tr.metro_pk
		WHERE m.pk = ?
	`

	var metro MetroDetail
	err := a.envDB(ctx).QueryRow(ctx, query, pk, pk, pk, pk, pk).Scan(
		&metro.PK,
		&metro.Code,
		&metro.Name,
		&metro.Latitude,
		&metro.Longitude,
		&metro.DeviceCount,
		&metro.UserCount,
		&metro.UnicastUsersCount,
		&metro.MulticastSubscribersCount,
		&metro.MulticastPublishersCount,
		&metro.MaxUsers,
		&metro.MaxUnicastUsers,
		&metro.MaxMulticastSubscribers,
		&metro.MaxMulticastPublishers,
		&metro.RawMaxUnicastUsers,
		&metro.RawMaxMulticastSubscribers,
		&metro.RawMaxMulticastPublishers,
		&metro.ValidatorCount,
		&metro.StakeSol,
		&metro.InBps,
		&metro.OutBps,
	)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "metro not found", http.StatusNotFound)
			return
		}
		logError("metro query failed", "error", err, "pk", pk)
		http.Error(w, "metro not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(metro); err != nil {
		logError("failed to encode response", "error", err)
	}
}
