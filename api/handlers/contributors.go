package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/metrics"
)

type ContributorListItem struct {
	PK           string `json:"pk"`
	Code         string `json:"code"`
	Name         string `json:"name"`
	DeviceCount  uint64 `json:"device_count"`
	SideADevices uint64 `json:"side_a_devices"`
	SideZDevices uint64 `json:"side_z_devices"`
	LinkCount    uint64 `json:"link_count"`
	UserCount    uint64 `json:"user_count"`
	MaxUsers     int64  `json:"max_users"`
}

var contributorSortFields = map[string]string{
	"code":    "code",
	"name":    "name",
	"devices": "device_count",
	"sidea":   "side_a_devices",
	"sidez":   "side_z_devices",
	"links":   "link_count",
	"users":   "users_no_data|users_util_frac;max_users DESC",
}

var contributorFilterFields = map[string]FilterFieldConfig{
	"code":    {Column: "code", Type: FieldTypeText},
	"name":    {Column: "name", Type: FieldTypeText},
	"devices": {Column: "device_count", Type: FieldTypeNumeric},
	"sidea":   {Column: "side_a_devices", Type: FieldTypeNumeric},
	"sidez":   {Column: "side_z_devices", Type: FieldTypeNumeric},
	"links":   {Column: "link_count", Type: FieldTypeNumeric},
}

func (a *API) GetContributors(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "code", contributorSortFields)
	filters := ParseFilters(r)
	start := time.Now()

	filterClause, filterArgs := filters.BuildFilterClause(contributorFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}
	orderBy := sort.OrderByClause(contributorSortFields)

	query := `
		WITH device_counts AS (
			SELECT contributor_pk, count(*) as cnt
			FROM dz_devices_current
			WHERE contributor_pk IS NOT NULL
			GROUP BY contributor_pk
		),
		side_a_counts AS (
			SELECT d.contributor_pk as cpk, count(DISTINCT l.pk) as cnt
			FROM dz_links_current l
			JOIN dz_devices_current d ON l.side_a_pk = d.pk
			WHERE d.contributor_pk IS NOT NULL
			GROUP BY d.contributor_pk
		),
		side_z_counts AS (
			SELECT d.contributor_pk as cpk, count(DISTINCT l.pk) as cnt
			FROM dz_links_current l
			JOIN dz_devices_current d ON l.side_z_pk = d.pk
			WHERE d.contributor_pk IS NOT NULL
			GROUP BY d.contributor_pk
		),
		link_counts AS (
			SELECT contributor_pk, count(*) as cnt
			FROM dz_links_current
			WHERE contributor_pk IS NOT NULL
			GROUP BY contributor_pk
		),
		user_counts AS (
			SELECT d.contributor_pk, count(*) as cnt
			FROM dz_users_current u
			JOIN dz_devices_current d ON u.device_pk = d.pk
			WHERE u.status = 'activated' AND d.contributor_pk IS NOT NULL
			GROUP BY d.contributor_pk
		),
		max_users_agg AS (
			SELECT contributor_pk, SUM(max_users) as total_max_users
			FROM dz_devices_current
			WHERE contributor_pk IS NOT NULL
			GROUP BY contributor_pk
		),
		contributors_data AS (
			SELECT
				c.pk as pk,
				c.code as code,
				COALESCE(c.name, '') as name,
				COALESCE(dc.cnt, 0) as device_count,
				COALESCE(sa.cnt, 0) as side_a_devices,
				COALESCE(sz.cnt, 0) as side_z_devices,
				COALESCE(lc.cnt, 0) as link_count,
				COALESCE(uc.cnt, 0) as user_count,
				COALESCE(mu.total_max_users, 0) as max_users
			FROM dz_contributors_current c
			LEFT JOIN device_counts dc ON c.pk = dc.contributor_pk
			LEFT JOIN side_a_counts sa ON c.pk = sa.cpk
			LEFT JOIN side_z_counts sz ON c.pk = sz.cpk
			LEFT JOIN link_counts lc ON c.pk = lc.contributor_pk
			LEFT JOIN user_counts uc ON c.pk = uc.contributor_pk
			LEFT JOIN max_users_agg mu ON c.pk = mu.contributor_pk
		),
		contributors_util AS (
			SELECT *,
				toUInt8(max_users = 0) as users_no_data,
				if(max_users > 0, toFloat64(user_count) / toFloat64(max_users), 0.0) as users_util_frac
			FROM contributors_data
		)
		SELECT pk, code, name, device_count, side_a_devices, side_z_devices, link_count, user_count, max_users, count() OVER () as _total
		FROM contributors_util
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
		logError("contributors query failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var contributors []ContributorListItem
	var total uint64
	for rows.Next() {
		var c ContributorListItem
		if err := rows.Scan(
			&c.PK,
			&c.Code,
			&c.Name,
			&c.DeviceCount,
			&c.SideADevices,
			&c.SideZDevices,
			&c.LinkCount,
			&c.UserCount,
			&c.MaxUsers,
			&total,
		); err != nil {
			logError("contributors row scan failed", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		contributors = append(contributors, c)
	}

	if err := rows.Err(); err != nil {
		logError("contributors rows iteration failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null
	if contributors == nil {
		contributors = []ContributorListItem{}
	}

	response := PaginatedResponse[ContributorListItem]{
		Items:  contributors,
		Total:  int(total),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}

type ContributorDetail struct {
	PK                         string  `json:"pk"`
	Code                       string  `json:"code"`
	Name                       string  `json:"name"`
	DeviceCount                uint64  `json:"device_count"`
	SideADevices               uint64  `json:"side_a_devices"`
	SideZDevices               uint64  `json:"side_z_devices"`
	LinkCount                  uint64  `json:"link_count"`
	UserCount                  uint64  `json:"user_count"`
	UnicastUsersCount          uint64  `json:"unicast_users_count"`
	MulticastSubscribersCount  uint64  `json:"multicast_subscribers_count"`
	MulticastPublishersCount   uint64  `json:"multicast_publishers_count"`
	MaxUsers                   int64   `json:"max_users"`
	MaxUnicastUsers            uint64  `json:"max_unicast_users"`
	MaxMulticastSubscribers    uint64  `json:"max_multicast_subscribers"`
	MaxMulticastPublishers     uint64  `json:"max_multicast_publishers"`
	RawMaxUnicastUsers         uint64  `json:"raw_max_unicast_users"`
	RawMaxMulticastSubscribers uint64  `json:"raw_max_multicast_subscribers"`
	RawMaxMulticastPublishers  uint64  `json:"raw_max_multicast_publishers"`
	InBps                      float64 `json:"in_bps"`
	OutBps                     float64 `json:"out_bps"`
}

func (a *API) GetContributor(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing contributor pk", http.StatusBadRequest)
		return
	}

	start := time.Now()
	query := `
		WITH device_counts AS (
			SELECT contributor_pk, count(*) as cnt
			FROM dz_devices_current
			WHERE contributor_pk = ?
			GROUP BY contributor_pk
		),
		side_a_counts AS (
			SELECT d.contributor_pk as cpk, count(DISTINCT l.pk) as cnt
			FROM dz_links_current l
			JOIN dz_devices_current d ON l.side_a_pk = d.pk
			WHERE d.contributor_pk = ?
			GROUP BY d.contributor_pk
		),
		side_z_counts AS (
			SELECT d.contributor_pk as cpk, count(DISTINCT l.pk) as cnt
			FROM dz_links_current l
			JOIN dz_devices_current d ON l.side_z_pk = d.pk
			WHERE d.contributor_pk = ?
			GROUP BY d.contributor_pk
		),
		link_counts AS (
			SELECT contributor_pk, count(*) as cnt
			FROM dz_links_current
			WHERE contributor_pk = ?
			GROUP BY contributor_pk
		),
		user_counts AS (
			SELECT d.contributor_pk, count(*) as cnt
			FROM dz_users_current u
			JOIN dz_devices_current d ON u.device_pk = d.pk
			WHERE u.status = 'activated' AND d.contributor_pk = ?
			GROUP BY d.contributor_pk
		),
		onchain_user_counts AS (
			SELECT
				contributor_pk,
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
					contributor_pk,
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
				WHERE contributor_pk IS NOT NULL
			)
			GROUP BY contributor_pk
		),
		traffic_rates AS (
			SELECT
				d.contributor_pk,
				SUM(f.avg_in_bps) as in_bps,
				SUM(f.avg_out_bps) as out_bps
			FROM device_interface_rollup_5m f
			JOIN dz_devices_current d ON f.device_pk = d.pk
			WHERE f.bucket_ts >= now() - INTERVAL 15 MINUTE
				AND f.user_tunnel_id IS NULL
				AND f.link_pk = ''
				AND d.contributor_pk = ?
			GROUP BY d.contributor_pk
		)
		SELECT
			c.pk,
			c.code,
			COALESCE(c.name, '') as name,
			COALESCE(dc.cnt, 0) as device_count,
			COALESCE(sa.cnt, 0) as side_a_devices,
			COALESCE(sz.cnt, 0) as side_z_devices,
			COALESCE(lc.cnt, 0) as link_count,
			COALESCE(uc.cnt, 0) as user_count,
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
			COALESCE(tr.in_bps, 0) as in_bps,
			COALESCE(tr.out_bps, 0) as out_bps
		FROM dz_contributors_current c
		LEFT JOIN device_counts dc ON c.pk = dc.contributor_pk
		LEFT JOIN side_a_counts sa ON c.pk = sa.cpk
		LEFT JOIN side_z_counts sz ON c.pk = sz.cpk
		LEFT JOIN link_counts lc ON c.pk = lc.contributor_pk
		LEFT JOIN user_counts uc ON c.pk = uc.contributor_pk
		LEFT JOIN onchain_user_counts ouc ON c.pk = ouc.contributor_pk
		LEFT JOIN traffic_rates tr ON c.pk = tr.contributor_pk
		WHERE c.pk = ?
	`

	var contributor ContributorDetail
	err := a.envDB(ctx).QueryRow(ctx, query, pk, pk, pk, pk, pk, pk, pk).Scan(
		&contributor.PK,
		&contributor.Code,
		&contributor.Name,
		&contributor.DeviceCount,
		&contributor.SideADevices,
		&contributor.SideZDevices,
		&contributor.LinkCount,
		&contributor.UserCount,
		&contributor.UnicastUsersCount,
		&contributor.MulticastSubscribersCount,
		&contributor.MulticastPublishersCount,
		&contributor.MaxUsers,
		&contributor.MaxUnicastUsers,
		&contributor.MaxMulticastSubscribers,
		&contributor.MaxMulticastPublishers,
		&contributor.RawMaxUnicastUsers,
		&contributor.RawMaxMulticastSubscribers,
		&contributor.RawMaxMulticastPublishers,
		&contributor.InBps,
		&contributor.OutBps,
	)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "contributor not found", http.StatusNotFound)
			return
		}
		logError("contributor query failed", "error", err, "pk", pk)
		http.Error(w, "contributor not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(contributor); err != nil {
		logError("failed to encode response", "error", err)
	}
}
