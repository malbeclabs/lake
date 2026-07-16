package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

type FacilityListItem struct {
	PK                        string  `json:"pk"`
	Code                      string  `json:"code"`
	Name                      string  `json:"name"`
	Country                   string  `json:"country"`
	Lat                       float64 `json:"lat"`
	Lng                       float64 `json:"lng"`
	LocId                     uint32  `json:"loc_id"`
	MetroPK                   string  `json:"metro_pk"`
	MetroCode                 string  `json:"metro_code"`
	DeviceCount               uint32  `json:"device_count"`
	UserCount                 uint32  `json:"user_count"`
	MaxUsers                  uint64  `json:"max_users"`
	UnicastUsersCount         uint64  `json:"unicast_users_count"`
	MulticastSubscribersCount uint64  `json:"multicast_subscribers_count"`
	MulticastPublishersCount  uint64  `json:"multicast_publishers_count"`
	MaxUnicastUsers           uint64  `json:"max_unicast_users"`
	MaxMulticastSubscribers   uint64  `json:"max_multicast_subscribers"`
	MaxMulticastPublishers    uint64  `json:"max_multicast_publishers"`
}

type FacilityDetail struct {
	PK                        string  `json:"pk"`
	Code                      string  `json:"code"`
	Name                      string  `json:"name"`
	Country                   string  `json:"country"`
	Lat                       float64 `json:"lat"`
	Lng                       float64 `json:"lng"`
	LocId                     uint32  `json:"loc_id"`
	Status                    string  `json:"status"`
	MetroPK                   string  `json:"metro_pk"`
	MetroCode                 string  `json:"metro_code"`
	DeviceCount               uint32  `json:"device_count"`
	UserCount                 uint32  `json:"user_count"`
	MaxUsers                  uint64  `json:"max_users"`
	UnicastUsersCount         uint64  `json:"unicast_users_count"`
	MulticastSubscribersCount uint64  `json:"multicast_subscribers_count"`
	MulticastPublishersCount  uint64  `json:"multicast_publishers_count"`
	MaxUnicastUsers           uint64  `json:"max_unicast_users"`
	MaxMulticastSubscribers   uint64  `json:"max_multicast_subscribers"`
	MaxMulticastPublishers    uint64  `json:"max_multicast_publishers"`
}

var facilitySortFields = map[string]string{
	"code":    "code",
	"name":    "name",
	"country": "country",
	"loc_id":  "loc_id",
	"metro":   "metro_code",
	"devices": "device_count",
	"users":   "if(user_count = 0 AND max_users = 0 AND max_unicast_users = 0 AND max_multicast_subscribers = 0 AND max_multicast_publishers = 0, 1, 0)|toFloat64(user_count) / toFloat64(greatest(1, user_count + least(if(max_users > 0, greatest(0, toInt64(max_users) - toInt64(user_count)), toInt64(999999999)), if(max_unicast_users > 0, greatest(0, toInt64(max_unicast_users) - toInt64(unicast_users_count)), toInt64(999999999)), if(max_multicast_subscribers > 0, greatest(0, toInt64(max_multicast_subscribers) - toInt64(multicast_subscribers_count)), toInt64(999999999)), if(max_multicast_publishers > 0, greatest(0, toInt64(max_multicast_publishers) - toInt64(multicast_publishers_count)), toInt64(999999999)))))",
}

var facilityFilterFields = map[string]FilterFieldConfig{
	"code":     {Column: "code", Type: FieldTypeText},
	"name":     {Column: "name", Type: FieldTypeText},
	"country":  {Column: "country", Type: FieldTypeText},
	"status":   {Column: "status", Type: FieldTypeText},
	"loc_id":   {Column: "loc_id", Type: FieldTypeNumeric},
	"metro":    {Column: "metro_code", Type: FieldTypeText},
	"metro_pk": {Column: "metro_pk", Type: FieldTypeText},
	"devices":  {Column: "device_count", Type: FieldTypeNumeric},
	"users":    {Column: "user_count", Type: FieldTypeNumeric},
}

// facilitiesEnrichedCTE joins facilities with devices (via location_pk), metros, and users.
// Requires migration 20260421000002 to add location_pk to dz_devices_current.
const facilitiesEnrichedCTE = `
	WITH facility_device_stats AS (
		SELECT
			location_pk,
			toUInt32(countDistinct(pk)) AS device_count,
			SUM(unicast_users_count)         AS unicast_users_count,
			SUM(multicast_subscribers_count) AS multicast_subscribers_count,
			SUM(multicast_publishers_count)  AS multicast_publishers_count,
			SUM(max_users)                   AS max_users,
			SUM(max_unicast_users)           AS max_unicast_users,
			SUM(max_multicast_subscribers)   AS max_multicast_subscribers,
		SUM(max_multicast_publishers)    AS max_multicast_publishers
		FROM dz_devices_current
		WHERE location_pk != ''
		GROUP BY location_pk
	),
	facility_user_counts AS (
		SELECT d.location_pk, toUInt32(countDistinct(u.pk)) AS user_count
		FROM dz_users_current u
		JOIN dz_devices_current d ON u.device_pk = d.pk
		WHERE u.status = 'activated' AND d.location_pk != ''
		GROUP BY d.location_pk
	),
	facility_metro AS (
		SELECT d.location_pk, any(d.metro_pk) AS metro_pk, any(m.code) AS metro_code
		FROM dz_devices_current d
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		WHERE d.location_pk != ''
		GROUP BY d.location_pk
	),
	enriched AS (
		SELECT
			l.pk AS pk,
			l.code AS code,
			COALESCE(l.name, '')    AS name,
			COALESCE(l.country, '') AS country,
			COALESCE(l.lat, 0)      AS lat,
			COALESCE(l.lng, 0)      AS lng,
			COALESCE(l.loc_id, 0)   AS loc_id,
			COALESCE(l.status, '')  AS status,
			COALESCE(fm.metro_pk, '')   AS metro_pk,
			COALESCE(fm.metro_code, '') AS metro_code,
			toUInt32(COALESCE(fds.device_count, 0))               AS device_count,
			toUInt32(COALESCE(fuc.user_count, 0))                 AS user_count,
			toUInt64(COALESCE(fds.max_users, 0))                  AS max_users,
			toUInt64(COALESCE(fds.unicast_users_count, 0))        AS unicast_users_count,
			toUInt64(COALESCE(fds.multicast_subscribers_count, 0)) AS multicast_subscribers_count,
			toUInt64(COALESCE(fds.multicast_publishers_count, 0))  AS multicast_publishers_count,
			toUInt64(COALESCE(fds.max_unicast_users, 0))          AS max_unicast_users,
			toUInt64(COALESCE(fds.max_multicast_subscribers, 0))  AS max_multicast_subscribers,
			toUInt64(COALESCE(fds.max_multicast_publishers, 0))   AS max_multicast_publishers
		FROM dz_facilities_current l
		LEFT JOIN facility_device_stats fds ON l.pk = fds.location_pk
		LEFT JOIN facility_user_counts fuc ON l.pk = fuc.location_pk
		LEFT JOIN facility_metro fm ON l.pk = fm.location_pk
	)
`

// facilitiesSimpleCTE is used as a fallback when dz_devices_current lacks location_pk
// (i.e. before migration 20260421000002 has been applied).
const facilitiesSimpleCTE = `
	WITH enriched AS (
		SELECT
			l.pk AS pk,
			l.code AS code,
			COALESCE(l.name, '')    AS name,
			COALESCE(l.country, '') AS country,
			COALESCE(l.lat, 0)      AS lat,
			COALESCE(l.lng, 0)      AS lng,
			COALESCE(l.loc_id, 0)   AS loc_id,
			COALESCE(l.status, '')  AS status,
			'' AS metro_pk,
			'' AS metro_code,
			toUInt32(0) AS device_count,
			toUInt32(0) AS user_count,
			toUInt64(0) AS max_users,
		toUInt64(0) AS unicast_users_count,
		toUInt64(0) AS multicast_subscribers_count,
		toUInt64(0) AS multicast_publishers_count,
		toUInt64(0) AS max_unicast_users,
		toUInt64(0) AS max_multicast_subscribers,
		toUInt64(0) AS max_multicast_publishers
		FROM dz_facilities_current l
	)
`

func (a *API) GetFacilities(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "code", facilitySortFields)
	filters := ParseFilters(r)
	start := time.Now()

	filterClause, filterArgs := filters.BuildFilterClause(facilityFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}
	orderBy := sort.OrderByClause(facilitySortFields)

	listSuffix := `
	SELECT pk, code, name, country, lat, lng, loc_id, metro_pk, metro_code, device_count, user_count,
		max_users, unicast_users_count, multicast_subscribers_count, multicast_publishers_count,
		max_unicast_users, max_multicast_subscribers, max_multicast_publishers,
		count() OVER () AS _total
	FROM enriched
	WHERE 1=1` + whereFilter + " " + orderBy + `
	LIMIT ? OFFSET ?
	`

	var args []any
	args = append(args, filterArgs...)
	args = append(args, pagination.Limit, pagination.Offset)

	rows, err := a.envDB(ctx).Query(ctx, facilitiesEnrichedCTE+listSuffix, args...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery("facilities", duration, err)

	if err != nil && isUnknownIdentifierError(err) {
		// location_pk not yet in dz_devices_current — fall back to simple query without join
		rows, err = a.envDB(ctx).Query(ctx, facilitiesSimpleCTE+listSuffix, args...)
		metrics.RecordClickHouseQuery("facilities", time.Since(start), err)
	}

	if err != nil {
		logError("facilities query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var facilities []FacilityListItem
	var total uint64
	for rows.Next() {
		var l FacilityListItem
		if err := rows.Scan(
			&l.PK,
			&l.Code,
			&l.Name,
			&l.Country,
			&l.Lat,
			&l.Lng,
			&l.LocId,
			&l.MetroPK,
			&l.MetroCode,
			&l.DeviceCount,
			&l.UserCount,
			&l.MaxUsers,
			&l.UnicastUsersCount,
			&l.MulticastSubscribersCount,
			&l.MulticastPublishersCount,
			&l.MaxUnicastUsers,
			&l.MaxMulticastSubscribers,
			&l.MaxMulticastPublishers,
			&total,
		); err != nil {
			logError("facilities row scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		facilities = append(facilities, l)
	}

	if err := rows.Err(); err != nil {
		logError("facilities rows iteration failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	if facilities == nil {
		facilities = []FacilityListItem{}
	}

	response := PaginatedResponse[FacilityListItem]{
		Items:  facilities,
		Total:  int(total),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}

func (a *API) GetFacility(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	start := time.Now()

	detailSuffix := `
	SELECT pk, code, name, country, lat, lng, loc_id, status, metro_pk, metro_code, device_count, user_count,
		max_users, unicast_users_count, multicast_subscribers_count, multicast_publishers_count,
		max_unicast_users, max_multicast_subscribers, max_multicast_publishers
	FROM enriched
	WHERE pk = ?
	LIMIT 1
	`

	rows, err := a.envDB(ctx).Query(ctx, facilitiesEnrichedCTE+detailSuffix, pk)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery("facilities", duration, err)

	if err != nil && isUnknownIdentifierError(err) {
		rows, err = a.envDB(ctx).Query(ctx, facilitiesSimpleCTE+detailSuffix, pk)
		metrics.RecordClickHouseQuery("facilities", time.Since(start), err)
	}

	if err != nil {
		logError("facility query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	if !rows.Next() {
		http.Error(w, "facility not found", http.StatusNotFound)
		return
	}

	var l FacilityDetail
	if err := rows.Scan(
		&l.PK,
		&l.Code,
		&l.Name,
		&l.Country,
		&l.Lat,
		&l.Lng,
		&l.LocId,
		&l.Status,
		&l.MetroPK,
		&l.MetroCode,
		&l.DeviceCount,
		&l.UserCount,
		&l.MaxUsers,
		&l.UnicastUsersCount,
		&l.MulticastSubscribersCount,
		&l.MulticastPublishersCount,
		&l.MaxUnicastUsers,
		&l.MaxMulticastSubscribers,
		&l.MaxMulticastPublishers,
	); err != nil {
		logError("facility row scan failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(l); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// isUnknownIdentifierError returns true for ClickHouse error code 47 (UNKNOWN_IDENTIFIER),
// which fires when a column referenced in a query does not exist in the table.
func isUnknownIdentifierError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "code: 47") || strings.Contains(msg, "cannot be resolved")
}
