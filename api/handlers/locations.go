package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

type LocationListItem struct {
	PK          string  `json:"pk"`
	Code        string  `json:"code"`
	Name        string  `json:"name"`
	Country     string  `json:"country"`
	Lat         float64 `json:"lat"`
	Lng         float64 `json:"lng"`
	LocId       uint32  `json:"loc_id"`
	MetroPK     string  `json:"metro_pk"`
	MetroCode   string  `json:"metro_code"`
	DeviceCount uint32  `json:"device_count"`
	UserCount   uint32  `json:"user_count"`
}

type LocationDetail struct {
	PK          string  `json:"pk"`
	Code        string  `json:"code"`
	Name        string  `json:"name"`
	Country     string  `json:"country"`
	Lat         float64 `json:"lat"`
	Lng         float64 `json:"lng"`
	LocId       uint32  `json:"loc_id"`
	Status      string  `json:"status"`
	MetroPK     string  `json:"metro_pk"`
	MetroCode   string  `json:"metro_code"`
	DeviceCount uint32  `json:"device_count"`
	UserCount   uint32  `json:"user_count"`
}

var locationSortFields = map[string]string{
	"code":    "code",
	"name":    "name",
	"country": "country",
	"loc_id":  "loc_id",
	"metro":   "metro_code",
	"devices": "device_count",
	"users":   "user_count",
}

var locationFilterFields = map[string]FilterFieldConfig{
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

// locationsEnrichedCTE joins locations with devices (via location_pk), metros, and users.
// Requires migration 20260421000002 to add location_pk to dz_devices_current.
const locationsEnrichedCTE = `
	WITH enriched AS (
		SELECT
			l.pk AS pk,
			l.code AS code,
			COALESCE(l.name, '') AS name,
			COALESCE(l.country, '') AS country,
			COALESCE(l.lat, 0) AS lat,
			COALESCE(l.lng, 0) AS lng,
			COALESCE(l.loc_id, 0) AS loc_id,
			COALESCE(l.status, '') AS status,
			COALESCE(any(m.pk), '') AS metro_pk,
			COALESCE(any(m.code), '') AS metro_code,
			toUInt32(countDistinctIf(d.pk, d.pk != '')) AS device_count,
			toUInt32(countDistinctIf(u.pk, u.pk != '')) AS user_count
		FROM dz_locations_current l
		LEFT JOIN dz_devices_current d ON d.location_pk = l.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_users_current u ON u.device_pk = d.pk AND u.status = 'activated'
		GROUP BY l.pk, l.code, l.name, l.country, l.lat, l.lng, l.loc_id, l.status
	)
`

// locationsSimpleCTE is used as a fallback when dz_devices_current lacks location_pk
// (i.e. before migration 20260421000002 has been applied).
const locationsSimpleCTE = `
	WITH enriched AS (
		SELECT
			l.pk AS pk,
			l.code AS code,
			COALESCE(l.name, '') AS name,
			COALESCE(l.country, '') AS country,
			COALESCE(l.lat, 0) AS lat,
			COALESCE(l.lng, 0) AS lng,
			COALESCE(l.loc_id, 0) AS loc_id,
			COALESCE(l.status, '') AS status,
			'' AS metro_pk,
			'' AS metro_code,
			toUInt32(0) AS device_count,
			toUInt32(0) AS user_count
		FROM dz_locations_current l
	)
`

// isUnknownIdentifierError returns true for ClickHouse error code 47 (UNKNOWN_IDENTIFIER),
// which fires when a column referenced in a query does not exist in the table.
func isUnknownIdentifierError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "code: 47") || strings.Contains(msg, "cannot be resolved")
}

func (a *API) GetLocations(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "code", locationSortFields)
	filters := ParseFilters(r)
	start := time.Now()

	filterClause, filterArgs := filters.BuildFilterClause(locationFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}
	orderBy := sort.OrderByClause(locationSortFields)

	listSuffix := `
		SELECT pk, code, name, country, lat, lng, loc_id, metro_pk, metro_code, device_count, user_count,
			count() OVER () AS _total
		FROM enriched
		WHERE 1=1` + whereFilter + " " + orderBy + `
		LIMIT ? OFFSET ?
	`

	var args []any
	args = append(args, filterArgs...)
	args = append(args, pagination.Limit, pagination.Offset)

	rows, err := a.envDB(ctx).Query(ctx, locationsEnrichedCTE+listSuffix, args...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil && isUnknownIdentifierError(err) {
		// location_pk not yet in dz_devices_current — fall back to simple query without join
		rows, err = a.envDB(ctx).Query(ctx, locationsSimpleCTE+listSuffix, args...)
		metrics.RecordClickHouseQuery(time.Since(start), err)
	}

	if err != nil {
		logError("locations query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var locations []LocationListItem
	var total uint64
	for rows.Next() {
		var l LocationListItem
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
			&total,
		); err != nil {
			logError("locations row scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		locations = append(locations, l)
	}

	if err := rows.Err(); err != nil {
		logError("locations rows iteration failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	if locations == nil {
		locations = []LocationListItem{}
	}

	response := PaginatedResponse[LocationListItem]{
		Items:  locations,
		Total:  int(total),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}

func (a *API) GetLocation(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	start := time.Now()

	detailSuffix := `
		SELECT pk, code, name, country, lat, lng, loc_id, status, metro_pk, metro_code, device_count, user_count
		FROM enriched
		WHERE pk = ?
		LIMIT 1
	`

	rows, err := a.envDB(ctx).Query(ctx, locationsEnrichedCTE+detailSuffix, pk)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil && isUnknownIdentifierError(err) {
		rows, err = a.envDB(ctx).Query(ctx, locationsSimpleCTE+detailSuffix, pk)
		metrics.RecordClickHouseQuery(time.Since(start), err)
	}

	if err != nil {
		logError("location query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	if !rows.Next() {
		http.Error(w, "location not found", http.StatusNotFound)
		return
	}

	var l LocationDetail
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
	); err != nil {
		logError("location row scan failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(l); err != nil {
		logError("failed to encode response", "error", err)
	}
}
