package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/utils/pkg/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

type GeolocProbe struct {
	PK                 string `json:"pk"`
	Owner              string `json:"owner"`
	ExchangePK         string `json:"exchange_pk"`
	PublicIP           string `json:"public_ip"`
	LocationOffsetPort uint16 `json:"location_offset_port"`
	MetricsPublisherPK string `json:"metrics_publisher_pk"`
	ReferenceCount     uint32 `json:"reference_count"`
	Code               string `json:"code"`
	ParentDevices      string `json:"parent_devices"`
	TargetUpdateCount  uint32 `json:"target_update_count"`
}

var geolocProbeSortFields = map[string]string{
	"code":       "code",
	"owner":      "owner",
	"ip":         "public_ip",
	"exchange":   "exchange_pk",
	"references": "reference_count",
	"updates":    "target_update_count",
}

var geolocProbeFilterFields = map[string]FilterFieldConfig{
	"code":       {Column: "code", Type: FieldTypeText},
	"owner":      {Column: "owner", Type: FieldTypeText},
	"ip":         {Column: "public_ip", Type: FieldTypeText},
	"exchange":   {Column: "exchange_pk", Type: FieldTypeText},
	"references": {Column: "reference_count", Type: FieldTypeNumeric},
	"updates":    {Column: "target_update_count", Type: FieldTypeNumeric},
}

func (a *API) GetGeolocProbes(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "code", geolocProbeSortFields)
	filters := ParseFilters(r)
	start := time.Now()

	filterClause, filterArgs := filters.BuildFilterClause(geolocProbeFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}
	orderBy := sort.OrderByClause(geolocProbeSortFields)

	query := `
		SELECT pk, owner, exchange_pk, public_ip, location_offset_port, metrics_publisher_pk, reference_count, code, parent_devices, target_update_count, count() OVER () as _total
		FROM geoloc_probes_current
		WHERE 1=1` + whereFilter + " " + orderBy + `
		LIMIT ? OFFSET ?
	`

	var args []any
	args = append(args, filterArgs...)
	args = append(args, pagination.Limit, pagination.Offset)

	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery("geoloc_probes", duration, err)

	if err != nil {
		logError("geoloc probes query error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var probes []GeolocProbe
	var total uint64
	for rows.Next() {
		var g GeolocProbe
		if err := rows.Scan(
			&g.PK,
			&g.Owner,
			&g.ExchangePK,
			&g.PublicIP,
			&g.LocationOffsetPort,
			&g.MetricsPublisherPK,
			&g.ReferenceCount,
			&g.Code,
			&g.ParentDevices,
			&g.TargetUpdateCount,
			&total,
		); err != nil {
			logError("geoloc probes scan error", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		probes = append(probes, g)
	}

	if err := rows.Err(); err != nil {
		logError("geoloc probes rows error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null
	if probes == nil {
		probes = []GeolocProbe{}
	}

	response := PaginatedResponse[GeolocProbe]{
		Items:  probes,
		Total:  int(total),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}
