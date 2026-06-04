package handlers

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

// GetMulticastGroupHealth returns per-group health counts across mroutes,
// multicast users, and publisher↔subscriber paths. Reads from the three
// health_* views.
func (a *API) GetMulticastGroupHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing multicast group pk", http.StatusBadRequest)
		return
	}

	group, err := a.queryMulticastDeliveryGroup(ctx, pkOrCode)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "multicast group not found", http.StatusNotFound)
			return
		}
		logError("multicast group health query error", "error", err, "pk", pkOrCode)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	counts, err := a.queryMulticastHealthCounts(ctx, group.PK)
	if err != nil {
		logError("multicast health counts error", "error", err, "pk", group.PK)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, MulticastHealthGroupSummaryResponse{
		Group:           group,
		SourceAvailable: true,
		GeneratedAt:     formatMulticastTime(time.Now().UTC()),
		Counts:          counts,
	})
}

// queryMulticastHealthCounts rolls up per-status totals across the three
// health views in a single round-trip via a UNION ALL of grouped SELECTs.
func (a *API) queryMulticastHealthCounts(ctx context.Context, groupPK string) (MulticastHealthCounts, error) {
	out := MulticastHealthCounts{}
	query := `
		SELECT source, health_status, count() AS n FROM (
			SELECT 'mroutes' AS source, health_status FROM health_mroute WHERE multicast_group_pk = ?
			UNION ALL
			SELECT 'users' AS source, health_status FROM health_multicast_user WHERE multicast_group_pk = ?
			UNION ALL
			SELECT 'paths' AS source, health_status FROM health_publisher_subscriber_path WHERE multicast_group_pk = ?
		)
		GROUP BY source, health_status
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, groupPK, groupPK, groupPK)
	metrics.RecordClickHouseQuery("multicast_health_counts", time.Since(start), err)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var source, status string
		var n uint64
		if err := rows.Scan(&source, &status, &n); err != nil {
			return out, err
		}
		addStatusCount(getCountsBucket(&out, source), status, n)
	}
	return out, rows.Err()
}

func getCountsBucket(c *MulticastHealthCounts, source string) *MulticastHealthStatusCounts {
	switch source {
	case "mroutes":
		return &c.Mroutes
	case "users":
		return &c.Users
	case "paths":
		return &c.Paths
	}
	return nil
}

func addStatusCount(bucket *MulticastHealthStatusCounts, status string, n uint64) {
	if bucket == nil {
		return
	}
	switch status {
	case "healthy":
		bucket.Healthy += n
	case "degraded":
		bucket.Degraded += n
	case "unhealthy":
		bucket.Unhealthy += n
	}
	bucket.Total += n
}

// parseLimitOffset extracts optional pagination params. Both default to 0,
// which means "stream all rows" (no slicing in the handler).
func parseLimitOffset(r *http.Request) (limit, offset int) {
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			offset = n
		}
	}
	return
}

// applyPagination slices items per requested limit/offset. limit=0 means
// return all rows; offset is applied before limit. Returns the original
// total before pagination so callers can include it in the response.
func applyPagination[T any](items []T, limit, offset int) (page []T, total int) {
	total = len(items)
	if offset < 0 {
		offset = 0
	}
	if offset > total {
		offset = total
	}
	items = items[offset:]
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	return items, total
}

// Lower-cased helper for the per-(pk-or-code) pattern, mirroring the existing
// multicast-delivery group resolution but exposed publicly within the package
// so the health endpoints can reuse it.
var _ = strings.TrimSpace
