package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

// MulticastHealthSummariesCacheKey is the page-cache key for hot multicast
// group health summaries.
const MulticastHealthSummariesCacheKey = "multicast_health_summaries"

// MulticastHealthCachedPageSize is the page size the worker pre-fetches for
// the hot ShredGroupPK on /health/users and /health/paths. The UI defaults
// to this same value so the first paint hits the cache; anything else
// (different page size or non-zero offset) falls through to a live query.
const MulticastHealthCachedPageSize = 25

// multicast health users / paths per-pk cache keys.
const (
	multicastHealthUsersCacheKeyPrefix = "multicast_health_users:"
	multicastHealthPathsCacheKeyPrefix = "multicast_health_paths:"
)

// MulticastHealthUsersCacheKey returns the per-pk cache key for the worker
// to write and the handler to read for the hot first page of
// /health/users. One row per pk, no list walk.
func MulticastHealthUsersCacheKey(pk string) string {
	return multicastHealthUsersCacheKeyPrefix + pk
}

// MulticastHealthPathsCacheKey returns the per-pk cache key for /health/paths.
func MulticastHealthPathsCacheKey(pk string) string {
	return multicastHealthPathsCacheKeyPrefix + pk
}

// GetMulticastGroupHealth returns per-group health counts across mroutes,
// multicast users, and publisher↔subscriber paths. Reads from the three
// health_* views, or from the page cache for mainnet requests when available.
func (a *API) GetMulticastGroupHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing multicast group pk", http.StatusBadRequest)
		return
	}

	if isMainnet(r.Context()) {
		if cached, ok := a.readMulticastHealthSummaryCache(ctx, pkOrCode); ok {
			w.Header().Set("X-Cache", "HIT")
			writeJSON(w, cached)
			return
		}
	}
	w.Header().Set("X-Cache", "MISS")

	resp, err := a.FetchMulticastGroupHealthData(ctx, pkOrCode)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "multicast group not found", http.StatusNotFound)
			return
		}
		logError("multicast group health query error", "error", err, "pk", pkOrCode)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, resp)
}

func (a *API) readMulticastHealthSummaryCache(ctx context.Context, pkOrCode string) (*MulticastHealthGroupSummaryResponse, bool) {
	data, err := a.readPageCache(ctx, MulticastHealthSummariesCacheKey)
	if err != nil {
		return nil, false
	}

	var cached MulticastHealthSummariesCache
	if err := json.Unmarshal(data, &cached); err != nil {
		return nil, false
	}

	for i := range cached.Summaries {
		summary := cached.Summaries[i]
		if summary.Group.PK == pkOrCode || summary.Group.Code == pkOrCode {
			return &summary, true
		}
	}
	return nil, false
}

// FetchMulticastGroupHealthData performs the live per-group summary query.
func (a *API) FetchMulticastGroupHealthData(ctx context.Context, pkOrCode string) (*MulticastHealthGroupSummaryResponse, error) {
	group, err := a.queryMulticastDeliveryGroup(ctx, pkOrCode)
	if err != nil {
		return nil, err
	}

	counts, err := a.queryMulticastHealthCounts(ctx, group.PK)
	if err != nil {
		return nil, err
	}

	return &MulticastHealthGroupSummaryResponse{
		Group:           group,
		SourceAvailable: true,
		GeneratedAt:     formatMulticastTime(time.Now().UTC()),
		Counts:          counts,
	}, nil
}

// FetchMulticastHealthSummariesData builds the page-cache payload for hot
// multicast group health summaries. With no explicit groups it caches the
// large edge-solana-shreds group, which is the high-cardinality Health tab.
func (a *API) FetchMulticastHealthSummariesData(ctx context.Context, pkOrCodes ...string) (*MulticastHealthSummariesCache, error) {
	if len(pkOrCodes) == 0 {
		pkOrCodes = []string{ShredGroupPK}
	}

	generatedAt := formatMulticastTime(time.Now().UTC())
	cache := &MulticastHealthSummariesCache{
		GeneratedAt: generatedAt,
		Summaries:   []MulticastHealthGroupSummaryResponse{},
	}
	seen := map[string]bool{}
	for _, pkOrCode := range pkOrCodes {
		if pkOrCode == "" || seen[pkOrCode] {
			continue
		}
		seen[pkOrCode] = true

		summary, err := a.FetchMulticastGroupHealthData(ctx, pkOrCode)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}
		summary.GeneratedAt = generatedAt
		cache.Summaries = append(cache.Summaries, *summary)
	}
	return cache, nil
}

// queryMulticastHealthCounts rolls up per-status totals across the three
// health views in a single round-trip via a UNION ALL of grouped SELECTs.
func (a *API) queryMulticastHealthCounts(ctx context.Context, groupPK string) (MulticastHealthCounts, error) {
	out := MulticastHealthCounts{}
	query := `
		SELECT source, health_status, count() AS n FROM (
			SELECT 'mroutes' AS source, health_status FROM health_mroute WHERE multicast_group_pk = ?
			UNION ALL
			SELECT 'users' AS source, health_status FROM health_multicast_user_rate WHERE multicast_group_pk = ?
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
	case "unknown":
		bucket.Unknown += n
	}
	bucket.Total += n
}

// parseHealthSearch returns the trimmed `?search=` query param. Empty when
// absent; handlers treat empty as "no filter" and avoid appending any
// WHERE-LIKE clause.
func parseHealthSearch(r *http.Request) string {
	return strings.TrimSpace(r.URL.Query().Get("search"))
}

// healthSearchTokens splits a search string into whitespace-separated tokens.
// Each token is either `field:value` (case-insensitive field, free-form
// value) or a bare term (matched as substring across the table's default
// columns). Multiple tokens AND together.
//
// Examples:
//
//	"device:nyc001 status:unhealthy"      → two field-scoped filters
//	"3b2Ze7VY"                            → one bare term (any column)
//	"device:nyc001 unhealthy"             → field-scoped + bare term
type healthSearchToken struct {
	field string // empty for bare terms
	value string
}

// tokenizeHealthSearch accepts tokens separated by EITHER commas or
// whitespace. The web UI's InlineFilter convention writes
// `?search=device:foo,status:bar` (comma-delimited chips); operators
// poking the API by hand tend to use spaces. We accept both. A leading
// `all:` prefix (InlineFilter's marker for a bare term) is unwrapped so
// the server sees just the value.
func tokenizeHealthSearch(search string) []healthSearchToken {
	out := []healthSearchToken{}
	for _, raw := range strings.FieldsFunc(search, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	}) {
		if idx := strings.IndexByte(raw, ':'); idx > 0 && idx < len(raw)-1 {
			field := strings.ToLower(raw[:idx])
			value := raw[idx+1:]
			if field == "all" {
				// InlineFilter writes `all:value` for unprefixed text;
				// treat it as a bare term.
				out = append(out, healthSearchToken{value: value})
				continue
			}
			out = append(out, healthSearchToken{field: field, value: value})
			continue
		}
		// Bare term or malformed field prefix → substring fallback.
		out = append(out, healthSearchToken{value: strings.TrimSuffix(raw, ":")})
	}
	return out
}

// buildHealthSearchClause builds a parameterized WHERE clause from a search
// string. Tokens of the form `field:value` are matched against the column
// listed under that field in fieldMap (substring, case-insensitive). Bare
// tokens are OR'd across fallbackCols. Multiple tokens AND together.
//
// Returns ("", nil) when search is empty so callers can unconditionally
// append. Tokens whose field prefix isn't in fieldMap silently fall back to
// the bare-token path so a typo doesn't break the whole query.
// healthSearchFieldSpec describes how a field:value token is matched.
// exact=true does case-insensitive equality (right for enum-like columns
// like health_status / mode where substring would match too much, e.g.
// `status:healthy` accidentally hitting `unhealthy`). exact=false is
// case-insensitive substring via positionCaseInsensitive (right for
// free-form columns like user_pk / device_code / dz_ip).
type healthSearchFieldSpec struct {
	cols  []string
	exact bool
}

func buildHealthSearchClause(search string, fieldMap map[string]healthSearchFieldSpec, fallbackCols []string) (string, []any) {
	if search == "" {
		return "", nil
	}
	tokens := tokenizeHealthSearch(search)
	if len(tokens) == 0 {
		return "", nil
	}
	clauses := make([]string, 0, len(tokens))
	args := make([]any, 0, len(tokens))
	for _, tok := range tokens {
		if tok.value == "" {
			continue
		}
		// Resolve the columns + match mode. Unknown prefix → fallback
		// substring across the table's default columns.
		var cols []string
		exact := false
		if tok.field != "" {
			if spec, ok := fieldMap[tok.field]; ok && len(spec.cols) > 0 {
				cols = spec.cols
				exact = spec.exact
			}
		}
		if cols == nil {
			cols = fallbackCols
		}
		parts := make([]string, 0, len(cols))
		for _, c := range cols {
			if exact {
				// Equality on lower(col) gives case-insensitive exact match.
				// toString() handles non-string columns (tunnel id).
				parts = append(parts, "lower(toString("+c+")) = lower(?)")
			} else {
				parts = append(parts, "positionCaseInsensitive(toString("+c+"), ?) > 0")
			}
			args = append(args, tok.value)
		}
		if len(parts) == 0 {
			continue
		}
		clauses = append(clauses, "("+strings.Join(parts, " OR ")+")")
	}
	if len(clauses) == 0 {
		return "", nil
	}
	return " AND " + strings.Join(clauses, " AND "), args
}

// parseLimitOffset extracts optional pagination params. Both default to 0,
// which means "stream all rows" for handlers that don't set a bounded
// default before querying.
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

func (a *API) queryMulticastHealthTotal(ctx context.Context, table, whereClause string, args []any, metricName string) (int, error) {
	query := `
		SELECT count()
		FROM ` + table + `
		WHERE ` + whereClause + `
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`
	var total uint64
	start := time.Now()
	err := a.envDB(ctx).QueryRow(ctx, query, args...).Scan(&total)
	metrics.RecordClickHouseQuery(metricName, time.Since(start), err)
	if err != nil {
		return 0, err
	}
	return int(total), nil
}

// Lower-cased helper for the per-(pk-or-code) pattern, mirroring the existing
// multicast-delivery group resolution but exposed publicly within the package
// so the health endpoints can reuse it.
var _ = strings.TrimSpace
