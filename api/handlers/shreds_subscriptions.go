package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

// ShredSubscriptionItem is one Solana Shreds feed seat held on an access pass.
type ShredSubscriptionItem struct {
	PassPK         string                  `json:"pass_pk"`
	PassStatus     string                  `json:"pass_status"`
	OwnerPubkey    string                  `json:"owner_pubkey"`
	Payer          string                  `json:"payer"`
	FeedPK         string                  `json:"feed_pk"`
	FeedCode       string                  `json:"feed_code"`
	FeedName       string                  `json:"feed_name"`
	MetroPK        string                  `json:"metro_pk"`
	MetroCode      string                  `json:"metro_code"`
	MaxUsers       uint8                   `json:"max_users"`
	MaxFutureUsers uint8                   `json:"max_future_users"`
	CurrentUsers   uint8                   `json:"current_users"`
	AnniversaryDay uint8                   `json:"anniversary_day"`
	WindowEnd      string                  `json:"window_end"`
	TerminatesAt   string                  `json:"terminates_at"`
	Users          []ShredSubscriptionUser `json:"users"`
	StartedAt      string                  `json:"started_at"`
}

// ShredSubscriptionUser is one activated DZ user account attached to a feed.
type ShredSubscriptionUser struct {
	PK         string `json:"pk"`
	DevicePK   string `json:"device_pk"`
	DeviceCode string `json:"device_code"`
	BGPStatus  string `json:"bgp_status"`
}

// shredSubscriptionSeatsCTE flattens the feed_seats array into one row per
// (pass, feed)
const shredSubscriptionSeatsCTE = `seats AS (
		SELECT
			ap.entity_id AS entity_id,
			ap.pk AS pass_pk,
			ap.owner_pubkey AS owner_pubkey,
			ap.user_payer AS payer,
			ap.status AS pass_status,
			JSONExtractString(e, 'feed_pk') AS feed_pk,
			toUInt8(JSONExtractUInt(e, 'max_users')) AS max_users,
			toUInt8(JSONExtractUInt(e, 'max_future_users')) AS max_future_users,
			toUInt8(JSONExtractUInt(e, 'current_users')) AS current_users,
			toUInt8(JSONExtractUInt(e, 'anniversary_day')) AS anniversary_day,
			toDateTime(JSONExtractInt(e, 'window_end')) AS window_end,
			toDateTime(JSONExtractInt(e, 'terminates_at')) AS terminates_at
		FROM dz_access_passes_current AS ap
		ARRAY JOIN JSONExtractArrayRaw(ap.feed_seats) AS e
		WHERE ap.type_tag = 'edge_seat' AND ap.feed_seats NOT IN ('', '[]')
	)`

// shredSubscriptionFeedsCTE collapses the feed dimension to one row per pk so a
// duplicate cannot double a seat.
const shredSubscriptionFeedsCTE = `feed_map AS (
		SELECT pk AS feed_pk, any(code) AS feed_code, any(name) AS feed_name, any(metro_pk) AS metro_pk
		FROM dz_feeds_current
		GROUP BY pk
	)`

// shredSubscriptionStartedCTE is the first snapshot each seat appeared on its
// pass.
const shredSubscriptionStartedCTE = `started AS (
		SELECT
			entity_id,
			JSONExtractString(e, 'feed_pk') AS feed_pk,
			min(snapshot_ts) AS started_at
		FROM dim_dz_access_passes_history
		ARRAY JOIN JSONExtractArrayRaw(feed_seats) AS e
		WHERE type_tag = 'edge_seat' AND feed_seats NOT IN ('', '[]')
		GROUP BY entity_id, feed_pk
	)`

// shredSubscriptionConnectedCTE resolves the users attached to each feed, and
// through them the devices they connected on.
const shredSubscriptionConnectedCTE = `connected AS (
		SELECT
			payer,
			feed_pk,
			arrayMap(x -> tupleElement(x, 1), users) AS user_pks,
			arrayMap(x -> tupleElement(x, 2), users) AS user_device_pks,
			arrayMap(x -> tupleElement(x, 3), users) AS user_device_codes,
			arrayMap(x -> tupleElement(x, 4), users) AS user_bgp_statuses,
			first_device_code
		FROM (
			SELECT
				uf.payer AS payer,
				uf.feed_pk AS feed_pk,
				groupArray(tuple(
					uf.user_pk,
					ifNull(d.pk, ''),
					ifNull(d.code, ''),
					uf.bgp_status
				)) AS users,
				min(ifNull(d.code, '')) AS first_device_code
			FROM (
				SELECT
					u.owner_pubkey AS payer,
					u.pk AS user_pk,
					u.device_pk AS device_pk,
					u.bgp_status AS bgp_status,
					fp AS feed_pk
				FROM dz_users_current AS u
				ARRAY JOIN JSONExtract(u.feed_pks, 'Array(String)') AS fp
				WHERE u.status = 'activated'
			) AS uf
			LEFT JOIN dz_devices_current AS d ON d.pk = uf.device_pk
			GROUP BY payer, feed_pk
		)
	)`

const shredSubscriptionFrom = `
		FROM seats AS s
		INNER JOIN feed_map AS f ON f.feed_pk = s.feed_pk
		LEFT JOIN dz_metros_current AS m ON m.pk = f.metro_pk
		LEFT JOIN connected AS c ON c.payer = s.payer AND c.feed_pk = s.feed_pk
	`

var shredSubscriptionSortFields = map[string]string{
	"pass":       "s.pass_pk",
	"payer":      "s.payer",
	"feed":       "feed_name",
	"metro":      "metro_code",
	"device":     "first_device_code",
	"users":      "s.current_users",
	"window_end": "s.window_end",
	"started_at": "started_at;s.pass_pk ASC",
}

// A seat can hold more than one user, so device and user filter against every
// one of them joined into a searchable string.
var shredSubscriptionFilterFields = map[string]FilterFieldConfig{
	"pass":   {Column: "s.pass_pk", Type: FieldTypeText},
	"payer":  {Column: "s.payer", Type: FieldTypeText},
	"feed":   {Column: "f.feed_name", Type: FieldTypeText},
	"metro":  {Column: "COALESCE(m.code, '')", Type: FieldTypeText},
	"device": {Column: "arrayStringConcat(c.user_device_codes, ' ')", Type: FieldTypeText},
	"user":   {Column: "arrayStringConcat(c.user_pks, ' ')", Type: FieldTypeText},
	"users":  {Column: "s.current_users", Type: FieldTypeNumeric},
}

// shredSubscriptionStatusClauses are the three states a feed seat can be in, all
// cut on window_end — the date the page reports as the term end — which makes
// them exhaustive and disjoint.
var shredSubscriptionStatusClauses = map[string]string{
	"active":  "(s.window_end > now() AND s.current_users > 0)",
	"pending": "(s.window_end > now() AND s.current_users = 0)",
	"expired": "s.window_end <= now()",
}

// GetShredSubscriptions returns a page of Solana Shreds feed subscriptions —
// the seats sold by the feed subscription program, as opposed to the per-epoch
// client seats GetShredClientSeats serves.
func (a *API) GetShredSubscriptions(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "started_at", shredSubscriptionSortFields)
	filters := ParseFilters(r)

	whereClauses := []string{"startsWith(f.feed_code, ?)"}
	whereArgs := []any{ShredsFeedCodePrefix}

	filterClause, filterArgs := filters.BuildFilterClause(shredSubscriptionFilterFields)
	if filterClause != "" {
		whereClauses = append(whereClauses, filterClause)
		whereArgs = append(whereArgs, filterArgs...)
	}

	if statusParam := r.URL.Query().Get("status"); statusParam != "" {
		var statusOr []string
		for _, s := range splitCSV(statusParam) {
			if clause, ok := shredSubscriptionStatusClauses[s]; ok {
				statusOr = append(statusOr, clause)
			}
		}
		if len(statusOr) > 0 {
			whereClauses = append(whereClauses, "("+strings.Join(statusOr, " OR ")+")")
		} else {
			whereClauses = append(whereClauses, "0")
		}
	}

	whereSQL := " WHERE " + strings.Join(whereClauses, " AND ")

	start := time.Now()

	countQuery := `
		WITH ` + shredSubscriptionSeatsCTE + `,
		` + shredSubscriptionFeedsCTE + `,
		` + shredSubscriptionConnectedCTE + `
		SELECT count()` + shredSubscriptionFrom + whereSQL

	var total uint64
	if err := a.envDB(ctx).QueryRow(ctx, countQuery, whereArgs...).Scan(&total); err != nil {
		metrics.RecordClickHouseQuery("shreds", time.Since(start), err)
		if isMissingTable(err) {
			logWarn("shred subscriptions tables not available", "error", err)
			writeJSON(w, PaginatedResponse[ShredSubscriptionItem]{
				Items: []ShredSubscriptionItem{}, Limit: pagination.Limit, Offset: pagination.Offset,
			})
			return
		}
		logError("shred subscriptions count failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	query := `
		WITH ` + shredSubscriptionSeatsCTE + `,
		` + shredSubscriptionFeedsCTE + `,
		` + shredSubscriptionConnectedCTE + `,
		` + shredSubscriptionStartedCTE + `
		SELECT
			s.pass_pk, s.pass_status, s.owner_pubkey, s.payer,
			s.feed_pk, f.feed_code AS feed_code, f.feed_name AS feed_name,
			COALESCE(f.metro_pk, '') AS metro_pk, COALESCE(m.code, '') AS metro_code,
			s.max_users, s.max_future_users, s.current_users, s.anniversary_day,
			s.window_end, s.terminates_at, st.started_at AS started_at,
			c.user_pks AS user_pks, c.user_device_pks AS user_device_pks,
			c.user_device_codes AS user_device_codes, c.user_bgp_statuses AS user_bgp_statuses` +
		shredSubscriptionFrom + `
		LEFT JOIN started AS st ON st.entity_id = s.entity_id AND st.feed_pk = s.feed_pk` +
		whereSQL + ` ` + sort.OrderByClause(shredSubscriptionSortFields) + ` LIMIT ? OFFSET ?`

	rows, err := a.envDB(ctx).Query(ctx, query, append(whereArgs, pagination.Limit, pagination.Offset)...)
	metrics.RecordClickHouseQuery("shreds", time.Since(start), err)
	if err != nil {
		logError("shred subscriptions query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	items := []ShredSubscriptionItem{}
	for rows.Next() {
		var (
			item                               ShredSubscriptionItem
			windowEnd, terminatesAt, startedAt time.Time
			userPKs, devicePKs                 []string
			deviceCodes, bgpStatuses           []string
		)
		if err := rows.Scan(
			&item.PassPK, &item.PassStatus, &item.OwnerPubkey, &item.Payer,
			&item.FeedPK, &item.FeedCode, &item.FeedName,
			&item.MetroPK, &item.MetroCode,
			&item.MaxUsers, &item.MaxFutureUsers, &item.CurrentUsers, &item.AnniversaryDay,
			&windowEnd, &terminatesAt, &startedAt,
			&userPKs, &devicePKs, &deviceCodes, &bgpStatuses,
		); err != nil {
			logError("shred subscriptions row scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		item.WindowEnd = formatSubscriptionTime(windowEnd)
		item.TerminatesAt = formatSubscriptionTime(terminatesAt)
		item.StartedAt = formatSubscriptionTime(startedAt)
		item.Users = zipSubscriptionUsers(userPKs, devicePKs, deviceCodes, bgpStatuses)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		logError("shred subscriptions rows failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(PaginatedResponse[ShredSubscriptionItem]{
		Items: items, Total: int(total), Limit: pagination.Limit, Offset: pagination.Offset,
	}); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// zipSubscriptionUsers pairs up the parallel arrays groupArray produced, sorted
// by user pk so a row does not reshuffle between reads — groupArray promises no
// order of its own.
func zipSubscriptionUsers(pks, devicePKs, deviceCodes, bgpStatuses []string) []ShredSubscriptionUser {
	users := make([]ShredSubscriptionUser, 0, len(pks))
	for i, pk := range pks {
		u := ShredSubscriptionUser{PK: pk}
		if i < len(devicePKs) {
			u.DevicePK = devicePKs[i]
		}
		if i < len(deviceCodes) {
			u.DeviceCode = deviceCodes[i]
		}
		if i < len(bgpStatuses) {
			u.BGPStatus = bgpStatuses[i]
		}
		users = append(users, u)
	}
	slices.SortFunc(users, func(a, b ShredSubscriptionUser) int { return strings.Compare(a.PK, b.PK) })
	return users
}

// formatSubscriptionTime renders a timestamp, or "" for the zero value a cleared
// window or an absent history row scans as — the UI shows a dash rather than 1970.
func formatSubscriptionTime(t time.Time) string {
	if t.IsZero() || t.Unix() <= 0 {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
