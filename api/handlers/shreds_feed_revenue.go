package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

// usdcBaseUnitsPerDollar is USDC's 6 decimals. Base units are stored and only
// converted at this edge, so no rounding happens before the response.
const usdcBaseUnitsPerDollar = 1_000_000.0

// ShredsFeedCodePrefix scopes a feed-revenue read to the Solana shreds feeds.
// The feed-subscription program is shared by every DoubleZero feed product, so
// the shreds economics page passes this to keep another product's revenue out of
// its totals.
const ShredsFeedCodePrefix = "solana-shreds"

// ShredFeedRevenueItem is one feed's collected USDC for one calendar month.
//
// Code and Name are empty when dz_feeds_current has no row for the feed yet.
type ShredFeedRevenueItem struct {
	FeedKey          string  `json:"feed_key"`
	Code             string  `json:"code"`
	Name             string  `json:"name"`
	Year             uint16  `json:"year"`
	Month            uint8   `json:"month"`
	CollectedUSDC    uint64  `json:"collected_usdc"`
	CollectedDollars float64 `json:"collected_dollars"`
}

// GetShredFeedRevenue returns feed-subscription revenue at the (feed, year,
// month) grain.
//
// collected_usdc comes from the on-chain FeedDistribution account's
// collected_usdc_amount, which the program only increments. It is not the feed
// vault's balance: that drains to zero once a month settles.
//
// The month a row carries is the month the revenue is allocated to, not the
// month the cash arrived, so a row can exist for a month that has not started.
// The program splits one subscription payment across the calendar months the
// subscription spans: on 2026-08-19 every live feed's August and September rows
// summed to an exact round subscription price and split it in complementary day
// fractions of August's 31 days (solana-shreds-full-ams: 1,887.096773 +
// 2,612.903227 = 4,500.00, which is 13/31 + 18/31). Sum every row for cash
// collected to date. Do not read one month's row as that month's takings, and
// treat the current month and anything after it as still accruing.
//
// code_prefix filters on dz_feeds_current.code (see ShredsFeedCodePrefix). An
// empty prefix returns every feed. A feed whose dz_feeds_current row has not
// landed carries no code and is always kept: revenue must not disappear because
// a label is late.
//
// The result is a few dozen rows off a small dimension, so there is no
// pagination and no page-cache entry.
func (a *API) GetShredFeedRevenue(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	codePrefix := r.URL.Query().Get("code_prefix")

	start := time.Now()
	// LEFT JOIN, not INNER: a feed whose serviceability snapshot has not caught
	// up must still show the revenue it collected. The join runs in a subquery so
	// the prefix filter reads the coalesced code rather than the nullable join
	// column, which keeps an unlabelled feed in the result on either branch.
	query := `
		SELECT feed_key, code, name, year, month, collected_usdc
		FROM (
			SELECT
				fd.feed_key AS feed_key,
				ifNull(f.code, '') AS code,
				ifNull(f.name, '') AS name,
				fd.year AS year,
				fd.month AS month,
				fd.collected_usdc AS collected_usdc
			FROM dim_dz_shred_feed_distributions_current AS fd
			LEFT JOIN dz_feeds_current AS f ON f.pk = fd.feed_key
		)
		WHERE ? = '' OR code = '' OR startsWith(code, ?)
		ORDER BY year DESC, month DESC, collected_usdc DESC, feed_key
	`

	rows, err := a.envDB(ctx).Query(ctx, query, codePrefix, codePrefix)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery("shreds", duration, err)

	if err != nil {
		// The dimension's migration ships with the indexer, so an API pod can
		// serve this route before the table exists, and an env whose database
		// the indexer does not write never gets it at all. Neither is
		// actionable: warn and answer with an empty list, which hides the page's
		// section exactly as on a cluster without the program deployed. Without
		// this an "Unknown table" error would land at ERROR on every page load
		// and page on-call through a deploy.
		if isMissingTable(err) {
			logWarn("shred feed revenue table not available", "error", err)
			writeJSON(w, []ShredFeedRevenueItem{})
			return
		}
		logError("shred feed revenue query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	items := []ShredFeedRevenueItem{}
	for rows.Next() {
		var item ShredFeedRevenueItem
		if err := rows.Scan(&item.FeedKey, &item.Code, &item.Name, &item.Year, &item.Month, &item.CollectedUSDC); err != nil {
			logError("shred feed revenue row scan", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		item.CollectedDollars = float64(item.CollectedUSDC) / usdcBaseUnitsPerDollar
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		logError("shred feed revenue rows", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(items); err != nil {
		logError("failed to encode response", "error", err)
	}
}
