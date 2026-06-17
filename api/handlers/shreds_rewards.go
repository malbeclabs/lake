package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

// Supported reward-token mints. From epoch 968 validators may be rewarded in
// one of these tokens; each has its own per-epoch ShredDistributionJournal and
// reward pool. Kept in sync with indexer/pkg/dz/shreds/validatorrewards/token.go.
const (
	rewardMint2Z   = "J6pQQ3FAcJQeWPPGppWRb4nM8jU3wLyYbRrLh7feMfvd"
	rewardMintUSDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	rewardMintWSOL = "So11111111111111111111111111111111111111112"
)

// tokenSymbolSQL maps a leaf-mint column expression to its display symbol.
func tokenSymbolSQL(col string) string {
	return fmt.Sprintf(`if(%[1]s = '%[2]s', 'USDC', if(%[1]s = '%[3]s', 'wSOL', '2Z'))`,
		col, rewardMintUSDC, rewardMintWSOL)
}

// tokenScaleSQL maps a leaf-mint column expression to its base-units-per-whole-
// token divisor (2Z=10^8, USDC=10^6, wSOL=10^9).
func tokenScaleSQL(col string) string {
	return fmt.Sprintf(`if(%[1]s = '%[2]s', 1000000.0, if(%[1]s = '%[3]s', 1000000000.0, 100000000.0))`,
		col, rewardMintUSDC, rewardMintWSOL)
}

// earnedWholeAndTokenSQL builds two parallel expressions: the per-leaf reward in
// whole tokens, and its display symbol — with a 2Z-equivalent fallback.
//
// USDC/wSOL journals don't pre-hold a swapped balance, so before a reward is
// actually distributed their pool (distributed_amount) is still 0 and the leaf's
// own-token reward would compute to 0 ("—" in the UI). In that no-pool case we
// instead value the leaf against the epoch's 2Z pool and label it 2Z, so a
// claimable non-2Z validator shows a meaningful 2Z-equivalent figure rather than
// a dash. Once the token is distributed (distributed_amount > 0) the own-token
// path is used and the real USDC/wSOL amount is shown.
//
// Requires both pool rows joined: P = the leaf's own token pool (on its mint),
// P2Z = the epoch's 2Z pool. slotsCol is the leader_slots column; leafMintExpr
// resolves the leaf's reward mint. T.total_leader_slots is the legacy fallback
// denominator.
func earnedWholeAndTokenSQL(slotsCol, leafMintExpr string) (earnedWhole, tokenSymbol string) {
	propFactor := `toFloat64(10000 - if(C.proportion > 0, C.proportion, if(C.default_proportion > 0, C.default_proportion, 3500)))`
	tokenPool := fmt.Sprintf(`if(P.reward_mint = '' OR P.reward_mint = '%s', toFloat64(P.tokens_received_2z) * 0.9, toFloat64(P.distributed_amount))`, rewardMint2Z)
	twoZPool := `toFloat64(P2Z.tokens_received_2z) * 0.9`
	denomP := `nullIf(if(P.accumulated_slots_scaled > 0, toFloat64(P.accumulated_slots_scaled), toFloat64(if(P.total_leader_slots > 0, P.total_leader_slots, T.total_leader_slots)) * 10000.0), 0)`
	denomP2Z := `nullIf(if(P2Z.accumulated_slots_scaled > 0, toFloat64(P2Z.accumulated_slots_scaled), toFloat64(if(P2Z.total_leader_slots > 0, P2Z.total_leader_slots, T.total_leader_slots)) * 10000.0), 0)`

	hasPool := fmt.Sprintf(`(%s) > 0`, tokenPool)
	earnedToken := fmt.Sprintf(`(%s) * toFloat64(%s) * %s / %s`, tokenPool, slotsCol, propFactor, denomP)
	earned2Z := fmt.Sprintf(`(%s) * toFloat64(%s) * %s / %s`, twoZPool, slotsCol, propFactor, denomP2Z)

	earnedBaseUnits := fmt.Sprintf(`if(%s, %s, %s)`, hasPool, earnedToken, earned2Z)
	scale := fmt.Sprintf(`if(%s, %s, 100000000.0)`, hasPool, tokenScaleSQL(leafMintExpr))
	earnedWhole = fmt.Sprintf(`(%s) / %s`, earnedBaseUnits, scale)
	tokenSymbol = fmt.Sprintf(`if(%s, %s, '2Z')`, hasPool, tokenSymbolSQL(leafMintExpr))
	return earnedWhole, tokenSymbol
}

// ShredsRewardsRow is a single row of the validator rewards list.
type ShredsRewardsRow struct {
	NodeID                 string             `json:"node_id"`
	VotePubkey             string             `json:"vote_pubkey"`
	ValidatorName          string             `json:"validator_name"`
	ActivatedStake         uint64             `json:"activated_stake"`
	DZUserIP               string             `json:"dz_user_ip"`
	TotalEarned2Z          float64            `json:"total_earned_2z"`
	ImmediatelyClaimable2Z float64            `json:"immediately_claimable_2z"`
	EpochEarnings          map[uint64]float64 `json:"epoch_earnings"`
	// EpochTokens is the reward-token symbol earned in each recent epoch,
	// parallel to EpochEarnings (a validator picks one token per epoch). The
	// per-epoch amount in EpochEarnings is in whole units of that token.
	EpochTokens map[uint64]string `json:"epoch_tokens"`
}

// ShredsRewardsResponse is the full response for GET /api/dz/shreds/rewards.
type ShredsRewardsResponse struct {
	CurrentSolanaEpoch   uint64             `json:"current_solana_epoch"`
	LatestFinalizedEpoch uint64             `json:"latest_finalized_epoch"`
	EpochColumns         []uint64           `json:"epoch_columns"`
	Validators           []ShredsRewardsRow `json:"validators"`
	// Total is the count of distinct validators matching the (search) filter,
	// before limit/offset — used by the client to render the page count.
	Total int `json:"total"`
}

// buildShredsRewardsSearch parses a comma-separated search expression into a
// WHERE clause plus positional args. Supported field prefixes: name:, vote:,
// node:. Free-text terms match across all three.
func buildShredsRewardsSearch(search string) (string, []any) {
	if search == "" {
		return "", nil
	}
	filters := strings.Split(search, ",")
	clauses := make([]string, 0, len(filters))
	args := make([]any, 0, len(filters))
	for _, f := range filters {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		parts := strings.SplitN(f, ":", 2)
		if len(parts) == 2 {
			field, val := strings.ToLower(strings.TrimSpace(parts[0])), strings.TrimSpace(parts[1])
			switch field {
			case "name":
				clauses = append(clauses, "positionCaseInsensitive(coalesce(va.name, ''), ?) > 0")
				args = append(args, val)
				continue
			case "vote":
				clauses = append(clauses, "positionCaseInsensitive(coalesce(v.vote_pubkey, ''), ?) > 0")
				args = append(args, val)
				continue
			case "node":
				clauses = append(clauses, "positionCaseInsensitive(pv.pv_node_id, ?) > 0")
				args = append(args, val)
				continue
			}
		}
		// Free text: match across name, vote pubkey, and node id.
		clauses = append(clauses,
			"(positionCaseInsensitive(coalesce(va.name, ''), ?) > 0 "+
				"OR positionCaseInsensitive(coalesce(v.vote_pubkey, ''), ?) > 0 "+
				"OR positionCaseInsensitive(pv.pv_node_id, ?) > 0)")
		args = append(args, f, f, f)
	}
	if len(clauses) == 0 {
		return "", nil
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

// buildShredsRewardsSort returns the ORDER BY fragment (without the keyword)
// for the given sort field and direction.
func buildShredsRewardsSort(field, order string) string {
	dir := "DESC"
	if strings.EqualFold(order, "asc") {
		dir = "ASC"
	}
	switch field {
	case "validator_name":
		return "validator_name " + dir + ", total_earned_2z DESC"
	case "activated_stake":
		return "activated_stake " + dir + ", total_earned_2z DESC"
	case "immediately_claimable_2z":
		return "immediately_claimable_2z " + dir + ", total_earned_2z DESC"
	default:
		return "total_earned_2z " + dir
	}
}

// shredsRewardsCacheKey is the Postgres page-cache key for the default-params
// list response. Refreshed by the worker every ~30s.
const shredsRewardsCacheKey = "shreds_rewards"

// shredsRewardsDefaultLimit is the page size for the default list view. It must
// match the web client's PAGE_SIZE so the cached default-params entry (computed
// with this limit by the refresh worker) is reusable for the page's first-page
// request — otherwise every page load misses the cache and runs the full query.
const shredsRewardsDefaultLimit = 100

// shredsRewardsDefaultSort / Order are the canonical default ordering the web
// sends on the first-page view. A request carrying these explicit values is
// equivalent to the param-less default and is served from the page cache.
const (
	shredsRewardsDefaultSort  = "total_earned_2z"
	shredsRewardsDefaultOrder = "desc"
)

// GetShredsRewards returns a paginated list of validators with all-time
// earnings, immediately-claimable amounts, and the last 10 epochs of
// per-epoch $2Z earnings.
func (a *API) GetShredsRewards(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	q := r.URL.Query()
	search := strings.TrimSpace(q.Get("search"))
	sortField := strings.TrimSpace(q.Get("sort"))
	order := strings.TrimSpace(q.Get("order"))

	limit := shredsRewardsDefaultLimit
	limitProvided := false
	if v := q.Get("limit"); v != "" {
		limitProvided = true
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > 500 {
				n = 500
			}
			limit = n
		}
	}
	offset := 0
	offsetProvided := false
	if v := q.Get("offset"); v != "" {
		offsetProvided = true
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	// Default-view requests are served from the page cache to keep the page
	// snappy. The worker refreshes this entry on a 30s cadence. The web client
	// always sends explicit sort/order/limit/offset, so we match the canonical
	// default *values* (not just absent params) — anything else is a custom view
	// and bypasses the cache.
	isDefault := search == "" &&
		(sortField == "" || sortField == shredsRewardsDefaultSort) &&
		(order == "" || order == shredsRewardsDefaultOrder) &&
		(!limitProvided || limit == shredsRewardsDefaultLimit) &&
		(!offsetProvided || offset == 0)
	if isDefault {
		if data, err := a.readPageCache(r.Context(), shredsRewardsCacheKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	resp, err := a.computeShredsRewards(ctx, search, sortField, order, limit, offset)
	if err != nil {
		logError("shreds rewards query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("shreds rewards encode failed", "error", err)
	}
}

// computeShredsRewards runs the rewards-list queries directly against
// ClickHouse and returns the response. It deliberately does NOT touch the page
// cache, so it is safe to call from both the handler (on a cache miss) and the
// page-cache refresh worker. (The worker must recompute fresh — if it went
// through GetShredsRewards it would read its own cached entry and write it
// straight back, freezing the cache.)
func (a *API) computeShredsRewards(ctx context.Context, search, sortField, order string, limit, offset int) (*ShredsRewardsResponse, error) {
	whereClause, args := buildShredsRewardsSearch(search)
	sortSQL := buildShredsRewardsSort(sortField, order)

	// Fetch epoch columns: the up-to-10 newest Solana epochs that have a
	// funded 2Z reward pool (non-zero tokens_received_2z on the epoch's 2Z
	// journal). `subscription_epoch` numerically equals the Solana epoch — the
	// shred-subscription program creates one ShredDistribution per Solana
	// epoch starting from its launch epoch. `associated_dz_epoch` is the
	// parent revenue-distribution program's epoch counter (different,
	// slower) and is NOT the Solana epoch.
	start := time.Now()
	epochRows, err := a.envDB(ctx).Query(ctx, `
		SELECT subscription_epoch
		FROM dim_dz_shred_distribution_2z_pool_current
		WHERE tokens_received_2z > 0
		GROUP BY subscription_epoch
		ORDER BY subscription_epoch DESC
		LIMIT 10
	`)
	metrics.RecordClickHouseQuery("shreds_rewards:epochs", time.Since(start), err)
	if err != nil {
		return nil, fmt.Errorf("shreds rewards epoch query: %w", err)
	}
	var epochColumns []uint64
	for epochRows.Next() {
		var e uint64
		if err := epochRows.Scan(&e); err != nil {
			epochRows.Close()
			return nil, fmt.Errorf("shreds rewards epoch scan: %w", err)
		}
		epochColumns = append(epochColumns, e)
	}
	epochRows.Close()

	// Current Solana epoch (from vote accounts) and latest finalized DZ epoch
	// (newest non-zero distribution).
	var currentSolanaEpochRaw int64
	if err := a.envDB(ctx).QueryRow(ctx,
		`SELECT coalesce(max(epoch), 0) FROM solana_vote_accounts_current`,
	).Scan(&currentSolanaEpochRaw); err != nil {
		logError("shreds rewards current epoch query failed", "error", err)
	}
	var currentSolanaEpoch uint64
	if currentSolanaEpochRaw > 0 {
		currentSolanaEpoch = uint64(currentSolanaEpochRaw)
	}
	var latestFinalized uint64
	if len(epochColumns) > 0 {
		latestFinalized = epochColumns[0]
	}

	// Per-leaf reward token: the journal that accumulated the leaf, recorded on
	// its status row's journal_mint_key; legacy/unknown leaves (no status row)
	// default to 2Z. The pool and per-token denominator are then joined per
	// (epoch, token), so each token's pool is split only over the validators who
	// picked it — not the epoch-wide total.
	leafMintExpr := fmt.Sprintf(`if(S.node_id != '' AND S.journal_mint_key != '', S.journal_mint_key, '%s')`, rewardMint2Z)
	poolMintExpr := fmt.Sprintf(`if(P.reward_mint != '', P.reward_mint, '%s')`, rewardMint2Z)
	earnedWholeList, tokenSymList := earnedWholeAndTokenSQL("lt.leader_slots", "lt.leaf_mint")

	// We compute the is_recent flag as a JOIN to the recent_epochs CTE rather
	// than via `IN (SELECT ...)` inside a sumIf/groupArrayIf: the ClickHouse
	// analyzer's CTE-column scoping breaks when a CTE is referenced from
	// inside an aggregate's filter expression.
	query := fmt.Sprintf(`
		WITH epoch_totals AS (
			SELECT subscription_epoch, sum(leader_slots) AS total_leader_slots
			FROM dim_dz_shred_validator_rewards_leaves_current
			GROUP BY subscription_epoch
		),
		recent_epochs AS (
			SELECT subscription_epoch AS solana_epoch
			FROM dim_dz_shred_distribution_2z_pool_current
			WHERE tokens_received_2z > 0
			GROUP BY subscription_epoch
			ORDER BY subscription_epoch DESC
			LIMIT 10
		),
		leaves_tok AS (
			SELECT
				L.node_id AS node_id,
				L.subscription_epoch AS subscription_epoch,
				L.leader_slots AS leader_slots,
				L.client_id AS client_id,
				%[1]s AS leaf_mint,
				coalesce(S.is_claimable, 0) AS is_claimable
			FROM dim_dz_shred_validator_rewards_leaves_current L
			LEFT JOIN dim_dz_shred_validator_leaf_distribution_status_current S
				ON S.subscription_epoch = L.subscription_epoch
			   AND S.node_id = L.node_id
			   AND S.client_id = L.client_id
		),
		earnings AS (
			SELECT
				lt.node_id AS node_id,
				lt.subscription_epoch AS subscription_epoch,
				lt.leaf_mint AS leaf_mint,
				-- token_symbol and earned carry a 2Z-equivalent fallback for
				-- claimable non-2Z leaves with no own-token pool yet; see
				-- earnedWholeAndTokenSQL.
				%[4]s AS token_symbol,
				%[2]s AS earned,
				lt.is_claimable AS is_claimable,
				if(R.solana_epoch IS NULL, 0, 1) AS is_recent
			FROM leaves_tok lt
			INNER JOIN dim_dz_shred_distribution_2z_pool_current P
				ON P.subscription_epoch = lt.subscription_epoch
			   AND %[3]s = lt.leaf_mint
			-- The epoch's 2Z pool, for the 2Z-equivalent fallback.
			LEFT JOIN dim_dz_shred_distribution_2z_pool_current P2Z
				ON P2Z.subscription_epoch = lt.subscription_epoch
			   AND P2Z.reward_mint = '%[5]s'
			INNER JOIN epoch_totals T
				ON T.subscription_epoch = lt.subscription_epoch
			LEFT JOIN dim_dz_shred_distribution_client_proportions_current C
				ON C.subscription_epoch = lt.subscription_epoch
			   AND C.client_id = lt.client_id
			LEFT JOIN recent_epochs R
				ON R.solana_epoch = lt.subscription_epoch
		),
		per_validator AS (
			SELECT
				node_id AS pv_node_id,
				-- Headline columns are 2Z-denominated. Sum by the DISPLAYED token
				-- (token_symbol = '2Z'), not leaf_mint, so a claimable non-2Z leaf
				-- valued via the 2Z-equivalent fallback (no own-token pool yet) is
				-- counted here too — otherwise its claimable amount shows as "—".
				-- Paid non-2Z leaves keep their own token and stay out of these.
				sumIf(coalesce(earned, 0), token_symbol = '2Z') AS total_earned_2z,
				sumIf(coalesce(earned, 0), is_claimable = 1 AND is_recent = 1 AND token_symbol = '2Z') AS immediately_claimable_2z,
				groupArrayIf(subscription_epoch, is_recent = 1) AS recent_dz_epochs,
				groupArrayIf(coalesce(earned, 0), is_recent = 1) AS recent_dz_earnings,
				groupArrayIf(token_symbol, is_recent = 1) AS recent_dz_tokens
			FROM earnings
			GROUP BY node_id
		)
		SELECT
			pv.pv_node_id AS node_id,
			coalesce(v.vote_pubkey, '') AS vote_pubkey,
			coalesce(va.name, '') AS validator_name,
			toUInt64(coalesce(v.activated_stake_lamports, 0)) AS activated_stake,
			coalesce(u.client_ip, '') AS dz_user_ip,
			pv.total_earned_2z AS total_earned_2z,
			pv.immediately_claimable_2z AS immediately_claimable_2z,
			pv.recent_dz_epochs AS recent_dz_epochs,
			pv.recent_dz_earnings AS recent_dz_earnings,
			pv.recent_dz_tokens AS recent_dz_tokens
		FROM per_validator pv
		LEFT JOIN solana_vote_accounts_current v
			ON v.node_pubkey = pv.pv_node_id AND v.epoch_vote_account = 'true'
		LEFT JOIN validatorsapp_validators_current va
			ON v.vote_pubkey = va.vote_account
		LEFT JOIN solana_gossip_nodes_current g
			ON g.pubkey = pv.pv_node_id
		LEFT JOIN dz_users_current u
			ON u.client_ip = g.gossip_ip AND u.status = 'activated'
		%[7]s
		ORDER BY %[8]s
		-- Collapse to one row per validator: the gossip/dz_users joins can
		-- fan out (e.g. multiple activated DZ users sharing a gossip IP),
		-- which would otherwise duplicate a validator in the list.
		LIMIT 1 BY node_id
		LIMIT %[9]d OFFSET %[10]d
	`, leafMintExpr, earnedWholeList, poolMintExpr,
		tokenSymList, rewardMint2Z, rewardMint2Z,
		whereClause, sortSQL, limit, offset)

	start = time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("shreds_rewards", time.Since(start), err)
	if err != nil {
		return nil, fmt.Errorf("shreds rewards query: %w", err)
	}
	defer rows.Close()

	validators := make([]ShredsRewardsRow, 0)
	for rows.Next() {
		var row ShredsRewardsRow
		var recentEpochs []uint64
		var recentEarnings []float64
		var recentTokens []string
		if err := rows.Scan(
			&row.NodeID,
			&row.VotePubkey,
			&row.ValidatorName,
			&row.ActivatedStake,
			&row.DZUserIP,
			&row.TotalEarned2Z,
			&row.ImmediatelyClaimable2Z,
			&recentEpochs,
			&recentEarnings,
			&recentTokens,
		); err != nil {
			return nil, fmt.Errorf("shreds rewards scan: %w", err)
		}
		row.EpochEarnings = make(map[uint64]float64, len(recentEpochs))
		row.EpochTokens = make(map[uint64]string, len(recentEpochs))
		for i, e := range recentEpochs {
			if i >= len(recentEarnings) {
				break
			}
			// If a node appears multiple times in an epoch (shouldn't happen
			// in practice — but if data is duplicated across rebuilds, sum).
			// A validator picks one token per epoch, so the symbol is consistent
			// across its leaves; record it alongside the summed amount.
			row.EpochEarnings[e] += recentEarnings[i]
			if i < len(recentTokens) {
				row.EpochTokens[e] = recentTokens[i]
			}
		}
		validators = append(validators, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("shreds rewards rows iteration: %w", err)
	}

	if epochColumns == nil {
		epochColumns = []uint64{}
	}

	// Total distinct validators matching the filter (before limit/offset), for
	// the client's page count. Mirrors the page query's population — validators
	// with at least one leaf in a funded (pooled) epoch, after the same search
	// joins/filter — and countDistinct collapses the gossip/dz_users fan-out the
	// way LIMIT 1 BY node_id does in the page query.
	countQuery := fmt.Sprintf(`
		WITH leaves_tok AS (
			SELECT L.node_id AS node_id, L.subscription_epoch AS subscription_epoch, %[1]s AS leaf_mint
			FROM dim_dz_shred_validator_rewards_leaves_current L
			LEFT JOIN dim_dz_shred_validator_leaf_distribution_status_current S
				ON S.subscription_epoch = L.subscription_epoch
			   AND S.node_id = L.node_id
			   AND S.client_id = L.client_id
		),
		per_validator AS (
			SELECT lt.node_id AS pv_node_id
			FROM leaves_tok lt
			INNER JOIN dim_dz_shred_distribution_2z_pool_current P
				ON P.subscription_epoch = lt.subscription_epoch
			   AND %[2]s = lt.leaf_mint
			GROUP BY lt.node_id
		)
		SELECT countDistinct(pv.pv_node_id)
		FROM per_validator pv
		LEFT JOIN solana_vote_accounts_current v
			ON v.node_pubkey = pv.pv_node_id AND v.epoch_vote_account = 'true'
		LEFT JOIN validatorsapp_validators_current va
			ON v.vote_pubkey = va.vote_account
		LEFT JOIN solana_gossip_nodes_current g
			ON g.pubkey = pv.pv_node_id
		LEFT JOIN dz_users_current u
			ON u.client_ip = g.gossip_ip AND u.status = 'activated'
		%[3]s
	`, leafMintExpr, poolMintExpr, whereClause)
	var total uint64
	cstart := time.Now()
	cerr := a.envDB(ctx).QueryRow(ctx, countQuery, args...).Scan(&total)
	metrics.RecordClickHouseQuery("shreds_rewards:count", time.Since(cstart), cerr)
	if cerr != nil {
		return nil, fmt.Errorf("shreds rewards count query: %w", cerr)
	}

	return &ShredsRewardsResponse{
		CurrentSolanaEpoch:   currentSolanaEpoch,
		LatestFinalizedEpoch: latestFinalized,
		EpochColumns:         epochColumns,
		Validators:           validators,
		Total:                int(total),
	}, nil
}

// FetchShredsRewardsData computes the default-params shreds rewards response
// for the page-cache refresh worker. Returns the same payload the handler
// would produce for an unparameterized GET /api/dz/shreds/rewards request.
func (a *API) FetchShredsRewardsData(ctx context.Context) (*ShredsRewardsResponse, error) {
	// Compute directly (NOT via GetShredsRewards) so the refresh worker never
	// reads the page cache it is responsible for populating.
	return a.computeShredsRewards(ctx, "", "", "", shredsRewardsDefaultLimit, 0)
}

// Claim-state values for ShredsRewardsEpochDetail.State. These let the page
// distinguish "already paid out" from "not finalized yet" — both of which
// previously surfaced as a bare null is_claimable.
const (
	// ClaimStateClaimable: the leaf is accumulated and not yet distributed —
	// the validator can claim it now (publisher bit set on its status row).
	ClaimStateClaimable = "claimable"
	// ClaimStateDistributed: the reward for this leaf has already been paid
	// out. This is the ONLY positive proof of payment: a status row exists for
	// the (epoch, node, client) leaf and its publisher bit has been cleared.
	// The bit is set when a leaf is accumulated and cleared on distribution,
	// and the indexer freezes a leaf's final bit when its journal is swept — so
	// a cleared bit, whether the journal is still live or long swept, means the
	// reward was distributed.
	ClaimStateDistributed = "distributed"
	// ClaimStatePending: not claimable yet — either the distribution isn't
	// finalized (no funded pool), or the leaf hasn't been accumulated yet while
	// the epoch's journal is still live.
	ClaimStatePending = "pending"
	// ClaimStateUnknown: the epoch's pool is funded but we have no per-validator
	// status row for the leaf and the journal is no longer live, so we cannot
	// prove whether it was claimed. This is the unrecoverable history before the
	// indexer began tracking the publisher bitmap — once a journal was swept
	// without ever being captured, its per-leaf state is gone for good. Epochs
	// indexed while their journal was live keep a real claimable/distributed
	// state after the sweep, so this only ever applies to that backfill gap.
	ClaimStateUnknown = "unknown"
)

// ShredsRewardsEpochDetail is a single epoch row in the per-validator drilldown.
//
// State is the derived lifecycle of the leaf (see ClaimState* constants) and is
// always set. IsClaimable is retained for back-compat: it is non-nil only when
// a live journal row exists for the leaf (true → claimable, false → a live row
// with the bit cleared); it is nil for the distributed and pending states,
// where there is no live journal bit to report.
type ShredsRewardsEpochDetail struct {
	SolanaEpoch       uint64 `json:"solana_epoch"`
	SubscriptionEpoch uint64 `json:"subscription_epoch"`
	LeaderSlots       uint32 `json:"leader_slots"`
	ClientID          uint16 `json:"client_id"`
	// Earned is the reward in whole units of TokenSymbol (the token the
	// validator chose for the epoch: 2Z, USDC, or wSOL).
	Earned      float64 `json:"earned"`
	TokenSymbol string  `json:"token_symbol"`
	IsClaimable *bool   `json:"is_claimable,omitempty"`
	State       string  `json:"state"`
}

// ShredsRewardsDetailResponse is the response body for the per-validator
// drilldown endpoint. Epochs is newest first.
type ShredsRewardsDetailResponse struct {
	NodeID         string                     `json:"node_id"`
	VotePubkey     string                     `json:"vote_pubkey"`
	ValidatorName  string                     `json:"validator_name"`
	ActivatedStake uint64                     `json:"activated_stake"`
	DZUserIP       string                     `json:"dz_user_ip"`
	Epochs         []ShredsRewardsEpochDetail `json:"epochs"`
}

// GetShredsRewardsDetail returns the full per-epoch reward history for a
// single validator (node_id from the URL). Unlike the list endpoint, this
// is not limited to the most-recent 10 epochs; every epoch with a row in
// dim_dz_shred_validator_rewards_leaves_current is returned. The per-row
// is_claimable field is null when no status row exists (epoch outside the
// on-chain tracking window).
func (a *API) GetShredsRewardsDetail(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	nodeID := chi.URLParam(r, "nodeId")

	resp := ShredsRewardsDetailResponse{
		NodeID: nodeID,
		Epochs: []ShredsRewardsEpochDetail{},
	}

	// Header: vote pubkey, validator name, activated stake, DZ user IP.
	// Same join shape as the list endpoint. A single-row anchor SELECT lets
	// the LEFT JOINs return zero values for unknown node_ids without erroring.
	const headerQuery = `
		SELECT
			coalesce(v.vote_pubkey, ''),
			coalesce(va.name, ''),
			toUInt64(coalesce(v.activated_stake_lamports, 0)),
			coalesce(u.client_ip, '')
		FROM (SELECT 1 AS x) one
		LEFT JOIN solana_vote_accounts_current v
			ON v.node_pubkey = ? AND v.epoch_vote_account = 'true'
		LEFT JOIN validatorsapp_validators_current va
			ON v.vote_pubkey = va.vote_account
		LEFT JOIN solana_gossip_nodes_current g
			ON g.pubkey = ?
		LEFT JOIN dz_users_current u
			ON u.client_ip = g.gossip_ip AND u.status = 'activated'
		LIMIT 1
	`
	if err := a.envDB(ctx).QueryRow(ctx, headerQuery, nodeID, nodeID).Scan(
		&resp.VotePubkey, &resp.ValidatorName, &resp.ActivatedStake, &resp.DZUserIP,
	); err != nil && !errors.Is(err, sql.ErrNoRows) {
		// Tolerate missing rows; leave fields zero-valued.
		logError("shreds rewards detail header scan failed", "error", err)
	}

	// Per-epoch rows. Mirrors the list query's earnings logic but restricted to
	// a single node_id and returning every epoch (not just the last 10). The
	// per-leaf reward token comes from its status row's journal_mint_key
	// (defaulting to 2Z for legacy/unknown leaves), and the pool/denominator are
	// joined per (epoch, token); see the list query for the formula.
	leafMintExpr := fmt.Sprintf(`if(S.node_id != '' AND S.journal_mint_key != '', S.journal_mint_key, '%s')`, rewardMint2Z)
	poolMintExpr := fmt.Sprintf(`if(P.reward_mint != '', P.reward_mint, '%s')`, rewardMint2Z)
	earnedWhole, tokenSym := earnedWholeAndTokenSQL("L.leader_slots", leafMintExpr)
	detailQuery := fmt.Sprintf(`
		WITH epoch_totals AS (
			SELECT subscription_epoch, sum(leader_slots) AS total_leader_slots
			FROM dim_dz_shred_validator_rewards_leaves_current
			GROUP BY subscription_epoch
		),
		-- Epochs whose distribution journal is still live on-chain (i.e. we have
		-- accumulation-status rows for them). Used to tell "already distributed"
		-- (funded pool, journal swept → no rows) apart from "not yet accumulated"
		-- (journal still live).
		journal_live_epochs AS (
			SELECT subscription_epoch, 1 AS jl_live
			FROM dim_dz_shred_validator_leaf_distribution_status_current
			GROUP BY subscription_epoch
		)
		SELECT
			-- subscription_epoch numerically equals the Solana epoch (the
			-- shred-subscription program creates one ShredDistribution per
			-- Solana epoch starting from its launch epoch). We return both
			-- the same value as the Solana epoch and the actual stored
			-- subscription_epoch column for clarity.
			L.subscription_epoch AS solana_epoch,
			L.subscription_epoch,
			L.leader_slots,
			L.client_id,
			-- token_symbol and earned carry a 2Z-equivalent fallback: a claimable
			-- USDC/wSOL leaf has no own-token pool yet (distributed_amount = 0), so
			-- it is valued against the epoch's 2Z pool and labelled 2Z. See
			-- earnedWholeAndTokenSQL.
			%[3]s AS token_symbol,
			%[4]s AS earned,
			-- has_status: ClickHouse LEFT JOINs default missing values rather than
			-- emitting NULL, so we detect "no status row" by checking that the
			-- joined node_id is the empty string (the String default).
			if(S.node_id = '' OR S.node_id IS NULL, 0, 1) AS has_status,
			coalesce(S.is_claimable, 0) AS is_claimable,
			-- Derived claim state. Order matters (first match wins):
			--   status row, bit set   -> claimable (accumulated, not yet distributed)
			--   status row, bit clear -> distributed (accumulated then paid; the
			--                            publisher bit is cleared on distribution
			--                            and a status row only exists once the leaf
			--                            has been accumulated, so a cleared bit is
			--                            positive proof the reward was paid out.
			--                            The indexer freezes this bit when the
			--                            journal is swept, so it holds for old
			--                            epochs too. For non-2Z tokens the per-leaf
			--                            sweep is not yet tracked, so a distributed
			--                            leaf may linger as claimable.)
			--   epoch has status rows -> pending (this leaf not yet accumulated
			--                            while the journal is still live)
			--   pool not funded       -> pending (distribution not finalized)
			--   funded, no status row -> unknown (journal swept before we ever
			--                            tracked it; per-leaf state is gone — we
			--                            must NOT assume it was paid)
			multiIf(
				S.node_id != '' AND S.is_claimable = 1, 'claimable',
				S.node_id != '', 'distributed',
				JL.jl_live = 1, 'pending',
				P.tokens_received_2z = 0, 'pending',
				'unknown'
			) AS state
		FROM dim_dz_shred_validator_rewards_leaves_current L
		LEFT JOIN dim_dz_shred_validator_leaf_distribution_status_current S
			ON S.subscription_epoch = L.subscription_epoch
		   AND S.node_id = L.node_id
		   AND S.client_id = L.client_id
		INNER JOIN dim_dz_shred_distribution_2z_pool_current P
			ON P.subscription_epoch = L.subscription_epoch
		   AND %[1]s = %[2]s
		-- The epoch's 2Z pool, for the 2Z-equivalent fallback when the leaf's own
		-- token has no pool yet.
		LEFT JOIN dim_dz_shred_distribution_2z_pool_current P2Z
			ON P2Z.subscription_epoch = L.subscription_epoch
		   AND P2Z.reward_mint = '%[5]s'
		INNER JOIN epoch_totals T
			ON T.subscription_epoch = L.subscription_epoch
		LEFT JOIN dim_dz_shred_distribution_client_proportions_current C
			ON C.subscription_epoch = L.subscription_epoch
		   AND C.client_id = L.client_id
		LEFT JOIN journal_live_epochs JL
			ON JL.subscription_epoch = L.subscription_epoch
		WHERE L.node_id = ?
		ORDER BY L.subscription_epoch DESC
	`, poolMintExpr, leafMintExpr, tokenSym, earnedWhole, rewardMint2Z)
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, detailQuery, nodeID)
	metrics.RecordClickHouseQuery("shreds_rewards_detail", time.Since(start), err)
	if err != nil {
		logError("shreds rewards detail query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var ed ShredsRewardsEpochDetail
		var hasStatus uint8
		var isClaimable uint8
		if err := rows.Scan(
			&ed.SolanaEpoch, &ed.SubscriptionEpoch, &ed.LeaderSlots, &ed.ClientID,
			&ed.TokenSymbol, &ed.Earned, &hasStatus, &isClaimable, &ed.State,
		); err != nil {
			logError("shreds rewards detail scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		// IsClaimable stays non-nil only when a live journal row exists, for
		// back-compat; State carries the full lifecycle (incl. distributed).
		if hasStatus == 1 {
			b := isClaimable == 1
			ed.IsClaimable = &b
		}
		resp.Epochs = append(resp.Epochs, ed)
	}
	if err := rows.Err(); err != nil {
		logError("shreds rewards detail rows iteration failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode shreds rewards detail", "error", err)
	}
}
