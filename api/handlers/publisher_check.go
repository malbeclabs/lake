package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/mod/semver"
	"golang.org/x/sync/errgroup"

	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

// minValidatorVersions defines the minimum acceptable version per client type.
// Update these via PRs when new minimum versions are required.
var minValidatorVersions = map[string]string{
	"agave":      "2.1.0",
	"jito":       "2.1.0",
	"firedancer": "0.1.0",
}

const (
	retransmitMinSlots uint64  = 50
	retransmitMinRatio float64 = 0.05
)

// isValidatorVersionOk checks if the version meets the minimum for the client type.
func isValidatorVersionOk(clientName, clientVersion string) bool {
	if clientVersion == "" {
		return false
	}

	key := strings.ToLower(clientName)
	minVersion, ok := minValidatorVersions[key]
	if !ok {
		return false
	}

	// semver requires "v" prefix
	return semver.Compare("v"+clientVersion, "v"+minVersion) >= 0
}

// PublisherCheckItem represents a single publisher's status.
type PublisherCheckItem struct {
	PublisherIP             string `json:"publisher_ip"`
	ClientIP                string `json:"client_ip"`
	NodePubkey              string `json:"node_pubkey"`
	VotePubkey              string `json:"vote_pubkey"`
	DZUserPubkey            string `json:"dz_user_pubkey"`
	DZDeviceCode            string `json:"dz_device_code"`
	DZMetroCode             string `json:"dz_metro_code"`
	ActivatedStake          uint64 `json:"activated_stake"`
	MulticastConnected      bool   `json:"multicast_connected"`
	PublishingLeaderShreds  bool   `json:"publishing_leader_shreds"`
	PublishingRetransmitted bool   `json:"publishing_retransmitted"`
	LeaderSlots             uint64 `json:"leader_slots"`
	TotalSlots              uint64 `json:"total_slots"`
	TotalUniqueShreds       uint64 `json:"total_unique_shreds"`
	SlotsNeedingRepair      uint64 `json:"slots_needing_repair"`
	ValidatorClient         string `json:"validator_client"`
	ValidatorVersion        string `json:"validator_version"`
	ValidatorName           string `json:"validator_name"`
	ValidatorVersionOk      bool   `json:"validator_version_ok"`
	IsBackup                bool   `json:"is_backup"`
}

// PublisherCheckResponse is the response for the publisher check endpoint.
type PublisherCheckResponse struct {
	Epoch               uint64               `json:"epoch"`
	MaxSlot             uint64               `json:"max_slot"`
	TotalNetworkStake   int64                `json:"total_network_stake"`
	TotalPublishers     uint64               `json:"total_publishers"`
	TotalPublisherStake int64                `json:"total_publisher_stake"`
	Publishers          []PublisherCheckItem `json:"publishers"`
}

const (
	// DefaultPublisherCheckEpochs is the default recent-epoch window and the only
	// shape backed by the page cache. Single source of truth for the default shape;
	// the worker's cache-warming entry and huma's `default` tag on the v1 endpoint
	// must match it (they can't reference this const from a struct tag).
	DefaultPublisherCheckEpochs = 2

	// publisherCheckLiveTimeout bounds a single live (uncached) publisher-check
	// attempt. Matches the REST handler's per-attempt budget.
	publisherCheckLiveTimeout = 20 * time.Second

	// publisherCheckStaleAfter caps how old a cached publisher_check payload may be
	// before the cached-or-live path ignores it and runs live. The page-cache worker
	// deliberately keeps its last payload on failure, so without this cap a stalled
	// worker could serve arbitrarily old data with no signal.
	//
	// It does NOT bound the worst healthy age the way ValidatorsCacheStaleAfter does:
	// that is publisherCheckInterval plus one cycle period plus the refresh itself,
	// which exceeds this cap once PAGE_CACHE_REFRESH_INTERVAL is above ~2.5 min
	// (staging runs ~4 min). A default-shape request then goes live, which is the
	// heaviest recurring query on the shared ClickHouse. Raising the cap is a
	// staleness-vs-load call for this specific view, so it is left alone here.
	publisherCheckStaleAfter = 6 * time.Minute

	// maxConcurrentPublisherCheckLive bounds simultaneous live runs of the heavy
	// publisher-check query across all callers. The per-IP rate-limit key is
	// client-controlled (X-Forwarded-For), so it can't be the aggregate DoS bound;
	// this server-side cap does not depend on client input.
	maxConcurrentPublisherCheckLive = 4
)

// isDefaultPublisherCheckShape reports whether the given parameters match the
// cached default (no filter, default epochs, no slots). Single source of truth for
// "is this the cacheable shape", used by both the REST and v1 entry points.
func isDefaultPublisherCheckShape(q string, epochsParam, slotsParam int) bool {
	return q == "" && epochsParam == DefaultPublisherCheckEpochs && slotsParam == 0
}

// FetchPublisherCheckCachedOrLive returns publisher-check data, serving the
// default-shape mainnet request from the page cache when a fresh payload exists
// and otherwise running the query live under publisherCheckLiveTimeout. It lets
// uncapped callers (e.g. the v1 edge endpoint) avoid an unbounded live run of the
// heavy query on every request while reusing the same cache the REST handler serves.
func (a *API) FetchPublisherCheckCachedOrLive(ctx context.Context, q string, epochsParam, slotsParam int) (*PublisherCheckResponse, error) {
	resp, _, err := a.fetchPublisherCheckCachedOrLive(ctx, q, epochsParam, slotsParam, a.FetchPublisherCheckData)
	return resp, err
}

// fetchPublisherCheckCachedOrLive is the shared cache-or-live core for both the
// REST handler and the v1 endpoint (fetch is injected so tests can observe the
// live path). It returns fromCache so the REST handler can set X-Cache. A cached
// default-shape payload is served only when mainnet, parseable, and fresher than
// publisherCheckStaleAfter; otherwise it runs live.
func (a *API) fetchPublisherCheckCachedOrLive(
	ctx context.Context, q string, epochsParam, slotsParam int,
	fetch func(context.Context, string, int, int) (*PublisherCheckResponse, error),
) (resp *PublisherCheckResponse, fromCache bool, err error) {
	q = strings.TrimSpace(q)

	if isMainnet(ctx) && isDefaultPublisherCheckShape(q, epochsParam, slotsParam) {
		// Bound the cache read too; it must not run on the caller's unbounded ctx.
		readCtx, cancel := context.WithTimeout(ctx, publisherCheckLiveTimeout)
		data, updatedAt, rerr := a.readPageCacheWithAge(readCtx, "publisher_check")
		cancel()
		if rerr == nil {
			if age := time.Since(updatedAt); age > publisherCheckStaleAfter {
				slog.Warn("publisher check: cached payload stale, running live",
					"age", age.Round(time.Second), "max_age", publisherCheckStaleAfter)
			} else {
				var cached PublisherCheckResponse
				if uerr := json.Unmarshal(data, &cached); uerr != nil {
					// A populated but unparseable cache entry indicates payload drift;
					// fall through to a live query but surface it so it's diagnosable.
					slog.Warn("publisher check: cached payload unmarshal failed", "error", uerr)
				} else {
					return &cached, true, nil
				}
			}
		}
	}

	resp, err = a.fetchPublisherCheckLive(ctx, q, epochsParam, slotsParam, fetch)
	return resp, false, err
}

// fetchPublisherCheckLive runs the heavy query under a bounded per-attempt
// deadline, retrying once on transient failure with a fresh budget. Concurrent
// identical default-shape misses are collapsed via singleflight (one query serves
// all), and every live run is gated by a server-side concurrency semaphore.
func (a *API) fetchPublisherCheckLive(
	ctx context.Context, q string, epochsParam, slotsParam int,
	fetch func(context.Context, string, int, int) (*PublisherCheckResponse, error),
) (*PublisherCheckResponse, error) {
	// Filtered shapes are varied and low-volume; run them directly under the
	// caller's context. Only the default shape stampedes (a Postgres outage sends
	// every default request live at once), so collapse just those.
	if !isDefaultPublisherCheckShape(q, epochsParam, slotsParam) {
		return a.fetchPublisherCheckLiveGuarded(ctx, q, epochsParam, slotsParam, fetch)
	}

	// The collapsed run must not be tied to the winning caller's context: with a
	// plain singleflight.Do the shared query would inherit the winner's ctx, so one
	// caller's disconnect (or earlier deadline) would cancel the query and 500 every
	// collapsed waiter — exactly the stampede this path protects against. Run the
	// shared fetch on a context detached from the winner (WithoutCancel keeps the
	// env value envDB routes on; the guarded run adds its own deadline so it can't
	// hold a semaphore slot forever), and use DoChan so each caller selects on its
	// own ctx and a disconnecting caller returns promptly without failing the rest.
	key := string(EnvFromContext(ctx))
	ch := a.pubCheckSF.DoChan(key, func() (any, error) {
		return a.fetchPublisherCheckLiveGuarded(context.WithoutCancel(ctx), q, epochsParam, slotsParam, fetch)
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*PublisherCheckResponse), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *API) fetchPublisherCheckLiveGuarded(
	ctx context.Context, q string, epochsParam, slotsParam int,
	fetch func(context.Context, string, int, int) (*PublisherCheckResponse, error),
) (*PublisherCheckResponse, error) {
	// Bound the semaphore wait even when the caller's context has no deadline
	// (REST/huma requests may not) — otherwise a burst of distinct live requests
	// parks goroutines indefinitely. The per-attempt deadline below only applies
	// after acquisition, so it can't bound the wait itself.
	acqCtx, cancelAcq := context.WithTimeout(ctx, publisherCheckLiveTimeout)
	defer cancelAcq()

	sem := a.publisherCheckLiveSem()
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-acqCtx.Done():
		return nil, acqCtx.Err()
	}

	attempt := func() (*PublisherCheckResponse, error) {
		actx, cancel := context.WithTimeout(ctx, publisherCheckLiveTimeout)
		defer cancel()
		return fetch(actx, q, epochsParam, slotsParam)
	}

	resp, err := attempt()
	if err != nil && dberror.IsTransient(err) && ctx.Err() == nil {
		resp, err = attempt()
	}
	return resp, err
}

func (a *API) GetPublisherCheck(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))

	epochsParam := DefaultPublisherCheckEpochs // current + previous epoch
	if e := r.URL.Query().Get("epochs"); e != "" {
		if parsed, err := strconv.Atoi(e); err == nil && parsed >= 1 && parsed <= 10 {
			epochsParam = parsed
		}
	}

	var slotsParam int
	if s := r.URL.Query().Get("slots"); s != "" {
		if parsed, err := strconv.Atoi(s); err == nil {
			switch parsed {
			case 100, 500, 1000, 5000:
				slotsParam = parsed
			default:
				slotsParam = 500
			}
		} else {
			slotsParam = 500
		}
	}

	resp, fromCache, err := a.fetchPublisherCheckCachedOrLive(r.Context(), q, epochsParam, slotsParam, a.FetchPublisherCheckData)
	if err != nil {
		slog.Warn("publisher check failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if fromCache {
		w.Header().Set("X-Cache", "HIT")
	} else {
		w.Header().Set("X-Cache", "MISS")
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// FetchPublisherCheckData performs the actual publisher check query.
func (a *API) FetchPublisherCheckData(ctx context.Context, q string, epochsParam, slotsParam int) (*PublisherCheckResponse, error) {
	start := time.Now()

	shredGroupPK := ShredGroupPK

	shredStatsTable := fmt.Sprintf("`%s`.publisher_shred_stats", a.PublisherDB)

	// Only the dz source has `publisher_stats: true` in the shredder configs,
	// so filtering to it preserves pre-per-feed-column numbers exactly. Pre-migration
	// rows have feed='' (the ALTER added the column without a default), so include
	// both to keep history continuous across the cutover.
	const leaderFeedFilter = "feed IN ('', 'dz')"

	var perSlotWhere string
	var args []any
	if slotsParam > 0 {
		perSlotWhere = `WHERE ` + leaderFeedFilter + `
			AND epoch >= (SELECT epoch FROM current_epoch) - 1
			AND slot >= (SELECT max(slot) FROM ` + shredStatsTable + ` WHERE ` + leaderFeedFilter + ` AND epoch >= (SELECT epoch FROM current_epoch) - 1) - ?`
		args = []any{slotsParam, shredGroupPK}
	} else {
		perSlotWhere = `WHERE ` + leaderFeedFilter + ` AND epoch >= (SELECT epoch FROM current_epoch) - ? + 1`
		args = []any{epochsParam, shredGroupPK}
	}

	query := fmt.Sprintf(`
		WITH current_epoch AS (
			SELECT max(epoch) AS epoch FROM %s
		),
		per_slot AS (
			SELECT
				dz_user_pubkey,
				slot,
				max(activated_stake) AS activated_stake,
				max(is_scheduled_leader) AS is_scheduled_leader,
				max(unique_shreds) AS unique_shreds,
				max(needs_repair) AS needs_repair
			FROM %s
			%s
			GROUP BY dz_user_pubkey, slot
		),
		stats AS (
			SELECT
				dz_user_pubkey,
				max(activated_stake) AS activated_stake,
				count() AS total_slots,
				countIf(is_scheduled_leader = true) AS leader_slots,
				countIf(is_scheduled_leader = false) AS retransmit_slots,
				sum(unique_shreds) AS total_unique_shreds,
				countIf(needs_repair = true) AS slots_needing_repair,
				max(slot) AS max_slot
			FROM per_slot
			GROUP BY dz_user_pubkey
		)
		SELECT
			u.dz_ip AS publisher_ip,
			u.client_ip,
			COALESCE(g.pubkey, '') AS node_pubkey,
			COALESCE(v.vote_pubkey, '') AS vote_pubkey,
			u.pk AS dz_user_pubkey,
			COALESCE(d.code, '') AS dz_device_code,
			COALESCE(m.code, '') AS dz_metro_code,
			COALESCE(v.activated_stake_lamports, 0) AS activated_stake,
			COALESCE(s.total_slots, 0) AS total_slots,
			COALESCE(s.leader_slots, 0) AS leader_slots,
			COALESCE(s.retransmit_slots, 0) AS retransmit_slots,
			COALESCE(s.total_unique_shreds, 0) AS total_unique_shreds,
			COALESCE(s.slots_needing_repair, 0) AS slots_needing_repair,
			(SELECT epoch FROM current_epoch) AS epoch,
			COALESCE(s.max_slot, 0) AS max_slot,
			if(va.software_client != '', va.software_client, '') AS validator_client,
			if(va.software_version != '', va.software_version, COALESCE(g.version, '')) AS validator_version,
			COALESCE(va.name, '') AS validator_name
		FROM dz_users_current u
		LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN solana_gossip_nodes_current g ON u.client_ip = g.gossip_ip AND u.client_ip != ''
		LEFT JOIN solana_vote_accounts_current v ON g.pubkey = v.node_pubkey AND v.epoch_vote_account = 'true'
		LEFT JOIN stats s ON u.pk = s.dz_user_pubkey
		LEFT JOIN validatorsapp_validators_current va ON v.vote_pubkey = va.vote_account
		WHERE u.status = 'activated'
			AND has(JSONExtract(u.publishers, 'Array(String)'), ?)
	`, shredStatsTable, shredStatsTable, perSlotWhere)
	if q != "" {
		if strings.Contains(q, ".") {
			query += " AND (u.dz_ip = ? OR u.client_ip = ?)"
			args = append(args, q, q)
		} else {
			query += " AND u.pk = ?"
			args = append(args, q)
		}
	}

	query += " ORDER BY activated_stake DESC, publisher_ip"

	// The main publisher query and the two totals queries are independent, so run
	// them concurrently under one context. Previously all three ran sequentially
	// under a single budget, so a slow main query starved the totals; propagating
	// the totals error (rather than warn-and-continue) then turned that starvation
	// into a 500. Running them together removes that ordering dependency.
	//
	// Totals errors are propagated rather than warn-and-continued: a failure would
	// otherwise return a 200 with silently-zeroed totals. Callers handle the error —
	// the REST handler and v1 edge endpoint retry transient failures then 500, and
	// the page-cache worker keeps its last complete payload.
	var (
		epoch, maxSlot      uint64
		publishers          []PublisherCheckItem
		totalNetworkStake   int64
		totalPublishers     uint64
		totalPublisherStake int64
	)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		rows, err := a.envDB(gctx).Query(gctx, query, args...)
		metrics.RecordClickHouseQuery("publisher_check", time.Since(start), err)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var p PublisherCheckItem
			var totalSlots, leaderSlots, retransmitSlots uint64
			var stakeRaw int64
			var rowEpoch uint64
			var rowMaxSlot uint64

			if err := rows.Scan(
				&p.PublisherIP,
				&p.ClientIP,
				&p.NodePubkey,
				&p.VotePubkey,
				&p.DZUserPubkey,
				&p.DZDeviceCode,
				&p.DZMetroCode,
				&stakeRaw,
				&totalSlots,
				&leaderSlots,
				&retransmitSlots,
				&p.TotalUniqueShreds,
				&p.SlotsNeedingRepair,
				&rowEpoch,
				&rowMaxSlot,
				&p.ValidatorClient,
				&p.ValidatorVersion,
				&p.ValidatorName,
			); err != nil {
				return fmt.Errorf("scan: %w", err)
			}

			if rowEpoch > epoch {
				epoch = rowEpoch
			}
			if rowMaxSlot > maxSlot {
				maxSlot = rowMaxSlot
			}
			if stakeRaw > 0 {
				p.ActivatedStake = uint64(stakeRaw)
			}
			p.TotalSlots = totalSlots
			p.LeaderSlots = leaderSlots
			p.MulticastConnected = true // All rows are bebop group members
			p.PublishingLeaderShreds = leaderSlots > 0
			p.PublishingRetransmitted = totalSlots > 0 &&
				retransmitSlots >= retransmitMinSlots &&
				float64(retransmitSlots)/float64(totalSlots) >= retransmitMinRatio
			p.ValidatorVersionOk = isValidatorVersionOk(p.ValidatorClient, p.ValidatorVersion)
			p.IsBackup = p.NodePubkey != "" && p.VotePubkey == ""

			publishers = append(publishers, p)
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("rows: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		if err := a.envDB(gctx).QueryRow(gctx,
			`SELECT COALESCE(SUM(activated_stake_lamports), 0)
			 FROM solana_vote_accounts_current
			 WHERE epoch_vote_account = 'true' AND activated_stake_lamports > 0`).Scan(&totalNetworkStake); err != nil {
			return fmt.Errorf("total network stake: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		if err := a.envDB(gctx).QueryRow(gctx,
			`SELECT count(), COALESCE(sum(v.activated_stake_lamports), 0)
			 FROM dz_users_current u
			 LEFT JOIN solana_gossip_nodes_current g ON u.client_ip = g.gossip_ip AND u.client_ip != ''
			 LEFT JOIN solana_vote_accounts_current v ON g.pubkey = v.node_pubkey AND v.epoch_vote_account = 'true'
			 WHERE u.status = 'activated'
			   AND JSONLength(u.publishers) > 0
			   AND v.vote_pubkey != ''
			   AND g.pubkey != ''`).Scan(&totalPublishers, &totalPublisherStake); err != nil {
			return fmt.Errorf("total publishers: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if publishers == nil {
		publishers = []PublisherCheckItem{}
	}

	return &PublisherCheckResponse{
		Epoch:               epoch,
		MaxSlot:             maxSlot,
		TotalNetworkStake:   totalNetworkStake,
		TotalPublishers:     totalPublishers,
		TotalPublisherStake: totalPublisherStake,
		Publishers:          publishers,
	}, nil
}
