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

	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
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
	PublishingRoot          bool   `json:"publishing_root"`
	LeaderSlots             uint64 `json:"leader_slots"`
	RootSlots               uint64 `json:"root_slots"`
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

// GetPublisherCheck returns publisher status for all publishers in the current epoch,
// optionally filtered by IP address or DZ user pubkey.
// isDefaultPublisherCheckRequest returns true if the request uses default parameters
// (no filter, epochs=2, no slots), meaning it can be served from cache.
func isDefaultPublisherCheckRequest(r *http.Request) bool {
	q := r.URL.Query()
	if q.Get("q") != "" {
		return false
	}
	if e := q.Get("epochs"); e != "" && e != "2" {
		return false
	}
	if q.Get("slots") != "" {
		return false
	}
	return true
}

func (a *API) GetPublisherCheck(w http.ResponseWriter, r *http.Request) {
	// Try to serve from cache for default requests
	if isMainnet(r.Context()) && isDefaultPublisherCheckRequest(r) {
		if data, err := a.readPageCache(r.Context(), "publisher_check"); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	w.Header().Set("X-Cache", "MISS")
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	q := strings.TrimSpace(r.URL.Query().Get("q"))

	epochsParam := 2 // default: current + previous epoch
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

	resp, err := a.FetchPublisherCheckData(ctx, q, epochsParam, slotsParam)
	if err != nil && dberror.IsTransient(err) {
		cancel()
		var retryCancel context.CancelFunc
		ctx, retryCancel = context.WithTimeout(r.Context(), 20*time.Second)
		defer retryCancel()
		resp, err = a.FetchPublisherCheckData(ctx, q, epochsParam, slotsParam)
	}

	if err != nil {
		slog.Warn("publisher check failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
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

	// rootPublisherFeed is the feed value the shredder writes to publisher_shred_stats for
	// shreds a validator publishes as the turbine-tree root (the earliest DZ hop). It only
	// produces rows once the shredder enables `publisher_stats: true` on the edge-solana-root
	// source; until then root_slots reads zero for every publisher. Kept as a named constant
	// so it's trivial to realign if the shredder uses a different feed string.
	const rootPublisherFeed = "edge-solana-root"

	var perSlotWhere string
	var args []any
	// rootStatsCTE counts, per publisher, the distinct slots that carried a turbine-root
	// publish in the same epoch/slot window as the leader stats. Its epoch/slot bounds are
	// inlined (validated ints) to avoid disturbing the positional `?` arg ordering below.
	var rootStatsCTE string
	if slotsParam > 0 {
		perSlotWhere = `WHERE ` + leaderFeedFilter + `
			AND epoch >= (SELECT epoch FROM current_epoch) - 1
			AND slot >= (SELECT max(slot) FROM ` + shredStatsTable + ` WHERE ` + leaderFeedFilter + ` AND epoch >= (SELECT epoch FROM current_epoch) - 1) - ?`
		args = []any{slotsParam, shredGroupPK}
		rootStatsCTE = fmt.Sprintf(`,
		root_stats AS (
			SELECT dz_user_pubkey, uniqExact(slot) AS root_slots
			FROM %[1]s
			WHERE feed = '%[2]s'
				AND epoch >= (SELECT epoch FROM current_epoch) - 1
				AND slot >= (SELECT max(slot) FROM %[1]s WHERE %[3]s AND epoch >= (SELECT epoch FROM current_epoch) - 1) - %[4]d
			GROUP BY dz_user_pubkey
		)`, shredStatsTable, rootPublisherFeed, leaderFeedFilter, slotsParam)
	} else {
		perSlotWhere = `WHERE ` + leaderFeedFilter + ` AND epoch >= (SELECT epoch FROM current_epoch) - ? + 1`
		args = []any{epochsParam, shredGroupPK}
		rootStatsCTE = fmt.Sprintf(`,
		root_stats AS (
			SELECT dz_user_pubkey, uniqExact(slot) AS root_slots
			FROM %[1]s
			WHERE feed = '%[2]s' AND epoch >= (SELECT epoch FROM current_epoch) - %[3]d + 1
			GROUP BY dz_user_pubkey
		)`, shredStatsTable, rootPublisherFeed, epochsParam)
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
		)%s
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
			COALESCE(rs.root_slots, 0) AS root_slots,
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
		LEFT JOIN root_stats rs ON u.pk = rs.dz_user_pubkey
		LEFT JOIN validatorsapp_validators_current va ON v.vote_pubkey = va.vote_account
		WHERE u.status = 'activated'
			AND has(JSONExtract(u.publishers, 'Array(String)'), ?)
	`, shredStatsTable, shredStatsTable, perSlotWhere, rootStatsCTE)
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

	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery("publisher_check", duration, err)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var epoch uint64
	var maxSlot uint64
	var publishers []PublisherCheckItem

	for rows.Next() {
		var p PublisherCheckItem
		var totalSlots, leaderSlots, retransmitSlots, rootSlots uint64
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
			&rootSlots,
			&p.TotalUniqueShreds,
			&p.SlotsNeedingRepair,
			&rowEpoch,
			&rowMaxSlot,
			&p.ValidatorClient,
			&p.ValidatorVersion,
			&p.ValidatorName,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
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
		p.RootSlots = rootSlots
		p.MulticastConnected = true // All rows are bebop group members
		p.PublishingLeaderShreds = leaderSlots > 0
		p.PublishingRoot = rootSlots > 0
		p.PublishingRetransmitted = totalSlots > 0 &&
			retransmitSlots >= retransmitMinSlots &&
			float64(retransmitSlots)/float64(totalSlots) >= retransmitMinRatio
		p.ValidatorVersionOk = isValidatorVersionOk(p.ValidatorClient, p.ValidatorVersion)
		p.IsBackup = p.NodePubkey != "" && p.VotePubkey == ""

		publishers = append(publishers, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	if publishers == nil {
		publishers = []PublisherCheckItem{}
	}

	var totalNetworkStake int64
	err = a.envDB(ctx).QueryRow(ctx,
		`SELECT COALESCE(SUM(activated_stake_lamports), 0)
		 FROM solana_vote_accounts_current
		 WHERE epoch_vote_account = 'true' AND activated_stake_lamports > 0`).Scan(&totalNetworkStake)
	if err != nil {
		slog.Warn("publisher check: total network stake query failed", "error", err)
	}

	var totalPublishers uint64
	var totalPublisherStake int64
	err = a.envDB(ctx).QueryRow(ctx,
		`SELECT count(), COALESCE(sum(v.activated_stake_lamports), 0)
		 FROM dz_users_current u
		 LEFT JOIN solana_gossip_nodes_current g ON u.client_ip = g.gossip_ip AND u.client_ip != ''
		 LEFT JOIN solana_vote_accounts_current v ON g.pubkey = v.node_pubkey AND v.epoch_vote_account = 'true'
		 WHERE u.status = 'activated'
		   AND JSONLength(u.publishers) > 0
		   AND v.vote_pubkey != ''
		   AND g.pubkey != ''`).Scan(&totalPublishers, &totalPublisherStake)
	if err != nil {
		slog.Warn("publisher check: total publishers query failed", "error", err)
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
