package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/mod/semver"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/metrics"
)

// minValidatorVersions defines the minimum acceptable version per client type.
// Update these via PRs when new minimum versions are required.
var minValidatorVersions = map[string]string{
	"agave":      "2.1.0",
	"jito":       "2.1.0",
	"firedancer": "0.1.0",
}

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
	ValidatorVersionOk      bool   `json:"validator_version_ok"`
	IsBackup                bool   `json:"is_backup"`
}

// PublisherCheckResponse is the response for the publisher check endpoint.
type PublisherCheckResponse struct {
	Epoch      uint64               `json:"epoch"`
	Publishers []PublisherCheckItem `json:"publishers"`
}

// GetPublisherCheck returns publisher status for all publishers in the current epoch,
// optionally filtered by IP address or DZ user pubkey.
func GetPublisherCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	q := strings.TrimSpace(r.URL.Query().Get("q"))

	epochsParam := 2 // default: current + previous epoch
	if e := r.URL.Query().Get("epochs"); e != "" {
		if parsed, err := strconv.Atoi(e); err == nil && parsed >= 1 && parsed <= 10 {
			epochsParam = parsed
		}
	}

	start := time.Now()

	// Look up the bebop multicast group PK.
	// The group PK is needed to filter dz_users_current.publishers JSON array.
	var bebopPK string
	err := envDB(ctx).QueryRow(ctx,
		`SELECT pk FROM dz_multicast_groups_current WHERE code = 'bebop' LIMIT 1`).Scan(&bebopPK)
	if err != nil {
		log.Printf("PublisherCheck: bebop group not found: %v", err)
		writeJSON(w, PublisherCheckResponse{Publishers: []PublisherCheckItem{}})
		return
	}

	// Build query: start from all bebop publishers (dz_users_current),
	// LEFT JOIN shred stats and validator info.
	shredStatsTable := fmt.Sprintf("`%s`.publisher_shred_stats", config.ShredderDB)
	query := fmt.Sprintf(`
		WITH current_epoch AS (
			SELECT max(epoch) AS epoch FROM %s
		),
		stats AS (
			SELECT
				dz_user_pubkey,
				max(activated_stake) AS activated_stake,
				count(*) AS total_slots,
				countIf(is_scheduled_leader = true) AS leader_slots,
				countIf(is_scheduled_leader = false) AS retransmit_slots,
				sum(unique_shreds) AS total_unique_shreds,
				countIf(needs_repair = true) AS slots_needing_repair
			FROM %s
			WHERE epoch >= (SELECT epoch FROM current_epoch) - ? + 1
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
			if(va.software_client != '', va.software_client, '') AS validator_client,
			if(va.software_version != '', va.software_version, COALESCE(g.version, '')) AS validator_version
		FROM dz_users_current u
		LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN solana_gossip_nodes_current g ON u.client_ip = g.gossip_ip AND u.client_ip != ''
		LEFT JOIN solana_vote_accounts_current v ON g.pubkey = v.node_pubkey AND v.epoch_vote_account = 'true'
		LEFT JOIN stats s ON u.pk = s.dz_user_pubkey
		LEFT JOIN validatorsapp_validators_current va ON v.vote_pubkey = va.vote_account
		WHERE u.status = 'activated'
			AND has(JSONExtract(u.publishers, 'Array(String)'), ?)
	`, shredStatsTable, shredStatsTable)

	// Query args: epochsParam for stats CTE, then bebopPK for has() filter
	args := []any{epochsParam, bebopPK}
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

	rows, err := envDB(ctx).Query(ctx, query, args...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("PublisherCheck query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var epoch uint64
	var publishers []PublisherCheckItem

	for rows.Next() {
		var p PublisherCheckItem
		var totalSlots, leaderSlots, retransmitSlots uint64
		var stakeRaw int64
		var rowEpoch uint64

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
			&p.ValidatorClient,
			&p.ValidatorVersion,
		); err != nil {
			log.Printf("PublisherCheck scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if rowEpoch > epoch {
			epoch = rowEpoch
		}
		if stakeRaw > 0 {
			p.ActivatedStake = uint64(stakeRaw)
		}
		p.TotalSlots = totalSlots
		p.LeaderSlots = leaderSlots
		p.MulticastConnected = true // All rows are bebop group members
		p.PublishingLeaderShreds = leaderSlots > 0
		p.PublishingRetransmitted = retransmitSlots > 0
		p.ValidatorVersionOk = isValidatorVersionOk(p.ValidatorClient, p.ValidatorVersion)
		p.IsBackup = p.NodePubkey != "" && p.VotePubkey == ""

		publishers = append(publishers, p)
	}

	if err := rows.Err(); err != nil {
		log.Printf("PublisherCheck rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if publishers == nil {
		publishers = []PublisherCheckItem{}
	}

	resp := PublisherCheckResponse{
		Epoch:      epoch,
		Publishers: publishers,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("PublisherCheck JSON encoding error: %v", err)
	}
}
