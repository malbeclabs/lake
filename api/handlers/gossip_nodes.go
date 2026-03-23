package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/metrics"
)

type GossipNodeListItem struct {
	Pubkey      string  `json:"pubkey"`
	GossipIP    string  `json:"gossip_ip"`
	GossipPort  int32   `json:"gossip_port"`
	Version     string  `json:"version"`
	City        string  `json:"city"`
	Country     string  `json:"country"`
	OnDZ        bool    `json:"on_dz"`
	DeviceCode  string  `json:"device_code"`
	MetroCode   string  `json:"metro_code"`
	StakeSol    float64 `json:"stake_sol"`
	IsValidator bool    `json:"is_validator"`
}

type GossipNodeListResponse struct {
	Items          []GossipNodeListItem `json:"items"`
	Total          int                  `json:"total"`
	OnDZCount      int                  `json:"on_dz_count"`
	ValidatorCount int                  `json:"validator_count"`
	Limit          int                  `json:"limit"`
	Offset         int                  `json:"offset"`
}

var gossipNodeSortFields = map[string]string{
	"pubkey":    "pubkey",
	"ip":        "gossip_ip",
	"version":   "version",
	"city":      "city",
	"country":   "country",
	"validator": "is_validator",
	"stake":     "stake_sol",
	"dz":        "on_dz",
	"device":    "device_code",
}

var gossipNodeFilterFields = map[string]FilterFieldConfig{
	"pubkey":    {Column: "pubkey", Type: FieldTypeText},
	"ip":        {Column: "gossip_ip", Type: FieldTypeText},
	"version":   {Column: "version", Type: FieldTypeText},
	"city":      {Column: "city", Type: FieldTypeText},
	"country":   {Column: "country", Type: FieldTypeText},
	"validator": {Column: "is_validator", Type: FieldTypeBoolean},
	"stake":     {Column: "stake_sol", Type: FieldTypeStake},
	"dz":        {Column: "on_dz", Type: FieldTypeBoolean},
	"device":    {Column: "device_code", Type: FieldTypeText},
}

func GetGossipNodes(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "stake", gossipNodeSortFields)
	filters := ParseFilters(r)
	start := time.Now()

	// Build filter clause
	filterClause, filterArgs := filters.BuildFilterClause(gossipNodeFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}

	// Single query using window functions for counts to avoid repeating expensive CTEs.
	orderBy := sort.OrderByClause(gossipNodeSortFields)

	query := `
		WITH dz_nodes AS (
			SELECT
				u.client_ip,
				any(d.code) as device_code,
				any(m.code) as metro_code
			FROM dz_users_current u
			JOIN dz_devices_current d ON u.device_pk = d.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			WHERE u.status = 'activated'
				AND u.client_ip IS NOT NULL
				AND u.client_ip != ''
			GROUP BY u.client_ip
		),
		validator_stake AS (
			SELECT
				node_pubkey,
				activated_stake_lamports / 1e9 as stake_sol
			FROM solana_vote_accounts_current
			WHERE epoch_vote_account = 'true'
		),
		gossip_data AS (
			SELECT
				g.pubkey,
				COALESCE(g.gossip_ip, '') as gossip_ip,
				COALESCE(g.gossip_port, 0) as gossip_port,
				COALESCE(g.version, '') as version,
				COALESCE(geo.city, '') as city,
				COALESCE(geo.country, '') as country,
				dz.client_ip != '' as on_dz,
				COALESCE(dz.device_code, '') as device_code,
				COALESCE(dz.metro_code, '') as metro_code,
				COALESCE(vs.stake_sol, 0) as stake_sol,
				vs.node_pubkey != '' as is_validator
			FROM solana_gossip_nodes_current g
			LEFT JOIN geoip_records_current geo ON g.gossip_ip = geo.ip
			LEFT JOIN dz_nodes dz ON g.gossip_ip = dz.client_ip
			LEFT JOIN validator_stake vs ON g.pubkey = vs.node_pubkey
		)
		SELECT pubkey, gossip_ip, gossip_port, version, city, country,
			on_dz, device_code, metro_code, stake_sol, is_validator,
			count() OVER () as _total,
			countIf(on_dz = true) OVER () as _on_dz_count,
			countIf(is_validator = true) OVER () as _validator_count
		FROM gossip_data
		WHERE 1=1` + whereFilter + `
		` + orderBy + `
		LIMIT ? OFFSET ?
	`

	queryArgs := append(filterArgs, pagination.Limit, pagination.Offset)
	rows, err := envDB(ctx).Query(ctx, query, queryArgs...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		slog.Error("gossip nodes query failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var nodes []GossipNodeListItem
	var total, onDZCount, validatorCount uint64
	for rows.Next() {
		var n GossipNodeListItem
		if err := rows.Scan(
			&n.Pubkey,
			&n.GossipIP,
			&n.GossipPort,
			&n.Version,
			&n.City,
			&n.Country,
			&n.OnDZ,
			&n.DeviceCode,
			&n.MetroCode,
			&n.StakeSol,
			&n.IsValidator,
			&total,
			&onDZCount,
			&validatorCount,
		); err != nil {
			slog.Error("gossip nodes row scan failed", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		nodes = append(nodes, n)
	}

	if err := rows.Err(); err != nil {
		slog.Error("gossip nodes rows iteration failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null
	if nodes == nil {
		nodes = []GossipNodeListItem{}
	}

	response := GossipNodeListResponse{
		Items:          nodes,
		Total:          int(total),
		OnDZCount:      int(onDZCount),
		ValidatorCount: int(validatorCount),
		Limit:          pagination.Limit,
		Offset:         pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

type GossipNodeDetail struct {
	Pubkey      string  `json:"pubkey"`
	GossipIP    string  `json:"gossip_ip"`
	GossipPort  int32   `json:"gossip_port"`
	Version     string  `json:"version"`
	City        string  `json:"city"`
	Country     string  `json:"country"`
	OnDZ        bool    `json:"on_dz"`
	UserPK      string  `json:"user_pk"`
	OwnerPubkey string  `json:"owner_pubkey"`
	DevicePK    string  `json:"device_pk"`
	DeviceCode  string  `json:"device_code"`
	MetroPK     string  `json:"metro_pk"`
	MetroCode   string  `json:"metro_code"`
	StakeSol    float64 `json:"stake_sol"`
	IsValidator bool    `json:"is_validator"`
	VotePubkey  string  `json:"vote_pubkey"`
}

func GetGossipNode(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pubkey := chi.URLParam(r, "pubkey")
	if pubkey == "" {
		http.Error(w, "missing pubkey", http.StatusBadRequest)
		return
	}

	start := time.Now()
	query := `
		WITH dz_nodes AS (
			SELECT
				u.client_ip,
				any(u.pk) as user_pk,
				any(u.owner_pubkey) as owner_pubkey,
				any(u.device_pk) as device_pk,
				any(d.code) as device_code,
				any(d.metro_pk) as metro_pk,
				any(m.code) as metro_code
			FROM dz_users_current u
			JOIN dz_devices_current d ON u.device_pk = d.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			WHERE u.status = 'activated'
				AND u.client_ip IS NOT NULL
				AND u.client_ip != ''
			GROUP BY u.client_ip
		),
		validator_stake AS (
			SELECT
				node_pubkey,
				vote_pubkey,
				activated_stake_lamports / 1e9 as stake_sol
			FROM solana_vote_accounts_current
			WHERE epoch_vote_account = 'true'
		)
		SELECT
			g.pubkey,
			COALESCE(g.gossip_ip, '') as gossip_ip,
			COALESCE(g.gossip_port, 0) as gossip_port,
			COALESCE(g.version, '') as version,
			COALESCE(geo.city, '') as city,
			COALESCE(geo.country, '') as country,
			dz.client_ip != '' as on_dz,
			COALESCE(dz.user_pk, '') as user_pk,
			COALESCE(dz.owner_pubkey, '') as owner_pubkey,
			COALESCE(dz.device_pk, '') as device_pk,
			COALESCE(dz.device_code, '') as device_code,
			COALESCE(dz.metro_pk, '') as metro_pk,
			COALESCE(dz.metro_code, '') as metro_code,
			COALESCE(vs.stake_sol, 0) as stake_sol,
			vs.node_pubkey != '' as is_validator,
			COALESCE(vs.vote_pubkey, '') as vote_pubkey
		FROM solana_gossip_nodes_current g
		LEFT JOIN geoip_records_current geo ON g.gossip_ip = geo.ip
		LEFT JOIN dz_nodes dz ON g.gossip_ip = dz.client_ip
		LEFT JOIN validator_stake vs ON g.pubkey = vs.node_pubkey
		WHERE g.pubkey = ?
	`

	var node GossipNodeDetail
	err := envDB(ctx).QueryRow(ctx, query, pubkey).Scan(
		&node.Pubkey,
		&node.GossipIP,
		&node.GossipPort,
		&node.Version,
		&node.City,
		&node.Country,
		&node.OnDZ,
		&node.UserPK,
		&node.OwnerPubkey,
		&node.DevicePK,
		&node.DeviceCode,
		&node.MetroPK,
		&node.MetroCode,
		&node.StakeSol,
		&node.IsValidator,
		&node.VotePubkey,
	)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		slog.Error("gossip node query failed", "error", err, "pubkey", pubkey)
		http.Error(w, "gossip node not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(node); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}
