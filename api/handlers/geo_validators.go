package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

type GeoValidatorItem struct {
	VotePubkey  string  `json:"vote_pubkey"`
	NodePubkey  string  `json:"node_pubkey"`
	Name        string  `json:"name"`
	StakeSol    float64 `json:"stake_sol"`
	StakePct    float64 `json:"stake_pct"`
	Commission  int64   `json:"commission"`
	MetroCode   string  `json:"metro_code"`
	CountryCode string  `json:"country_code"`
	ASN         int64   `json:"asn"`
	ASNOrg      string  `json:"asn_org"`
	Datacenter  string  `json:"datacenter"`
	IsDZ        bool    `json:"is_dz"`
	DZDPLat     float64 `json:"dzdp_lat"`
	DZDPLng     float64 `json:"dzdp_lng"`
}

type GeoTierDistribution struct {
	Tier       string  `json:"tier"`
	Validators int     `json:"validators"`
	StakePct   float64 `json:"stake_pct"`
}

type GeoMetroBreakdown struct {
	MetroCode  string  `json:"metro_code"`
	Validators int     `json:"validators"`
	StakeSol   float64 `json:"stake_sol"`
	StakePct   float64 `json:"stake_pct"`
}

type GeoValidatorsResponse struct {
	TotalValidators  int                   `json:"total_validators"`
	TotalStakeSol    float64               `json:"total_stake_sol"`
	Validators       []GeoValidatorItem    `json:"validators"`
	TierDistribution []GeoTierDistribution `json:"tier_distribution"`
	MetroBreakdown   []GeoMetroBreakdown   `json:"metro_breakdown"`
}

func (a *API) GetGeoValidators(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) && isDefaultGeoValidatorsRequest(r) {
		if data, err := a.readPageCache(r.Context(), "geo_validators"); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	w.Header().Set("X-Cache", "MISS")

	metro := strings.TrimSpace(r.URL.Query().Get("metro"))
	dzFilter := strings.TrimSpace(r.URL.Query().Get("dz_filter"))

	resp, err := a.FetchGeoValidatorsData(r.Context(), metro, dzFilter)
	if err != nil {
		logError("geo validators query error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode response", "error", err)
	}
}

func isDefaultGeoValidatorsRequest(r *http.Request) bool {
	q := r.URL.Query()
	return q.Get("metro") == "" && q.Get("dz_filter") == ""
}

func (a *API) FetchGeoValidatorsData(ctx context.Context, metro, dzFilter string) (*GeoValidatorsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	query := `
		WITH geolocated AS (
			SELECT
				ls.target_ip AS target_ip,
				ls.lat AS dzdp_lat,
				ls.lng AS dzdp_lng,
				gn.pubkey AS node_pubkey
			FROM (SELECT * FROM dzdp.location_state FINAL) AS ls
			JOIN solana_gossip_nodes_current gn ON ls.target_ip = gn.gossip_ip
			WHERE ls.state = 'decided'
		),
		enriched AS (
			SELECT
				gv.target_ip AS target_ip,
				gv.dzdp_lat AS dzdp_lat,
				gv.dzdp_lng AS dzdp_lng,
				gv.node_pubkey AS node_pubkey,
				va.vote_pubkey AS vote_pubkey,
				va.activated_stake_lamports / 1e9 AS stake_sol,
				va.commission_percentage AS commission,
				coalesce(geo.asn, 0) AS asn,
				coalesce(geo.asn_org, '') AS asn_org,
				coalesce(geo.country_code, '') AS country_code,
				coalesce(vapp.name, '') AS vname,
				coalesce(vapp.data_center_key, '') AS datacenter,
				if(vapp.is_dz = 1, true, false) AS is_dz
			FROM geolocated gv
			JOIN solana_vote_accounts_current va ON gv.node_pubkey = va.node_pubkey
			LEFT JOIN geoip_records_current geo ON gv.target_ip = geo.ip
			LEFT JOIN validatorsapp_validators_current vapp ON va.vote_pubkey = vapp.vote_account
			WHERE va.epoch_vote_account = 'true' AND va.activated_stake_lamports > 0
		),
		nearest_metro AS (
			SELECT
				e.vote_pubkey AS vote_pubkey,
				e.node_pubkey AS node_pubkey,
				e.stake_sol AS stake_sol,
				e.commission AS commission,
				e.asn AS asn,
				e.asn_org AS asn_org,
				e.country_code AS country_code,
				e.vname AS vname,
				e.datacenter AS datacenter,
				e.is_dz AS is_dz,
				e.dzdp_lat AS dzdp_lat,
				e.dzdp_lng AS dzdp_lng,
				arrayElement(
					arraySort(
						(x, y) -> y,
						groupArray(m.code),
						groupArray(sqrt(pow(e.dzdp_lat - m.latitude, 2) + pow(e.dzdp_lng - m.longitude, 2)))
					), 1
				) AS metro_code
			FROM enriched e
			CROSS JOIN dz_metros_current m
			GROUP BY vote_pubkey, node_pubkey, stake_sol, commission,
				asn, asn_org, country_code, vname, datacenter, is_dz,
				dzdp_lat, dzdp_lng
		),
		deduped AS (
			SELECT
				vote_pubkey,
				argMax(node_pubkey, stake_sol) AS node_pubkey,
				max(stake_sol) AS max_stake,
				argMax(commission, stake_sol) AS commission,
				argMax(metro_code, stake_sol) AS metro_code,
				argMax(asn, stake_sol) AS asn,
				argMax(asn_org, stake_sol) AS asn_org,
				argMax(country_code, stake_sol) AS country_code,
				argMax(vname, stake_sol) AS vname,
				argMax(datacenter, stake_sol) AS datacenter,
				argMax(is_dz, stake_sol) AS is_dz,
				argMax(dzdp_lat, stake_sol) AS dzdp_lat,
				argMax(dzdp_lng, stake_sol) AS dzdp_lng
			FROM nearest_metro
			GROUP BY vote_pubkey
		)
		SELECT vote_pubkey, node_pubkey, max_stake AS stake_sol, commission, metro_code, asn, asn_org,
			country_code, vname, datacenter, is_dz, dzdp_lat, dzdp_lng
		FROM deduped
		ORDER BY max_stake DESC
	`

	start := time.Now()
	rows, err := a.DB.Query(ctx, query)
	metrics.RecordClickHouseQuery(time.Since(start), err)
	if err != nil {
		// Return empty response when DZDP tables aren't available
		var chErr *proto.Exception
		if errors.As(err, &chErr) && chErr.Code == 60 {
			return &GeoValidatorsResponse{
				Validators:       []GeoValidatorItem{},
				TierDistribution: []GeoTierDistribution{},
				MetroBreakdown:   []GeoMetroBreakdown{},
			}, nil
		}
		return nil, err
	}
	defer rows.Close()

	var allValidators []GeoValidatorItem
	for rows.Next() {
		var v GeoValidatorItem
		if err := rows.Scan(&v.VotePubkey, &v.NodePubkey, &v.StakeSol, &v.Commission,
			&v.MetroCode, &v.ASN, &v.ASNOrg, &v.CountryCode, &v.Name, &v.Datacenter,
			&v.IsDZ, &v.DZDPLat, &v.DZDPLng); err != nil {
			return nil, err
		}
		allValidators = append(allValidators, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Apply filters
	filtered := make([]GeoValidatorItem, 0, len(allValidators))
	for _, v := range allValidators {
		if metro != "" && !strings.EqualFold(v.MetroCode, metro) {
			continue
		}
		if dzFilter == "on" && !v.IsDZ {
			continue
		}
		if dzFilter == "off" && v.IsDZ {
			continue
		}
		filtered = append(filtered, v)
	}

	// Compute totals from all filtered validators
	var totalStake float64
	for _, v := range filtered {
		totalStake += v.StakeSol
	}

	// Compute stake_pct for each validator
	for i := range filtered {
		if totalStake > 0 {
			filtered[i].StakePct = math.Round(filtered[i].StakeSol/totalStake*10000) / 100
		}
	}

	// Tier distribution: validators are already sorted by stake DESC from the query.
	// "super" = cumulative stake before this validator < 33.3%
	// "high"  = cumulative stake before this validator < 66.6%
	// "mid"   = rest
	tierStake := map[string]float64{}
	tierCount := map[string]int{}
	var cumStake float64
	for _, v := range filtered {
		var tier string
		if totalStake > 0 && cumStake/totalStake < 0.333 {
			tier = "super"
		} else if totalStake > 0 && cumStake/totalStake < 0.666 {
			tier = "high"
		} else {
			tier = "mid"
		}
		cumStake += v.StakeSol
		tierStake[tier] += v.StakeSol
		tierCount[tier]++
	}
	tierDist := make([]GeoTierDistribution, 0, 3)
	for _, tier := range []string{"super", "high", "mid"} {
		pct := 0.0
		if totalStake > 0 {
			pct = math.Round(tierStake[tier]/totalStake*10000) / 100
		}
		tierDist = append(tierDist, GeoTierDistribution{
			Tier:       tier,
			Validators: tierCount[tier],
			StakePct:   pct,
		})
	}

	// Metro breakdown from all filtered validators
	metroMap := make(map[string]*GeoMetroBreakdown)
	for _, v := range filtered {
		mb, ok := metroMap[v.MetroCode]
		if !ok {
			mb = &GeoMetroBreakdown{MetroCode: v.MetroCode}
			metroMap[v.MetroCode] = mb
		}
		mb.Validators++
		mb.StakeSol += v.StakeSol
	}
	metroBreakdown := make([]GeoMetroBreakdown, 0, len(metroMap))
	for _, mb := range metroMap {
		if totalStake > 0 {
			mb.StakePct = math.Round(mb.StakeSol/totalStake*10000) / 100
		}
		metroBreakdown = append(metroBreakdown, *mb)
	}
	sort.Slice(metroBreakdown, func(i, j int) bool { return metroBreakdown[i].StakeSol > metroBreakdown[j].StakeSol })

	// Top 30 validators for response
	top := filtered
	if len(top) > 30 {
		top = top[:30]
	}

	resp := &GeoValidatorsResponse{
		TotalValidators:  len(filtered),
		TotalStakeSol:    totalStake,
		Validators:       top,
		TierDistribution: tierDist,
		MetroBreakdown:   metroBreakdown,
	}

	// Ensure nil slices are empty arrays in JSON
	if resp.Validators == nil {
		resp.Validators = []GeoValidatorItem{}
	}
	if resp.MetroBreakdown == nil {
		resp.MetroBreakdown = []GeoMetroBreakdown{}
	}

	return resp, nil
}
