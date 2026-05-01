package handlers

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

type GeoConcentrationHeroStats struct {
	ValidatorsMeasured   int     `json:"validators_measured"`
	StakeTopTwoMetrosPct float64 `json:"stake_top_two_metros_pct"`
	AnchorPoints         int     `json:"anchor_points"`
	StakeMaxASNPct       float64 `json:"stake_max_asn_pct"`
}

type GeoConcentrationMetro struct {
	MetroCode  string  `json:"metro_code"`
	Validators int     `json:"validators"`
	StakeSol   float64 `json:"stake_sol"`
	StakePct   float64 `json:"stake_pct"`
}

type GeoConcentrationCountry struct {
	CountryCode string  `json:"country_code"`
	CountryName string  `json:"country_name"`
	Validators  int     `json:"validators"`
	StakeSol    float64 `json:"stake_sol"`
	StakePct    float64 `json:"stake_pct"`
}

type GeoConcentrationASN struct {
	ASN        int64   `json:"asn"`
	ASNOrg     string  `json:"asn_org"`
	Validators int     `json:"validators"`
	StakeSol   float64 `json:"stake_sol"`
	StakePct   float64 `json:"stake_pct"`
}

type GeoConcentrationResponse struct {
	HeroStats GeoConcentrationHeroStats `json:"hero_stats"`
	Metros    []GeoConcentrationMetro   `json:"metros"`
	Countries []GeoConcentrationCountry `json:"countries"`
	ASNs      []GeoConcentrationASN     `json:"asns"`
}

func (a *API) GetGeoConcentration(w http.ResponseWriter, r *http.Request) {
	// Cache check for mainnet default requests
	if isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), "geo_concentration"); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	w.Header().Set("X-Cache", "MISS")

	resp, err := a.FetchGeoConcentrationData(r.Context())
	if err != nil {
		logError("geo concentration query error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode response", "error", err)
	}
}

func (a *API) FetchGeoConcentrationData(ctx context.Context) (*GeoConcentrationResponse, error) {
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
				va.vote_pubkey AS vote_pubkey,
				va.activated_stake_lamports / 1e9 AS stake_sol,
				coalesce(geo.asn, 0) AS asn,
				coalesce(geo.asn_org, '') AS asn_org,
				coalesce(geo.country_code, '') AS country_code,
				coalesce(geo.country, '') AS country_name
			FROM geolocated gv
			JOIN solana_vote_accounts_current va ON gv.node_pubkey = va.node_pubkey
			LEFT JOIN geoip_records_current geo ON gv.target_ip = geo.ip
			WHERE va.epoch_vote_account = 'true' AND va.activated_stake_lamports > 0
		),
		nearest_metro AS (
			SELECT
				e.vote_pubkey AS vote_pubkey,
				e.stake_sol AS stake_sol,
				e.asn AS asn,
				e.asn_org AS asn_org,
				e.country_code AS country_code,
				e.country_name AS country_name,
				arrayElement(
					arraySort(
						(x, y) -> y,
						groupArray(m.code),
						groupArray(sqrt(pow(e.dzdp_lat - m.latitude, 2) + pow(e.dzdp_lng - m.longitude, 2)))
					), 1
				) AS metro_code
			FROM enriched e
			CROSS JOIN dz_metros_current m
			GROUP BY vote_pubkey, stake_sol, asn, asn_org, country_code, country_name
		),
		deduped AS (
			SELECT
				vote_pubkey,
				max(stake_sol) AS max_stake,
				argMax(metro_code, stake_sol) AS metro_code,
				argMax(asn, stake_sol) AS asn,
				argMax(asn_org, stake_sol) AS asn_org,
				argMax(country_code, stake_sol) AS country_code,
				argMax(country_name, stake_sol) AS country_name
			FROM nearest_metro
			GROUP BY vote_pubkey
		)
		SELECT vote_pubkey, max_stake AS stake_sol, metro_code, asn, asn_org, country_code, country_name
		FROM deduped
	`

	start := time.Now()
	rows, err := a.DB.Query(ctx, query)
	metrics.RecordClickHouseQuery(time.Since(start), err)
	if err != nil {
		return nil, err
	}

	// Collect per-validator rows and aggregate in Go
	type validatorRow struct {
		votePubkey  string
		stakeSol    float64
		metroCode   string
		asn         int64
		asnOrg      string
		countryCode string
		countryName string
	}

	var validators []validatorRow
	for rows.Next() {
		var v validatorRow
		if err := rows.Scan(&v.votePubkey, &v.stakeSol, &v.metroCode, &v.asn, &v.asnOrg, &v.countryCode, &v.countryName); err != nil {
			rows.Close()
			return nil, err
		}
		validators = append(validators, v)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	// Compute total stake
	var totalStake float64
	for _, v := range validators {
		totalStake += v.stakeSol
	}

	// Aggregate by metro
	metroMap := make(map[string]*GeoConcentrationMetro)
	for _, v := range validators {
		m, ok := metroMap[v.metroCode]
		if !ok {
			m = &GeoConcentrationMetro{MetroCode: v.metroCode}
			metroMap[v.metroCode] = m
		}
		m.Validators++
		m.StakeSol += v.stakeSol
	}
	metros := make([]GeoConcentrationMetro, 0, len(metroMap))
	for _, m := range metroMap {
		if totalStake > 0 {
			m.StakePct = math.Round(m.StakeSol/totalStake*10000) / 100
		}
		metros = append(metros, *m)
	}
	sort.Slice(metros, func(i, j int) bool { return metros[i].StakeSol > metros[j].StakeSol })

	// Aggregate by country
	type countryKey struct{ code, name string }
	countryMap := make(map[countryKey]*GeoConcentrationCountry)
	for _, v := range validators {
		ck := countryKey{v.countryCode, v.countryName}
		c, ok := countryMap[ck]
		if !ok {
			c = &GeoConcentrationCountry{CountryCode: v.countryCode, CountryName: v.countryName}
			countryMap[ck] = c
		}
		c.Validators++
		c.StakeSol += v.stakeSol
	}
	countries := make([]GeoConcentrationCountry, 0, len(countryMap))
	for _, c := range countryMap {
		if totalStake > 0 {
			c.StakePct = math.Round(c.StakeSol/totalStake*10000) / 100
		}
		countries = append(countries, *c)
	}
	sort.Slice(countries, func(i, j int) bool { return countries[i].StakeSol > countries[j].StakeSol })

	// Aggregate by ASN
	type asnKey struct {
		asn int64
		org string
	}
	asnMap := make(map[asnKey]*GeoConcentrationASN)
	for _, v := range validators {
		ak := asnKey{v.asn, v.asnOrg}
		entry, ok := asnMap[ak]
		if !ok {
			entry = &GeoConcentrationASN{ASN: v.asn, ASNOrg: v.asnOrg}
			asnMap[ak] = entry
		}
		entry.Validators++
		entry.StakeSol += v.stakeSol
	}
	asns := make([]GeoConcentrationASN, 0, len(asnMap))
	for _, entry := range asnMap {
		if totalStake > 0 {
			entry.StakePct = math.Round(entry.StakeSol/totalStake*10000) / 100
		}
		asns = append(asns, *entry)
	}
	sort.Slice(asns, func(i, j int) bool { return asns[i].StakeSol > asns[j].StakeSol })

	// Compute hero stats
	var stakeTopTwo float64
	for i := 0; i < len(metros) && i < 2; i++ {
		stakeTopTwo += metros[i].StakePct
	}

	// Count anchor points (distinct DZ metros)
	var anchorPoints uint64
	anchorRows, err := a.DB.Query(ctx, "SELECT count() FROM dz_metros_current")
	if err != nil {
		logError("geo concentration anchor points query error", "error", err)
	} else {
		if anchorRows.Next() {
			_ = anchorRows.Scan(&anchorPoints)
		}
		anchorRows.Close()
	}

	var maxASNPct float64
	if len(asns) > 0 {
		maxASNPct = asns[0].StakePct
	}

	// Round percentages
	stakeTopTwo = math.Round(stakeTopTwo*100) / 100
	maxASNPct = math.Round(maxASNPct*100) / 100

	if metros == nil {
		metros = []GeoConcentrationMetro{}
	}
	if countries == nil {
		countries = []GeoConcentrationCountry{}
	}
	if asns == nil {
		asns = []GeoConcentrationASN{}
	}

	resp := &GeoConcentrationResponse{
		HeroStats: GeoConcentrationHeroStats{
			ValidatorsMeasured:   len(validators),
			StakeTopTwoMetrosPct: stakeTopTwo,
			AnchorPoints:         int(anchorPoints),
			StakeMaxASNPct:       maxASNPct,
		},
		Metros:    metros,
		Countries: countries,
		ASNs:      asns,
	}

	return resp, nil
}
