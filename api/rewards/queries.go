package rewards

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Cache for validator demand results to avoid re-querying ClickHouse on every poll.
var demandCache struct {
	sync.Mutex
	demands []Demand
	fetched time.Time
}

const demandCacheTTL = 5 * time.Minute

func getCachedDemand() []Demand {
	demandCache.Lock()
	defer demandCache.Unlock()
	if time.Since(demandCache.fetched) < demandCacheTTL && len(demandCache.demands) > 0 {
		return demandCache.demands
	}
	return nil
}

func setCachedDemand(d []Demand) {
	demandCache.Lock()
	defer demandCache.Unlock()
	demandCache.demands = d
	demandCache.fetched = time.Now()
}

// Pseudo-operator names used by the Shapley computation.
const (
	operatorOthers  = "Others"
	operatorPublic  = "Public"
	operatorPrivate = "Private"
)

// Shapley tuning parameters.
const (
	DefaultOperatorUptime   = 0.98
	DefaultContiguityBonus  = 5.0
	DefaultDemandMultiplier = 1.0
	defaultLinkUptime       = 0.99
	maxSyntheticDemands     = 10 // cap synthetic demands for tractability
	maxDemandMetros         = 10 // cap validator demand metros for shapley-cli tractability

	// Constants matching doublezero-offchain/crates/contributor-rewards/src/calculator/constants.rs
	slotsInEpoch  = 432000.0
	demandTraffic = 0.05
)

// Haversine / latency estimation constants.
const (
	earthRadiusKm   = 6371
	latencyPerKm    = 0.01 // ms per km of fiber
	latencyOverhead = 1.2  // 20% overhead for routing hops
	latencyFloorMs  = 5.0  // minimum latency between any two cities
	latencyFallback = 100.0
)

// LiveNetworkResponse is the response from FetchLiveNetwork.
type LiveNetworkResponse struct {
	Network       ShapleyInput `json:"network"`
	DeviceCount   int          `json:"device_count"`
	LinkCount     int          `json:"link_count"`
	OperatorCount int          `json:"operator_count"`
	MetroCount    int          `json:"metro_count"`
}

// estimatePublicLatency estimates public internet latency between two cities using haversine.
func estimatePublicLatency(lat1, lng1, lat2, lng2 float64) float64 {
	rlat1 := lat1 * math.Pi / 180
	rlng1 := lng1 * math.Pi / 180
	rlat2 := lat2 * math.Pi / 180
	rlng2 := lng2 * math.Pi / 180

	dlat := rlat2 - rlat1
	dlng := rlng2 - rlng1

	a := math.Sin(dlat/2)*math.Sin(dlat/2) +
		math.Cos(rlat1)*math.Cos(rlat2)*math.Sin(dlng/2)*math.Sin(dlng/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	distance := earthRadiusKm * c

	return math.Round((distance*latencyPerKm*latencyOverhead+latencyFloorMs)*100) / 100
}

// FetchLiveNetwork queries ClickHouse for the current network topology
// and assembles it into a ShapleyInput.
func FetchLiveNetwork(ctx context.Context, db driver.Conn) (*LiveNetworkResponse, error) {
	// Query devices with their contributor (operator) and location info
	type deviceRow struct {
		DeviceCode      string
		ContributorPk   string
		ContributorCode string
		ContributorName string
		MetroCode       string
		MetroName       string
		MetroLat        float64
		MetroLng        float64
	}

	deviceQuery := `
		SELECT
			d.code AS device_code,
			COALESCE(c.pk, '') AS contributor_pk,
			COALESCE(c.code, '') AS contributor_code,
			COALESCE(c.name, '') AS contributor_name,
			COALESCE(m.code, '') AS metro_code,
			COALESCE(m.name, '') AS metro_name,
			COALESCE(m.latitude, 0) AS metro_lat,
			COALESCE(m.longitude, 0) AS metro_lng
		FROM dz_devices_current d
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		WHERE d.status = 'activated'
	`
	deviceRows, err := db.Query(ctx, deviceQuery)
	if err != nil {
		return nil, fmt.Errorf("query devices: %w", err)
	}
	defer deviceRows.Close()

	deviceMap := make(map[string]deviceRow)
	for deviceRows.Next() {
		var r deviceRow
		if err := deviceRows.Scan(&r.DeviceCode, &r.ContributorPk, &r.ContributorCode, &r.ContributorName, &r.MetroCode, &r.MetroName, &r.MetroLat, &r.MetroLng); err != nil {
			return nil, fmt.Errorf("scan device: %w", err)
		}
		deviceMap[r.DeviceCode] = r
	}

	// Query links
	type linkRow struct {
		SideACode string
		SideZCode string
		DelayNs   int64
		Bandwidth int64
	}

	linkQuery := `
		SELECT
			da.code AS side_a_code,
			dz.code AS side_z_code,
			COALESCE(lk.committed_rtt_ns, 0) AS delay_ns,
			COALESCE(lk.bandwidth_bps, 0) AS bandwidth
		FROM dz_links_current lk
		JOIN dz_devices_current da ON lk.side_a_pk = da.pk
		JOIN dz_devices_current dz ON lk.side_z_pk = dz.pk
		WHERE lk.status = 'activated'
	`
	linkRows, err := db.Query(ctx, linkQuery)
	if err != nil {
		return nil, fmt.Errorf("query links: %w", err)
	}
	defer linkRows.Close()

	addedDevices := make(map[string]bool)
	cities := make(map[string]bool)
	metroCoords := make(map[string][2]float64) // city code -> [lat, lng]

	var privateLinks []PrivateLink
	var devices []Device

	for linkRows.Next() {
		var r linkRow
		if err := linkRows.Scan(&r.SideACode, &r.SideZCode, &r.DelayNs, &r.Bandwidth); err != nil {
			return nil, fmt.Errorf("scan link: %w", err)
		}

		latencyMs := float64(r.DelayNs) / 1_000_000
		bandwidthGbps := float64(r.Bandwidth) / 1_000_000_000

		privateLinks = append(privateLinks, PrivateLink{
			Device1:   r.SideACode,
			Device2:   r.SideZCode,
			Latency:   math.Round(latencyMs*100) / 100,
			Bandwidth: math.Round(bandwidthGbps*10) / 10,
			Uptime:    defaultLinkUptime,
			Shared:    "NA",
		})

		// Add devices
		for _, rawCode := range []string{r.SideACode, r.SideZCode} {
			if addedDevices[rawCode] {
				continue
			}
			addedDevices[rawCode] = true

			devInfo := deviceMap[rawCode]
			city := strings.ToUpper(devInfo.MetroCode)
			if city == "" {
				continue // skip devices with no metro assignment
			}
			cities[city] = true
			if devInfo.MetroLat != 0 || devInfo.MetroLng != 0 {
				metroCoords[city] = [2]float64{devInfo.MetroLat, devInfo.MetroLng}
			}

			devices = append(devices, Device{
				Device:       rawCode,
				Edge:         10,
				Operator:     devInfo.ContributorCode,
				OperatorPk:   devInfo.ContributorPk,
				City:         city,
				CityName:     devInfo.MetroName,
				OperatorName: devInfo.ContributorName,
			})
		}
	}

	// Generate public links (all city pairs)
	cityList := make([]string, 0, len(cities))
	for c := range cities {
		cityList = append(cityList, c)
	}
	sort.Strings(cityList)

	// Try to get actual metro latencies from ClickHouse
	metroLatencies, err := fetchMetroLatencies(ctx, db)
	if err != nil {
		log.Printf("rewards: fetch metro latencies (falling back to haversine): %v", err)
	}

	var publicLinks []PublicLink
	for i, c1 := range cityList {
		for _, c2 := range cityList[i+1:] {
			// Use actual measured latency if available
			key := c1 + "-" + c2
			if c1 > c2 {
				key = c2 + "-" + c1
			}
			var latency float64
			if measured, ok := metroLatencies[key]; ok {
				latency = measured
			} else if coord1, ok1 := metroCoords[c1]; ok1 {
				if coord2, ok2 := metroCoords[c2]; ok2 {
					latency = estimatePublicLatency(coord1[0], coord1[1], coord2[0], coord2[1])
				} else {
					latency = latencyFallback
				}
			} else {
				latency = latencyFallback
			}
			publicLinks = append(publicLinks, PublicLink{
				City1:   c1,
				City2:   c2,
				Latency: latency,
			})
		}
	}

	// Build demand from validator leader-schedule slots, matching the offchain reward calculator.
	// Cache the result to avoid re-querying ClickHouse on every frontend poll.
	demands := getCachedDemand()
	if demands == nil {
		var demandErr error
		demands, demandErr = fetchValidatorDemand(ctx, db, cities)
		if demandErr != nil {
			log.Printf("rewards: fetch validator demand failed (falling back to synthetic): %v", demandErr)
		}
		if len(demands) > 0 {
			setCachedDemand(demands)
		}
	}
	if len(demands) == 0 {
		log.Printf("rewards: 0 metros with validator slots found, using synthetic demand for %d cities", len(cities))
		// Generate synthetic demands so the LP has flow requirements.
		typeCounter := 1
		for i, c1 := range cityList {
			for _, c2 := range cityList[i+1:] {
				if typeCounter > maxSyntheticDemands {
					break
				}
				demands = append(demands, Demand{
					Start: c1, End: c2,
					Receivers: 100, Traffic: 100.0,
					Priority: 1.0, Type: typeCounter,
					Multicast: "FALSE",
				})
				typeCounter++
			}
			if typeCounter > maxSyntheticDemands {
				break
			}
		}
	}

	// Count distinct operators from devices.
	operators := make(map[string]bool)
	for _, d := range devices {
		operators[d.Operator] = true
	}

	return &LiveNetworkResponse{
		Network: ShapleyInput{
			PrivateLinks:     privateLinks,
			Devices:          devices,
			Demands:          demands,
			PublicLinks:      publicLinks,
			OperatorUptime:   DefaultOperatorUptime,
			ContiguityBonus:  DefaultContiguityBonus,
			DemandMultiplier: DefaultDemandMultiplier,
		},
		DeviceCount:   len(devices),
		LinkCount:     len(privateLinks),
		OperatorCount: len(operators),
		MetroCount:    len(cities),
	}, nil
}

// fetchMetroLatencies fetches actual measured latencies between metros from ClickHouse.
func fetchMetroLatencies(ctx context.Context, db driver.Conn) (map[string]float64, error) {
	query := `
		SELECT
			m1.code AS origin_metro_code,
			m2.code AS target_metro_code,
			avg(f.rtt_us) / 1000.0 AS avg_latency_ms
		FROM fact_dz_internet_metro_latency f
		JOIN dz_metros_current m1 ON f.origin_metro_pk = m1.pk
		JOIN dz_metros_current m2 ON f.target_metro_pk = m2.pk
		WHERE f.rtt_us > 0
		GROUP BY m1.code, m2.code
	`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	latencies := make(map[string]float64)
	for rows.Next() {
		var origin, target string
		var avgLatency float64
		if err := rows.Scan(&origin, &target, &avgLatency); err != nil {
			continue
		}
		// Store with sorted key so A-B and B-A both map to same entry
		if origin > target {
			origin, target = target, origin
		}
		key := origin + "-" + target
		if _, exists := latencies[key]; !exists {
			latencies[key] = avgLatency
		} else {
			// Average both directions
			latencies[key] = (latencies[key] + avgLatency) / 2
		}
	}
	return latencies, nil
}

// fetchValidatorDemand queries ClickHouse for DZ validators, maps them to metro codes
// via their device, and weights city-to-city demand pairs by leader schedule slots.
// This matches the demand model used by the production offchain reward calculator
// (doublezero-offchain/crates/contributor-rewards/src/ingestor/demand.rs).
func fetchValidatorDemand(ctx context.Context, db driver.Conn, activeCities map[string]bool) ([]Demand, error) {
	// Step 1: Get metro → pubkeys mapping from DZ validators.
	metroQuery := `
		SELECT upper(m.code) AS metro, g.pubkey
		FROM dz_users_current u
		JOIN dz_devices_current d ON u.device_pk = d.pk
		JOIN dz_metros_current m ON d.metro_pk = m.pk
		JOIN solana_gossip_nodes_current g ON g.gossip_ip = u.dz_ip
		WHERE u.status = 'activated'
		  AND u.dz_ip != ''
		  AND m.code != ''
	`
	metroRows, err := db.Query(ctx, metroQuery)
	if err != nil {
		return nil, fmt.Errorf("query metro pubkeys: %w", err)
	}
	type pubkeyInfo struct {
		metro  string
		pubkey string
	}
	var pubkeys []pubkeyInfo
	for metroRows.Next() {
		var pi pubkeyInfo
		if err := metroRows.Scan(&pi.metro, &pi.pubkey); err != nil {
			continue
		}
		pi.metro = strings.ToUpper(pi.metro)
		pubkeys = append(pubkeys, pi)
	}
	metroRows.Close() // Must close before next query on same connection.

	// Step 2: Get pubkey → slots from block production.
	// Use max() instead of argMax() — column is cumulative so max = latest value.
	// This also avoids potential ClickHouse driver type issues with argMax.
	slotsQuery := `
		SELECT leader_identity_pubkey, max(leader_slots_assigned_cum) AS slots
		FROM fact_solana_block_production
		GROUP BY leader_identity_pubkey
		HAVING slots > 0
	`
	slotsRows, err := db.Query(ctx, slotsQuery)
	if err != nil {
		return nil, fmt.Errorf("query block production slots: %w", err)
	}
	slotsByPubkey := make(map[string]int64)
	for slotsRows.Next() {
		var pubkey string
		var slots int64
		if err := slotsRows.Scan(&pubkey, &slots); err != nil {
			continue
		}
		slotsByPubkey[pubkey] = slots
	}
	slotsRows.Close()

	// Step 3: Join in Go — aggregate by metro.
	type metroStats struct {
		metro          string
		validatorCount int64
		totalSlots     int64
	}
	metroMap := make(map[string]*metroStats)
	var matched int
	for _, pi := range pubkeys {
		slots, ok := slotsByPubkey[pi.pubkey]
		if !ok {
			continue
		}
		matched++
		ms, exists := metroMap[pi.metro]
		if !exists {
			ms = &metroStats{metro: pi.metro}
			metroMap[pi.metro] = ms
		}
		ms.validatorCount++
		ms.totalSlots += slots
	}

	var metros []metroStats
	for _, ms := range metroMap {
		if !activeCities[ms.metro] || ms.validatorCount == 0 {
			continue
		}
		metros = append(metros, *ms)
	}

	// Sort by total slots descending and optionally cap to top metros for shapley-cli tractability.
	sort.Slice(metros, func(i, j int) bool {
		return metros[i].totalSlots > metros[j].totalSlots
	})
	if len(metros) > maxDemandMetros {
		metros = metros[:maxDemandMetros]
	}

	var totalSlotsAll int64
	for _, ms := range metros {
		totalSlotsAll += ms.totalSlots
	}

	log.Printf("rewards: validator demand: %d metros (capped from %d), %d validators matched, %d total slots",
		len(metros), len(metroMap), matched, totalSlotsAll)

	if len(metros) < 2 || totalSlotsAll == 0 {
		return nil, nil
	}

	// Generate city-to-city demand pairs matching the offchain calculator:
	// doublezero-offchain/crates/contributor-rewards/src/ingestor/demand.rs
	// Priority = (1/SLOTS_IN_EPOCH) * (dst.totalSlots / dst.validatorCount)
	// Traffic = constant 0.05
	// Multicast = false
	var demands []Demand
	typeCounter := 1
	for _, src := range metros {
		for _, dst := range metros {
			if dst.metro == src.metro {
				continue
			}
			slotsPerValidator := float64(dst.totalSlots) / float64(dst.validatorCount)
			priority := (1.0 / slotsInEpoch) * slotsPerValidator
			demands = append(demands, Demand{
				Start:     src.metro,
				End:       dst.metro,
				Receivers: int(dst.validatorCount),
				Traffic:   demandTraffic,
				Priority:  priority,
				Type:      typeCounter,
				Multicast: "FALSE",
			})
			typeCounter++
		}
	}

	return demands, nil
}
