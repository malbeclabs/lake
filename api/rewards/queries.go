package rewards

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

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

	// Build demand from real metro-to-metro traffic data
	demands, err := fetchMetroTrafficDemand(ctx, db, cities)
	if err != nil {
		log.Printf("rewards: fetch metro traffic demand (falling back to synthetic): %v", err)
	}
	if len(demands) == 0 {
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

// fetchMetroTrafficDemand queries real metro-to-metro traffic volumes from ClickHouse
// and converts them into demand entries weighted by actual bandwidth usage.
func fetchMetroTrafficDemand(ctx context.Context, db driver.Conn, activeCities map[string]bool) ([]Demand, error) {
	query := `
		WITH link_metros AS (
			SELECT
				l.pk AS link_pk,
				da.metro_pk AS side_a_metro_pk,
				dz.metro_pk AS side_z_metro_pk
			FROM dz_links_current l
			JOIN dz_devices_current da ON l.side_a_pk = da.pk
			JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			WHERE l.status = 'activated'
		),
		metro_codes AS (
			SELECT pk, upper(code) AS code FROM dz_metros_current
		),
		per_link_traffic AS (
			SELECT
				f.link_pk,
				SUM(GREATEST(0, f.in_octets_delta) + GREATEST(0, f.out_octets_delta)) AS total_bytes
			FROM fact_dz_device_interface_counters f
			WHERE f.event_ts > now() - INTERVAL 24 HOUR
			  AND f.link_pk != ''
			  AND f.delta_duration > 0
			  AND f.in_octets_delta >= 0
			  AND f.out_octets_delta >= 0
			GROUP BY f.link_pk
		)
		SELECT
			ma.code AS origin_metro,
			mz.code AS dest_metro,
			SUM(t.total_bytes) AS traffic_volume_bytes
		FROM per_link_traffic t
		JOIN link_metros lm ON t.link_pk = lm.link_pk
		JOIN metro_codes ma ON lm.side_a_metro_pk = ma.pk
		JOIN metro_codes mz ON lm.side_z_metro_pk = mz.pk
		GROUP BY ma.code, mz.code
		HAVING traffic_volume_bytes > 0
		ORDER BY traffic_volume_bytes DESC
	`

	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query metro traffic: %w", err)
	}
	defer rows.Close()

	type trafficPair struct {
		origin, dest string
		bytes        float64
	}
	var pairs []trafficPair
	var maxBytes float64

	for rows.Next() {
		var origin, dest string
		var volumeBytes uint64
		if err := rows.Scan(&origin, &dest, &volumeBytes); err != nil {
			continue
		}
		origin = strings.ToUpper(origin)
		dest = strings.ToUpper(dest)
		if !activeCities[origin] || !activeCities[dest] || origin == dest {
			continue
		}
		b := float64(volumeBytes)
		pairs = append(pairs, trafficPair{origin: origin, dest: dest, bytes: b})
		if b > maxBytes {
			maxBytes = b
		}
	}

	if len(pairs) == 0 {
		return nil, nil
	}

	// Normalize traffic volumes to 0.01-1.0 range for the Shapley computation.
	// Scale receivers proportionally (1-100) so higher-traffic pairs have more weight.
	var demands []Demand
	for i, p := range pairs {
		normalizedTraffic := p.bytes / maxBytes
		if normalizedTraffic < 0.01 {
			normalizedTraffic = 0.01
		}
		receivers := int(math.Round(normalizedTraffic * 100))
		if receivers < 1 {
			receivers = 1
		}

		demands = append(demands, Demand{
			Start:     p.origin,
			End:       p.dest,
			Receivers: receivers,
			Traffic:   math.Round(normalizedTraffic*100) / 100,
			Priority:  0.5,
			Type:      i + 1,
			Multicast: "FALSE",
		})
	}

	return demands, nil
}
