package handlers

import (
	"context"
	"errors"
	"math"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// metroCoord is a DZ metro's anchor point, which validators are assigned to.
type metroCoord struct {
	code     string
	lat, lng float64
}

// errNoMetros is returned when dz_metros_current is readable but empty. The geo
// handlers cannot degrade past this: with no metros every validator is assigned
// an empty metro code, which reports an unmeasured geography as a measured one —
// a single nameless bucket holding 100% of stake, and an anchor-point count of 0.
var errNoMetros = errors.New("dz_metros_current returned no rows")

// fetchMetroCoords reads every DZ metro's anchor point. Shared by both geo
// handlers so their metro assignment cannot drift apart.
func fetchMetroCoords(ctx context.Context, conn driver.Conn) ([]metroCoord, error) {
	rows, err := conn.Query(ctx, "SELECT code, latitude, longitude FROM dz_metros_current")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metros []metroCoord
	for rows.Next() {
		var m metroCoord
		if err := rows.Scan(&m.code, &m.lat, &m.lng); err != nil {
			return nil, err
		}
		metros = append(metros, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(metros) == 0 {
		return nil, errNoMetros
	}
	return metros, nil
}

// nearestMetro returns the code of the metro closest to a point. metros must be
// non-empty — fetchMetroCoords is what guarantees that.
func nearestMetro(lat, lng float64, metros []metroCoord) string {
	bestCode := ""
	bestDist := math.MaxFloat64
	for _, m := range metros {
		if d := haversine(lat, lng, m.lat, m.lng); d < bestDist {
			bestDist = d
			bestCode = m.code
		}
	}
	return bestCode
}

// haversine returns the great-circle distance in meters between two points.
func haversine(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadius = 6_371_000 // meters
	dLat := (lat2 - lat1) * math.Pi / 180
	dLng := (lng2 - lng1) * math.Pi / 180
	lat1r := lat1 * math.Pi / 180
	lat2r := lat2 * math.Pi / 180
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1r)*math.Cos(lat2r)*math.Sin(dLng/2)*math.Sin(dLng/2)
	return earthRadius * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}
