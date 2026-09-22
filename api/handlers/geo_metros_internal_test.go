package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// metroRows yields the scripted metro rows, then stops.
type metroRows struct {
	panicRows
	metros []metroCoord
	i      int
}

func (r *metroRows) Next() bool {
	if r.i >= len(r.metros) {
		return false
	}
	r.i++
	return true
}

func (r *metroRows) Scan(dest ...any) error {
	m := r.metros[r.i-1]
	*(dest[0].(*string)) = m.code
	*(dest[1].(*float64)) = m.lat
	*(dest[2].(*float64)) = m.lng
	return nil
}

func (r *metroRows) Err() error   { return nil }
func (r *metroRows) Close() error { return nil }

// TestFetchMetroCoordsRejectsAnEmptyTable pins the half of the metros read that a
// successful query hides — see errNoMetros for what an empty table renders as.
func TestFetchMetroCoordsRejectsAnEmptyTable(t *testing.T) {
	t.Parallel()

	_, err := fetchMetroCoords(t.Context(), &mockConn{rows: &metroRows{}})
	require.ErrorIs(t, err, errNoMetros)
}

func TestFetchMetroCoordsReadsEveryRow(t *testing.T) {
	t.Parallel()

	want := []metroCoord{
		{code: "ams", lat: 52.37, lng: 4.89},
		{code: "nyc", lat: 40.71, lng: -74.01},
	}
	got, err := fetchMetroCoords(t.Context(), &mockConn{rows: &metroRows{metros: want}})
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestNearestMetro covers the assignment both geo handlers now share, so a
// tie-break or max-distance change cannot move validators on only one of them.
func TestNearestMetro(t *testing.T) {
	t.Parallel()

	metros := []metroCoord{
		{code: "ams", lat: 52.37, lng: 4.89},
		{code: "nyc", lat: 40.71, lng: -74.01},
		{code: "tyo", lat: 35.68, lng: 139.69},
	}

	for _, tc := range []struct {
		name     string
		lat, lng float64
		want     string
	}{
		{"frankfurt lands on amsterdam", 50.11, 8.68, "ams"},
		{"boston lands on new york", 42.36, -71.06, "nyc"},
		{"osaka lands on tokyo", 34.69, 135.50, "tyo"},
		{"antimeridian does not wrap to the wrong hemisphere", 35.0, 179.0, "tyo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, nearestMetro(tc.lat, tc.lng, metros))
		})
	}
}
