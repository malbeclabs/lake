package handlers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultLinkParams returns params that satisfy isDefaultLinkThresholds, i.e. the shape
// the incidents page sends, which routes to the link_incidents_v view query.
func defaultLinkParams() incidentQueryParams {
	return incidentQueryParams{
		Duration:          6 * time.Hour,
		LossThreshold:     10,
		ErrorsThreshold:   1,
		FCSThreshold:      1,
		DiscardsThreshold: 1,
		CarrierThreshold:  1,
		MinDurationMin:    30,
		CoalesceGapMin:    180,
		TypeFilter:        "all",
	}
}

// defaultDeviceParams returns params that satisfy isDefaultDeviceThresholds.
func defaultDeviceParams() incidentQueryParams {
	p := defaultLinkParams()
	p.LossThreshold = 0
	return p
}

// joinAliases are the table aliases the CTE-based incident queries JOIN under. None of
// them exist in the flattened views, so a view query referencing one fails with
// "Unknown expression or function identifier" — the bug behind #763.
var joinAliases = []string{"l.", "cc.", "ma.", "mz.", "da.", "dz.", "m.", "d.", "c."}

// assertNoJoinAliases fails if a query built against a flattened view references any
// alias only the JOIN-based query has in scope.
func assertNoJoinAliases(t *testing.T, query string) {
	t.Helper()
	for _, alias := range joinAliases {
		assert.NotContains(t, query, alias, "view query must not reference a column qualified by the JOIN alias %q", alias)
	}
}

func TestBuildLinkFilterClauses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		filters []IncidentFilter
		cols    linkFilterColumns
		want    string
		args    []any
		wantOK  bool
	}{
		{"no filters", nil, linkViewFilterColumns, "", nil, true},
		{
			"metro on join scope compares both sides against one arg",
			[]IncidentFilter{{Type: "metro", Value: "NYC"}},
			linkJoinFilterColumns,
			"AND (ma.code = $3 OR mz.code = $3)",
			[]any{"NYC"},
			true,
		},
		{
			"metro on view scope",
			[]IncidentFilter{{Type: "metro", Value: "NYC"}},
			linkViewFilterColumns,
			"AND (side_a_metro = $3 OR side_z_metro = $3)",
			[]any{"NYC"},
			true,
		},
		{
			"link on join scope",
			[]IncidentFilter{{Type: "link", Value: "NYC-LAX-001"}},
			linkJoinFilterColumns,
			"AND l.code = $3",
			[]any{"NYC-LAX-001"},
			true,
		},
		{
			"link on view scope",
			[]IncidentFilter{{Type: "link", Value: "NYC-LAX-001"}},
			linkViewFilterColumns,
			"AND link_code = $3",
			[]any{"NYC-LAX-001"},
			true,
		},
		{
			"contributor on join scope",
			[]IncidentFilter{{Type: "contributor", Value: "CONTRIB1"}},
			linkJoinFilterColumns,
			"AND cc.code = $3",
			[]any{"CONTRIB1"},
			true,
		},
		{
			"contributor on view scope",
			[]IncidentFilter{{Type: "contributor", Value: "CONTRIB1"}},
			linkViewFilterColumns,
			"AND contributor_code = $3",
			[]any{"CONTRIB1"},
			true,
		},
		{
			"device on join scope",
			[]IncidentFilter{{Type: "device", Value: "NYC-CORE-01"}},
			linkJoinFilterColumns,
			"AND (da.code = $3 OR dz.code = $3)",
			[]any{"NYC-CORE-01"},
			true,
		},
		{
			"device is unexpressible on view scope",
			[]IncidentFilter{{Type: "device", Value: "NYC-CORE-01"}},
			linkViewFilterColumns,
			"",
			nil,
			false,
		},
		{
			"unexpressible filter discards the whole clause set",
			[]IncidentFilter{
				{Type: "metro", Value: "NYC"},
				{Type: "device", Value: "NYC-CORE-01"},
			},
			linkViewFilterColumns,
			"",
			nil,
			false,
		},
		{
			"unknown filter type is ignored",
			[]IncidentFilter{{Type: "bogus", Value: "x"}},
			linkViewFilterColumns,
			"",
			nil,
			true,
		},
		{
			"multiple filters increment the arg index once each",
			[]IncidentFilter{
				{Type: "metro", Value: "NYC"},
				{Type: "contributor", Value: "CONTRIB1"},
				{Type: "link", Value: "NYC-LAX-001"},
			},
			linkViewFilterColumns,
			"AND (side_a_metro = $3 OR side_z_metro = $3)\n  AND contributor_code = $4\n  AND link_code = $5",
			[]any{"NYC", "CONTRIB1", "NYC-LAX-001"},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, args, ok := buildLinkFilterClauses(tt.filters, 3, tt.cols)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.args, args)
		})
	}
}

func TestBuildDeviceFilterClauses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		filters []IncidentFilter
		cols    deviceFilterColumns
		want    string
		args    []any
	}{
		{"no filters", nil, deviceViewFilterColumns, "", nil},
		{
			"metro on join scope",
			[]IncidentFilter{{Type: "metro", Value: "NYC"}},
			deviceJoinFilterColumns,
			"AND m.code = $3",
			[]any{"NYC"},
		},
		{
			"metro on view scope",
			[]IncidentFilter{{Type: "metro", Value: "NYC"}},
			deviceViewFilterColumns,
			"AND metro = $3",
			[]any{"NYC"},
		},
		{
			"device on join scope",
			[]IncidentFilter{{Type: "device", Value: "NYC-CORE-01"}},
			deviceJoinFilterColumns,
			"AND d.code = $3",
			[]any{"NYC-CORE-01"},
		},
		{
			"device on view scope",
			[]IncidentFilter{{Type: "device", Value: "NYC-CORE-01"}},
			deviceViewFilterColumns,
			"AND device_code = $3",
			[]any{"NYC-CORE-01"},
		},
		{
			"contributor on join scope",
			[]IncidentFilter{{Type: "contributor", Value: "CONTRIB1"}},
			deviceJoinFilterColumns,
			"AND cc.code = $3",
			[]any{"CONTRIB1"},
		},
		{
			"contributor on view scope",
			[]IncidentFilter{{Type: "contributor", Value: "CONTRIB1"}},
			deviceViewFilterColumns,
			"AND contributor_code = $3",
			[]any{"CONTRIB1"},
		},
		{
			"link filter is not supported for devices",
			[]IncidentFilter{{Type: "link", Value: "NYC-LAX-001"}},
			deviceViewFilterColumns,
			"",
			nil,
		},
		{
			"multiple filters increment the arg index once each",
			[]IncidentFilter{
				{Type: "metro", Value: "NYC"},
				{Type: "device", Value: "NYC-CORE-01"},
			},
			deviceViewFilterColumns,
			"AND metro = $3\n  AND device_code = $4",
			[]any{"NYC", "NYC-CORE-01"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, args, ok := buildDeviceFilterClauses(tt.filters, 3, tt.cols)
			assert.True(t, ok, "both device scopes expose every filterable value")
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.args, args)
		})
	}
}

// TestBuildLinkIncidentsQuery_FilterScope pins the bug from #763: the view query has no
// JOINs, so every filter it can express must use the view's own flattened columns.
func TestBuildLinkIncidentsQuery_FilterScope(t *testing.T) {
	t.Parallel()

	for _, f := range []IncidentFilter{
		{Type: "metro", Value: "NYC"},
		{Type: "link", Value: "NYC-LAX-001"},
		{Type: "contributor", Value: "CONTRIB1"},
	} {
		t.Run(f.Type+" filter uses the view", func(t *testing.T) {
			t.Parallel()
			p := defaultLinkParams()
			p.Filters = []IncidentFilter{f}

			query, args := buildLinkIncidentsQuery(p)
			assert.Contains(t, query, "FROM link_incidents_v")
			assert.Equal(t, []any{int64(21600), f.Value}, args)
			assertNoJoinAliases(t, query)
		})
	}

	t.Run("device filter falls back to the join query", func(t *testing.T) {
		t.Parallel()
		p := defaultLinkParams()
		p.Filters = []IncidentFilter{{Type: "device", Value: "NYC-CORE-01"}}

		query, args := buildLinkIncidentsQuery(p)
		assert.NotContains(t, query, "link_incidents_v", "link view exposes no device codes")
		assert.Contains(t, query, "LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk")
		assert.Contains(t, query, "AND (da.code = $8 OR dz.code = $8)")
		assert.Contains(t, args, "NYC-CORE-01")
	})

	t.Run("raw source keeps using the join query", func(t *testing.T) {
		t.Parallel()
		p := defaultLinkParams()
		p.UseRaw = true
		p.Filters = []IncidentFilter{{Type: "contributor", Value: "CONTRIB1"}}

		query, _ := buildLinkIncidentsQuery(p)
		assert.NotContains(t, query, "link_incidents_v")
		assert.Contains(t, query, "AND cc.code = $")
	})
}

// TestBuildDeviceIncidentsQuery_FilterScope is the device_incidents_v counterpart. The
// device view exposes all three filterable columns, so there is no fallback case.
func TestBuildDeviceIncidentsQuery_FilterScope(t *testing.T) {
	t.Parallel()

	for _, f := range []IncidentFilter{
		{Type: "metro", Value: "NYC"},
		{Type: "device", Value: "NYC-CORE-01"},
		{Type: "contributor", Value: "CONTRIB1"},
	} {
		t.Run(f.Type+" filter uses the view", func(t *testing.T) {
			t.Parallel()
			p := defaultDeviceParams()
			p.Filters = []IncidentFilter{f}

			query, args := buildDeviceIncidentsQuery(p)
			assert.Contains(t, query, "FROM device_incidents_v")
			assert.Equal(t, []any{int64(21600), f.Value}, args)
			assertNoJoinAliases(t, query)
		})
	}

	t.Run("non-default thresholds use the join query aliases", func(t *testing.T) {
		t.Parallel()
		p := defaultDeviceParams()
		p.ErrorsThreshold = 50
		p.Filters = []IncidentFilter{{Type: "metro", Value: "NYC"}}

		query, args := buildDeviceIncidentsQuery(p)
		assert.NotContains(t, query, "device_incidents_v")
		assert.Contains(t, query, "AND m.code = $")
		assert.Contains(t, args, "NYC")
	})
}

// TestLinkFilterColumnsCoverEveryFilterType guards the empty deviceA/deviceZ entries in
// linkViewFilterColumns: a filter type a scope cannot express must make the whole clause
// set unexpressible, never an empty column name formatted into "AND  = $N".
func TestLinkFilterColumnsCoverEveryFilterType(t *testing.T) {
	t.Parallel()
	for _, filterType := range []string{"metro", "link", "contributor", "device"} {
		f := []IncidentFilter{{Type: filterType, Value: "x"}}

		joinClauses, _, joinOK := buildLinkFilterClauses(f, 1, linkJoinFilterColumns)
		require.True(t, joinOK, "join scope must express every filter type")
		assert.Contains(t, joinClauses, "$1")

		viewClauses, _, viewOK := buildLinkFilterClauses(f, 1, linkViewFilterColumns)
		if viewOK {
			assert.Contains(t, viewClauses, "$1", "filter %q emitted no clause", filterType)
		} else {
			assert.Empty(t, viewClauses, "unexpressible filter %q must emit no clause", filterType)
		}
	}
}
