package handlers

import (
	"fmt"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Values containing a quote, a backslash, and both together.
var quotingInputs = []string{`O'Brien`, `C:\edge`, `it\'s`}

func quotingRequest(params ...string) url.Values {
	q := url.Values{}
	for _, p := range params {
		q.Set(p, strings.Join(quotingInputs, ","))
	}
	return q
}

func namedArg(t *testing.T, args []any, name string) any {
	t.Helper()
	for _, a := range args {
		if nv, ok := a.(driver.NamedValue); ok && nv.Name == name {
			return nv.Value
		}
	}
	t.Fatalf("no named arg %q in %v", name, args)
	return nil
}

func assertNoInputsInSQL(t *testing.T, sql string) {
	t.Helper()
	for _, v := range quotingInputs {
		assert.NotContains(t, sql, v)
	}
	assert.NotContains(t, sql, "O'")
	assert.NotContains(t, sql, `\`)
}

func TestBuildDimensionFilters_ValuesOnlyInArgs(t *testing.T) {
	names := []string{"metro", "device", "link_type", "contributor", "user_kind", "cyoa_type", "interface_type", "intf"}
	q := quotingRequest(names...)
	r := httptest.NewRequest("GET", "/?"+q.Encode(), nil)

	filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, _, _, _, _, _, _, args := buildDimensionFilters(r)

	assertNoInputsInSQL(t, filterSQL+intfFilterSQL+intfTypeSQL+userKindSQL)
	for _, name := range names {
		assert.Contains(t, filterSQL+intfFilterSQL+intfTypeSQL+userKindSQL, "IN (@"+name+")")
		assert.Equal(t, quotingInputs, namedArg(t, args, name))
	}
}

func TestBuildDrilldownQuery_DevicePKOnlyInArgs(t *testing.T) {
	for _, build := range []func(string, string, string, string, bool) (string, []any){BuildDrilldownQuery, BuildDrilldownQueryRaw} {
		for _, pk := range quotingInputs {
			query, args := build("bucket_ts >= now() - INTERVAL 1 HOUR", "5 MINUTE", pk, "AND f.intf = @intf", false)
			assertNoInputsInSQL(t, query)
			assert.Contains(t, query, "f.device_pk = @device_pk")
			assert.Equal(t, pk, namedArg(t, args, "device_pk"))
		}
	}
}

func TestBuildScopedFieldValuesQuery_ValuesOnlyInArgs(t *testing.T) {
	cases := []struct {
		entity, field string
		params        []string
	}{
		{"interfaces", "intf", []string{"metro", "device", "contributor", "link_type"}},
		{"devices", "metro", []string{"contributor", "device"}},
		{"devices", "contributor", []string{"metro", "device"}},
		{"links", "type", []string{"metro", "contributor"}},
	}
	for _, tc := range cases {
		t.Run(tc.entity+"/"+tc.field, func(t *testing.T) {
			q := quotingRequest(tc.params...)
			r := httptest.NewRequest("GET", "/?"+q.Encode(), nil)
			query, args := BuildScopedFieldValuesQuery(tc.entity, tc.field, entityFieldConfigs[tc.entity][tc.field], r)
			require.NotEmpty(t, query)
			assertNoInputsInSQL(t, query)
			for _, p := range tc.params {
				assert.Equal(t, quotingInputs, namedArg(t, args, p))
			}
		})
	}
}

func TestBuildMulticastMembersFieldQuery_GroupOnlyInArgs(t *testing.T) {
	for _, group := range quotingInputs {
		r := httptest.NewRequest("GET", "/?"+url.Values{"group": {group}}.Encode(), nil)
		query, args := buildMulticastMembersFieldQuery("multicast-members", "device", entityFieldConfigs["multicast-members"]["device"], r)
		assertNoInputsInSQL(t, query)
		assert.Equal(t, group, namedArg(t, args, "group"))
	}
}

func TestLinkLatencyFilterSQL_ValuesOnlyInArgs(t *testing.T) {
	names := []string{"metro", "device", "device_a", "device_z", "contributor", "link_type", "code", "status", "pks"}
	q := quotingRequest(names...)
	q.Set("search", quotingInputs[2])
	r := httptest.NewRequest("GET", "/?"+q.Encode(), nil)

	filterSQL, _, _, args := linkLatencyFilterSQL(r)

	assertNoInputsInSQL(t, filterSQL)
	for _, name := range names {
		assert.Equal(t, quotingInputs, namedArg(t, args, name))
	}
	assert.Equal(t, quotingInputs[2], namedArg(t, args, "search"))
}

func TestMetroPairLatencyFilterSQL_ValuesOnlyInArgs(t *testing.T) {
	existing := []any{"start", "end"}
	metroSQL, providerSQL, args := metroPairLatencyFilterSQL(MetroPairLatencyFilter{
		MetroCodes:    quotingInputs,
		DataProviders: quotingInputs,
	}, existing)

	assertNoInputsInSQL(t, metroSQL+providerSQL)
	assert.Equal(t, " AND (lower(ma.code) IN ($3) OR lower(mz.code) IN ($3))", metroSQL)
	assert.Equal(t, " AND lower(f.data_provider) IN ($4)", providerSQL)
	lowered := []string{`o'brien`, `c:\edge`, `it\'s`}
	assert.Equal(t, []any{"start", "end", lowered, lowered}, args)
}

func TestNamedParamList_ValuesOnlyInArgs(t *testing.T) {
	list, args := namedParamList("source", quotingInputs)

	assertNoInputsInSQL(t, list)
	assert.Equal(t, "@source0, @source1, @source2", list)
	for i, v := range quotingInputs {
		assert.Equal(t, v, namedArg(t, args, fmt.Sprintf("source%d", i)))
	}
}
