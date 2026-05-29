package mroute

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fixture excerpted from a mainnet-beta `show ip mroute | json` dump.
// Covers the cases the parser must round-trip:
//   - ME flag: rpfInterface=Null0, no rpf{} block, empty oifList
//   - SMP flag: rpf block present, single OIF
//   - SBNP flag: tunnel RPF interface, multi-element OIF list
//   - E flag: encap-only, empty oifList
//   - SP flag: prune state with rpf block
//   - A single group with multiple sources (verifies map iteration)
//   - Bidirectional groups map present but empty (verifies safe walk)
const mrouteFixture = `{
  "vrfs": {
    "default": {
      "bidirectional": { "groups": {} },
      "sparseMode": {
        "groups": {
          "233.84.178.1": {
            "groupSources": {
              "148.51.120.0": {
                "sourceAddress": "148.51.120.0",
                "creationTime": 1777133184.0,
                "routeFlags": "SMP",
                "registerInOifList": false,
                "rpfInterface": "Port-Channel1000.2020",
                "rpf": {
                  "rpfRib": "U",
                  "rpfPrefix": "148.51.120.0/32",
                  "rpfPreference": 200,
                  "rpfMetric": 0,
                  "rpfNeighbor": "172.16.0.207",
                  "rpfAttached": false,
                  "rpfEvpnTenantDomain": false,
                  "rpfMvpn": false
                },
                "oifList": ["Port-Channel3000"]
              },
              "148.51.120.3": {
                "sourceAddress": "148.51.120.3",
                "creationTime": 1779234432.5,
                "routeFlags": "ME",
                "registerInOifList": false,
                "rpfInterface": "Null0",
                "oifList": []
              },
              "148.51.122.96": {
                "sourceAddress": "148.51.122.96",
                "creationTime": 1777570304.0,
                "routeFlags": "SBNP",
                "registerInOifList": false,
                "rpfInterface": "Tunnel504",
                "oifList": ["Port-Channel3000", "Port-Channel1000.2020"]
              }
            }
          },
          "233.84.178.6": {
            "groupSources": {
              "148.51.122.234": {
                "sourceAddress": "148.51.122.234",
                "creationTime": 1780067584.0,
                "routeFlags": "E",
                "registerInOifList": false,
                "rpfInterface": "Null0",
                "oifList": []
              }
            }
          },
          "233.84.178.10": {
            "groupSources": {
              "148.51.121.188": {
                "sourceAddress": "148.51.121.188",
                "creationTime": 1778752512.0,
                "routeFlags": "SP",
                "registerInOifList": false,
                "rpfInterface": "Port-Channel3000",
                "rpf": {
                  "rpfRib": "U",
                  "rpfPrefix": "148.51.121.188/32",
                  "rpfPreference": 200,
                  "rpfMetric": 0,
                  "rpfNeighbor": "172.16.1.137",
                  "rpfAttached": false,
                  "rpfEvpnTenantDomain": false,
                  "rpfMvpn": false
                },
                "oifList": ["Port-Channel1000.2020"]
              },
              "148.51.121.189": {
                "sourceAddress": "148.51.121.189",
                "creationTime": 1777961344.0,
                "routeFlags": "SP",
                "registerInOifList": false,
                "rpfInterface": "Port-Channel1000.2020",
                "rpf": {
                  "rpfRib": "U",
                  "rpfPrefix": "148.51.121.189/32",
                  "rpfPreference": 200,
                  "rpfMetric": 0,
                  "rpfNeighbor": "172.16.0.207",
                  "rpfAttached": false,
                  "rpfEvpnTenantDomain": false,
                  "rpfMvpn": false
                },
                "oifList": ["Port-Channel3000"]
              }
            }
          }
        }
      }
    }
  }
}`

func TestParse(t *testing.T) {
	entries, err := Parse([]byte(mrouteFixture))
	require.NoError(t, err)
	require.Len(t, entries, 6)

	byKey := indexByKey(entries)

	t.Run("ME entry has no rpf, empty OIF", func(t *testing.T) {
		e := byKey["default|sparse|233.84.178.1|148.51.120.3"]
		require.NotNil(t, e)
		assert.Equal(t, "ME", e.RouteFlags)
		assert.Equal(t, "Null0", e.RpfInterface)
		assert.Nil(t, e.RPF)
		assert.Empty(t, e.OifList)
		assert.False(t, e.CreationTime.IsZero(), "fractional creationTime should parse")
	})

	t.Run("SMP entry has rpf and single OIF", func(t *testing.T) {
		e := byKey["default|sparse|233.84.178.1|148.51.120.0"]
		require.NotNil(t, e)
		assert.Equal(t, "SMP", e.RouteFlags)
		require.NotNil(t, e.RPF)
		assert.Equal(t, "172.16.0.207", e.RPF.Neighbor)
		assert.Equal(t, uint32(200), e.RPF.Preference)
		assert.Equal(t, []string{"Port-Channel3000"}, e.OifList)
	})

	t.Run("SBNP entry has tunnel RPF and multi-element OIF", func(t *testing.T) {
		e := byKey["default|sparse|233.84.178.1|148.51.122.96"]
		require.NotNil(t, e)
		assert.Equal(t, "SBNP", e.RouteFlags)
		assert.Equal(t, "Tunnel504", e.RpfInterface)
		assert.Nil(t, e.RPF, "Arista does not emit rpf{} for tunnel SBNP")
		assert.Equal(t, []string{"Port-Channel3000", "Port-Channel1000.2020"}, e.OifList)
	})

	t.Run("E entry has empty OIF and no rpf", func(t *testing.T) {
		e := byKey["default|sparse|233.84.178.6|148.51.122.234"]
		require.NotNil(t, e)
		assert.Equal(t, "E", e.RouteFlags)
		assert.Nil(t, e.RPF)
		assert.Empty(t, e.OifList)
	})

	t.Run("multi-source group yields one entry per source", func(t *testing.T) {
		var sources []string
		for _, e := range entries {
			if e.GroupAddress == "233.84.178.10" {
				sources = append(sources, e.SourceAddress)
			}
		}
		sort.Strings(sources)
		assert.Equal(t, []string{"148.51.121.188", "148.51.121.189"}, sources)
	})

	t.Run("empty bidirectional groups produce no entries", func(t *testing.T) {
		for _, e := range entries {
			assert.NotEqual(t, ModeBidirectional, e.Mode)
		}
	})
}

func TestParse_Empty(t *testing.T) {
	const empty = `{"vrfs":{"default":{"bidirectional":{"groups":{}},"sparseMode":{"groups":{}}}}}`
	entries, err := Parse([]byte(empty))
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestParse_InvalidJSON(t *testing.T) {
	_, err := Parse([]byte("not json"))
	assert.Error(t, err)
}

func indexByKey(entries []Entry) map[string]*Entry {
	m := make(map[string]*Entry, len(entries))
	for i := range entries {
		e := &entries[i]
		k := string(e.VRF) + "|" + string(e.Mode) + "|" + e.GroupAddress + "|" + e.SourceAddress
		m[k] = e
	}
	return m
}
