package msdp

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fixture excerpted from a mainnet-beta `show ip msdp summary | json`
// dump. Includes one "connecting" peer with no sessionStartTime — the
// real case the parser must handle without producing garbage.
const summaryFixture = `{
  "peerList": [
    {
      "peerIpAddress": "172.16.0.244",
      "state": "established",
      "sessionStartTime": 1779369727.1818933,
      "saCount": 6,
      "resetCount": 7
    },
    {
      "peerIpAddress": "172.16.1.176",
      "state": "connecting",
      "saCount": 0,
      "resetCount": 0
    },
    {
      "peerIpAddress": "172.16.0.81",
      "state": "established",
      "sessionStartTime": 1758515130.1377997,
      "saCount": 3,
      "resetCount": 1
    }
  ]
}`

const pimSACacheFixture = `{
  "saCache": [
    {
      "sourceGroupPair": {
        "sourceAddress": "148.51.120.84",
        "groupAddress": "233.84.178.1"
      },
      "rpAddress": "10.0.0.0"
    },
    {
      "sourceGroupPair": {
        "sourceAddress": "148.51.121.150",
        "groupAddress": "233.84.178.1"
      },
      "rpAddress": "10.0.0.0"
    }
  ]
}`

const saCacheRejectedFixture = `{
  "acceptedSaMsg": [
    {
      "sourceGroupPair": {
        "sourceAddress": "148.51.120.16",
        "groupAddress": "233.84.178.1"
      },
      "rpAddress": "10.0.0.0",
      "remoteAddress": "172.16.1.6"
    },
    {
      "sourceGroupPair": {
        "sourceAddress": "148.51.122.86",
        "groupAddress": "233.84.178.1"
      },
      "rpAddress": "10.0.0.0",
      "remoteAddress": "172.16.0.244"
    }
  ],
  "rejectedSaMsg": [
    {
      "sourceGroupPair": {
        "sourceAddress": "10.99.99.99",
        "groupAddress": "239.0.0.1"
      },
      "rpAddress": "10.0.0.0",
      "remoteAddress": "172.16.0.244"
    }
  ]
}`

func TestParseSummary(t *testing.T) {
	peers, err := ParseSummary([]byte(summaryFixture))
	require.NoError(t, err)
	require.Len(t, peers, 3)

	byPeer := map[string]Peer{}
	for _, p := range peers {
		byPeer[p.PeerIPAddress] = p
	}

	t.Run("established peer has sessionStartTime", func(t *testing.T) {
		p, ok := byPeer["172.16.0.244"]
		require.True(t, ok)
		assert.Equal(t, "established", p.State)
		assert.False(t, p.SessionStartTime.IsZero())
		assert.Equal(t, int64(6), p.SACount)
		assert.Equal(t, int64(7), p.ResetCount)
	})

	t.Run("connecting peer has zero sessionStartTime", func(t *testing.T) {
		p, ok := byPeer["172.16.1.176"]
		require.True(t, ok)
		assert.Equal(t, "connecting", p.State)
		assert.True(t, p.SessionStartTime.IsZero(), "missing sessionStartTime must parse as zero, not garbage")
		assert.Equal(t, int64(0), p.SACount)
	})
}

func TestParsePimSACache(t *testing.T) {
	entries, err := ParsePimSACache([]byte(pimSACacheFixture))
	require.NoError(t, err)
	require.Len(t, entries, 2)

	var sources []string
	for _, e := range entries {
		sources = append(sources, e.SourceAddress)
		assert.Equal(t, "233.84.178.1", e.GroupAddress)
		assert.Equal(t, "10.0.0.0", e.RPAddress)
	}
	sort.Strings(sources)
	assert.Equal(t, []string{"148.51.120.84", "148.51.121.150"}, sources)
}

func TestParseSACacheRejected(t *testing.T) {
	entries, err := ParseSACacheRejected([]byte(saCacheRejectedFixture))
	require.NoError(t, err)
	require.Len(t, entries, 3, "should combine acceptedSaMsg + rejectedSaMsg into one slice")

	var accepted, rejected int
	for _, e := range entries {
		switch e.Status {
		case SACacheStatusAccepted:
			accepted++
		case SACacheStatusRejected:
			rejected++
			assert.Equal(t, "10.99.99.99", e.SourceAddress)
			assert.Equal(t, "239.0.0.1", e.GroupAddress)
		default:
			t.Errorf("unexpected status %q", e.Status)
		}
	}
	assert.Equal(t, 2, accepted)
	assert.Equal(t, 1, rejected)
}

func TestParse_Empty(t *testing.T) {
	t.Run("summary empty peerList", func(t *testing.T) {
		peers, err := ParseSummary([]byte(`{"peerList":[]}`))
		require.NoError(t, err)
		assert.Empty(t, peers)
	})
	t.Run("pim sa-cache empty", func(t *testing.T) {
		entries, err := ParsePimSACache([]byte(`{"saCache":[]}`))
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
	t.Run("sa-cache rejected both empty", func(t *testing.T) {
		entries, err := ParseSACacheRejected([]byte(`{"acceptedSaMsg":[],"rejectedSaMsg":[]}`))
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
}

func TestParse_InvalidJSON(t *testing.T) {
	_, err := ParseSummary([]byte("not json"))
	assert.Error(t, err)
	_, err = ParsePimSACache([]byte("not json"))
	assert.Error(t, err)
	_, err = ParseSACacheRejected([]byte("not json"))
	assert.Error(t, err)
}
