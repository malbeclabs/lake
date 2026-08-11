package handlers_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
)

// seedLinkSnapshot writes one type-2 row for a link, including the topology
// fields the divergence report reads.
func seedLinkSnapshot(t *testing.T, api *handlers.API, entityID, code, sideA, sideZ string, rttNs int64, topologies string, drained uint8, ts time.Time) {
	t.Helper()
	err := api.DB.Exec(t.Context(), `INSERT INTO dim_dz_links_history (
		entity_id, snapshot_ts, ingested_at, op_id, is_deleted,
		pk, code, link_type, status, contributor_pk, side_a_pk, side_z_pk,
		bandwidth_bps, committed_rtt_ns, link_topologies, unicast_drained
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
		entityID, ts, ts, "00000000-0000-0000-0000-000000000009", uint8(0),
		entityID, code, "WAN", "activated", "contrib-1", sideA, sideZ,
		int64(10_000_000_000), rttNs, topologies, drained,
	)
	require.NoError(t, err)
}

// The report is only as good as its SQL, and the SQL only runs in production.
// This exercises the real query and the real column scans against ClickHouse:
// an earlier version selected toUnixTimestamp (UInt32) into an int64 and every
// request failed, which no pure-Go test could have seen.
func TestExcludedLinksReadsRealClickHouse(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	seedMetro(t, api, "metro-fra", "fra")
	seedMetro(t, api, "metro-tyo", "tyo")
	seedDeviceMetadata(t, api, "dev-fra", "DEV-FRA", "switch", "contrib-1", "metro-fra", 10, "activated")
	seedDeviceMetadata(t, api, "dev-tyo", "DEV-TYO", "switch", "contrib-1", "metro-tyo", 10, "activated")

	rollout := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Second)
	removal := rollout.Add(24 * time.Hour)

	// A link that stayed in the topology.
	seedLinkSnapshot(t, api, "link-tagged", "fra-tyo-a", "dev-fra", "dev-tyo", 100_000_000, `["UNICAST-DEFAULT"]`, 0, rollout)

	// A link tagged at the rollout and pulled out a day later. Reporting the
	// removal rather than the link's first appearance is the whole point of
	// the history query.
	seedLinkSnapshot(t, api, "link-dropped", "fra-tyo-b", "dev-fra", "dev-tyo", 134_000_000, `["UNICAST-DEFAULT"]`, 0, rollout)
	seedLinkSnapshot(t, api, "link-dropped", "fra-tyo-b", "dev-fra", "dev-tyo", 134_000_000, `[]`, 0, removal)

	// A link that was never tagged at all.
	seedLinkSnapshot(t, api, "link-never", "fra-tyo-c", "dev-fra", "dev-tyo", 60_000_000, `[]`, 0, rollout)

	links, activated, err := api.ExportExcludedLinks(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 3, activated, "all three links are activated")

	require.Len(t, links, 2, "the tagged link must not be reported: %+v", links)

	byCode := map[string]handlers.ExcludedLink{}
	for _, l := range links {
		byCode[l.Code] = l
	}

	dropped, ok := byCode["fra-tyo-b"]
	require.True(t, ok, "the untagged link is missing: %+v", links)
	assert.Equal(t, "fra", dropped.FromMetro)
	assert.Equal(t, "tyo", dropped.ToMetro)
	assert.InDelta(t, 134.0, dropped.RttMs, 0.001)
	assert.False(t, dropped.Drained)
	assert.True(t, dropped.EverIncluded, "this link was in the topology before it left")
	assert.Equal(t, removal.Format(time.RFC3339), dropped.ExcludedAt,
		"must date the removal, not the link's first snapshot")

	never, ok := byCode["fra-tyo-c"]
	require.True(t, ok, "the never-tagged link is missing: %+v", links)
	assert.False(t, never.EverIncluded)
	assert.Equal(t, rollout.Format(time.RFC3339), never.ExcludedAt,
		"a link that was never tagged has been excluded since it first appeared")
}

// A drained link is outside the unicast topology even though it carries a tag.
func TestExcludedLinksReportsDrained(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	seedMetro(t, api, "metro-fra", "fra")
	seedMetro(t, api, "metro-tyo", "tyo")
	seedDeviceMetadata(t, api, "dev-fra", "DEV-FRA", "switch", "contrib-1", "metro-fra", 10, "activated")
	seedDeviceMetadata(t, api, "dev-tyo", "DEV-TYO", "switch", "contrib-1", "metro-tyo", 10, "activated")

	ts := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	seedLinkSnapshot(t, api, "link-drained", "fra-tyo-d", "dev-fra", "dev-tyo", 100_000_000, `["UNICAST-DEFAULT"]`, 1, ts)

	links, _, err := api.ExportExcludedLinks(t.Context())
	require.NoError(t, err)

	require.Len(t, links, 1)
	assert.True(t, links[0].Drained)
	// Drained from its first snapshot, so it never entered the unicast set,
	// tag or no tag.
	assert.False(t, links[0].EverIncluded)
}
