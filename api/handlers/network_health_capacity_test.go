package handlers_test

import (
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// insertLinkSideBps inserts one device_interface_rollup_5m row carrying the
// egress percentiles of a single side of a link at ts.
func insertLinkSideBps(t *testing.T, api *handlers.API, ts time.Time, linkPK, side, devicePK string, p50, p99 float64) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(),
		`INSERT INTO device_interface_rollup_5m
		 (bucket_ts, device_pk, intf, link_pk, link_side, ingested_at, p50_out_bps, p99_out_bps)
		 VALUES ($1, $2, $3, $4, $5, now(), $6, $7)`,
		ts, devicePK, "Ethernet1/1", linkPK, side, p50, p99))
}

// TestNetworkHealthCapacity_PeakIsBusierDirection pins that the fullest-links
// panel measures one direction against provisioned bandwidth. Each direction of
// a full-duplex link gets the whole bandwidth, so adding the A and Z sides
// reports a link at line rate while both directions still have headroom.
func TestNetworkHealthCapacity_PeakIsBusierDirection(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_links_current (pk, code, status, link_type, bandwidth_bps, side_a_pk, side_z_pk, contributor_pk)
		 VALUES ('link-duplex', 'NYC-LAX-DUPLEX', 'activated', 'WAN', 10000000000, 'dev-nyc-1', 'dev-lax-1', 'contrib-1')`))

	now := time.Now().UTC()
	start := now.Add(-2 * time.Hour)
	end := now.Add(2 * time.Hour)
	bucket := now.Add(-1 * time.Hour).Truncate(5 * time.Minute)

	// On a 10 Gbps link, A sends 6 Gbps while Z sends 4 Gbps in the same bucket.
	// Neither direction is full, but the two added together are exactly line rate.
	insertLinkSideBps(t, api, bucket, "link-duplex", "A", "dev-nyc-1", 3e9, 6e9)
	insertLinkSideBps(t, api, bucket, "link-duplex", "Z", "dev-lax-1", 1e9, 4e9)

	resp := api.FetchNetworkHealthCapacityData(ctx, start, end, "")

	var link *handlers.NHCapacityLink
	for i := range resp.CapacityLinks {
		if resp.CapacityLinks[i].LinkCode == "NYC-LAX-DUPLEX" {
			link = &resp.CapacityLinks[i]
		}
	}
	require.NotNil(t, link, "NYC-LAX-DUPLEX missing from CapacityLinks")

	// Summing the sides reported 10 Gbps and 100% on a link whose busiest
	// direction only reached 60%.
	assert.InDelta(t, 6.0, link.PeakGbps, 0.01, "peak is the busier direction, not A+Z")
	assert.InDelta(t, 60.0, link.UtilPct, 0.1, "60% of a 10 Gbps link, not 100%")
	assert.InDelta(t, 60.0, link.P99Util, 0.1)
	assert.InDelta(t, 30.0, link.P50Util, 0.1)
}
