package dzsvc

import (
	"net"
	"testing"
	"time"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

// The BGP RTT fact. Two things are worth pinning: the row is positional against the table, so a
// column reordered on either side has to fail here rather than in production; and the unique key
// is the onchain report, which is what keeps the table's size tied to writes instead of to the
// poll loop.

func newBGPRTTStore(t *testing.T) (*Store, func() any) {
	t.Helper()
	db := testClient(t)
	store, err := NewStore(StoreConfig{Logger: laketesting.NewLogger(), ClickHouse: db})
	require.NoError(t, err)

	query := func() any {
		conn, err := db.Conn(t.Context())
		require.NoError(t, err)
		defer conn.Close()
		rows, err := conn.Query(t.Context(), "SELECT count() FROM fact_dz_user_bgp_rtt FINAL")
		require.NoError(t, err)
		defer rows.Close()
		var n uint64
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&n))
		return n
	}
	return store, query
}

func bgpUser(pk string, tunnel uint16, status string, rttNs, reportedSlot uint64) User {
	return User{
		PK: pk, OwnerPubkey: testPK(2), Status: "activated", Kind: "multicast",
		ClientIP: net.IP{10, 0, 0, 1}, DZIP: net.IP{10, 0, 0, 2}, DevicePK: testPK(3),
		TunnelID: tunnel, BgpStatus: status, BgpRttNs: rttNs,
		LastBgpReportedAt: reportedSlot, LastBgpUpAt: reportedSlot,
	}
}

// The row lands in the columns it claims to. This is the test that catches a reordering of either
// the migration or userBGPRTTRow, which a bare positional INSERT would otherwise accept silently
// and store scrambled.
func TestLake_Serviceability_UserBGPRTT_RowLandsInItsColumns(t *testing.T) {
	t.Parallel()
	store, _ := newBGPRTTStore(t)
	ctx := t.Context()

	userPK := testPK(11)
	at := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	require.NoError(t, store.WriteUserBGPRTT(ctx, []User{
		bgpUser(userPK, 502, "up", 428_000, 900_123),
	}, at))

	conn, err := store.GetClickHouse().Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var (
		devicePK, clientIP, dzIP, status string
		tunnelID                         int32
		reportedSlot, upSlot, rttNs      uint64
	)
	rows, err := conn.Query(ctx, `
		SELECT device_pk, client_ip, dz_ip, tunnel_id, reported_at_slot, up_at_slot, bgp_status, bgp_rtt_ns
		FROM fact_dz_user_bgp_rtt FINAL WHERE user_pk = ?`, userPK)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next(), "the report must be there to be checked")
	require.NoError(t, rows.Scan(&devicePK, &clientIP, &dzIP, &tunnelID, &reportedSlot, &upSlot, &status, &rttNs))

	require.Equal(t, testPK(3), devicePK)
	require.Equal(t, "10.0.0.1", clientIP)
	require.Equal(t, "10.0.0.2", dzIP)
	require.EqualValues(t, 502, tunnelID)
	require.EqualValues(t, 900_123, reportedSlot)
	require.EqualValues(t, 900_123, upSlot)
	require.Equal(t, "up", status)
	require.EqualValues(t, 428_000, rttNs)
}

// Re-observing the same onchain report does not grow the table. The indexer polls every 60s and
// the agent writes about four times a day, so without this the fact would be two orders of
// magnitude larger than the thing it records.
func TestLake_Serviceability_UserBGPRTT_ReobservationCollapses(t *testing.T) {
	t.Parallel()
	store, count := newBGPRTTStore(t)
	ctx := t.Context()

	userPK := testPK(12)
	base := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	// Same report seen three times, then a new report.
	for i := range 3 {
		require.NoError(t, store.WriteUserBGPRTT(ctx,
			[]User{bgpUser(userPK, 502, "up", 428_000, 900_123)},
			base.Add(time.Duration(i)*time.Minute)))
	}
	require.NoError(t, store.WriteUserBGPRTT(ctx,
		[]User{bgpUser(userPK, 502, "up", 511_000, 900_500)},
		base.Add(4*time.Minute)))

	require.EqualValues(t, 2, count(), "three observations of one report are one row; the new report is the second")
}

// A user the agent has never reported for contributes nothing. Its rtt would be a zero that reads
// exactly like a measured zero, and its slot would collide with every other unreported user.
func TestLake_Serviceability_UserBGPRTT_UnreportedUsersAreSkipped(t *testing.T) {
	t.Parallel()
	store, count := newBGPRTTStore(t)
	ctx := t.Context()

	at := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	require.NoError(t, store.WriteUserBGPRTT(ctx, []User{
		bgpUser(testPK(13), 502, "unknown", 0, 0),
		bgpUser(testPK(14), 503, "unknown", 0, 0),
	}, at))

	require.EqualValues(t, 0, count())
}

// A session reported down carries a cleared rtt, and it is stored as reported rather than dropped:
// the report happened, and hiding it would leave the previous good value looking current.
func TestLake_Serviceability_UserBGPRTT_DownIsRecordedWithClearedRTT(t *testing.T) {
	t.Parallel()
	store, _ := newBGPRTTStore(t)
	ctx := t.Context()

	userPK := testPK(15)
	at := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	require.NoError(t, store.WriteUserBGPRTT(ctx,
		[]User{bgpUser(userPK, 502, "down", 0, 901_000)}, at))

	conn, err := store.GetClickHouse().Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	var status string
	var rttNs uint64
	rows, err := conn.Query(ctx,
		"SELECT bgp_status, bgp_rtt_ns FROM fact_dz_user_bgp_rtt FINAL WHERE user_pk = ?", userPK)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&status, &rttNs))
	require.Equal(t, "down", status)
	require.Zero(t, rttNs)
}
