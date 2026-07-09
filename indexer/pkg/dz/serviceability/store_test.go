package dzsvc

import (
	"net"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

func testPK(n int) string {
	bytes := make([]byte, 32)
	for i := range bytes {
		bytes[i] = byte(n + i)
	}
	return solana.PublicKeyFromBytes(bytes).String()
}

func TestLake_Serviceability_Store_NewStore(t *testing.T) {
	t.Parallel()

	t.Run("returns error when config validation fails", func(t *testing.T) {
		t.Parallel()

		t.Run("missing logger", func(t *testing.T) {
			t.Parallel()
			store, err := NewStore(StoreConfig{
				ClickHouse: nil,
			})
			require.Error(t, err)
			require.Nil(t, store)
			require.Contains(t, err.Error(), "logger is required")
		})

		t.Run("missing clickhouse", func(t *testing.T) {
			t.Parallel()
			store, err := NewStore(StoreConfig{
				Logger: laketesting.NewLogger(),
			})
			require.Error(t, err)
			require.Nil(t, store)
			require.Contains(t, err.Error(), "clickhouse connection is required")
		})
	})

	t.Run("returns store when config is valid", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		store, err := NewStore(StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: mockDB,
		})
		require.NoError(t, err)
		require.NotNil(t, store)
	})
}

func TestLake_Serviceability_Store_ReplaceContributors(t *testing.T) {
	t.Parallel()

	t.Run("saves contributors to database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		contributorPK := testPK(1)

		contributors := []Contributor{
			{
				PK:   contributorPK,
				Code: "TEST",
				Name: "Test Contributor",
			},
		}

		err = store.ReplaceContributors(ctx, contributors)
		require.NoError(t, err)

		// Verify data was inserted using the dataset API
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		ds, err := NewContributorDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(contributorPK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "TEST", current["code"])
		require.Equal(t, "Test Contributor", current["name"])
	})

	t.Run("replaces existing contributors", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		contributorPK1 := testPK(1)
		contributorPK2 := testPK(2)

		contributors1 := []Contributor{
			{
				PK:   contributorPK1,
				Code: "TEST1",
				Name: "Test Contributor 1",
			},
		}

		err = store.ReplaceContributors(ctx, contributors1)
		require.NoError(t, err)

		// Wait a moment to ensure different snapshot_ts (truncated to seconds)
		time.Sleep(1100 * time.Millisecond)

		contributors2 := []Contributor{
			{
				PK:   contributorPK1,
				Code: "TEST1",
				Name: "Test Contributor 1",
			},
			{
				PK:   contributorPK2,
				Code: "TEST2",
				Name: "Test Contributor 2",
			},
		}

		err = store.ReplaceContributors(ctx, contributors2)
		require.NoError(t, err)

		// Verify both contributors exist using the dataset API
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		ds, err := NewContributorDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID1 := dataset.NewNaturalKey(contributorPK1).ToSurrogate()
		entityID2 := dataset.NewNaturalKey(contributorPK2).ToSurrogate()

		current1, err := ds.GetCurrentRow(ctx, conn, entityID1)
		require.NoError(t, err)
		require.NotNil(t, current1)

		current2, err := ds.GetCurrentRow(ctx, conn, entityID2)
		require.NoError(t, err)
		require.NotNil(t, current2)
		require.Equal(t, "TEST2", current2["code"])
	})

	t.Run("handles empty slice", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		// First insert some data
		contributorPK := testPK(1)
		contributors := []Contributor{
			{
				PK:   contributorPK,
				Code: "TEST",
				Name: "Test Contributor",
			},
		}
		err = store.ReplaceContributors(ctx, contributors)
		require.NoError(t, err)

		// Wait a moment to ensure different snapshot_ts (truncated to seconds)
		time.Sleep(1100 * time.Millisecond)

		// Then replace with empty slice (should delete all)
		err = store.ReplaceContributors(ctx, []Contributor{})
		require.NoError(t, err)

		// Verify contributor was deleted (tombstoned) using the dataset API
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		ds, err := NewContributorDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(contributorPK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.Nil(t, current, "should return nil for deleted contributor")
	})
}

func TestLake_Serviceability_Store_ReplaceDevices(t *testing.T) {
	t.Parallel()

	t.Run("saves devices to database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		devicePK := testPK(1)
		contributorPK := testPK(2)
		metroPK := testPK(3)

		devices := []Device{
			{
				PK:            devicePK,
				Status:        "activated",
				DeviceType:    "hybrid",
				Code:          "DEV001",
				PublicIP:      "192.168.1.1",
				ContributorPK: contributorPK,
				MetroPK:       metroPK,
			},
		}

		err = store.ReplaceDevices(ctx, devices)
		require.NoError(t, err)

		// Verify data was inserted using the dataset API
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		ds, err := NewDeviceDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(devicePK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "activated", current["status"])
		require.Equal(t, "hybrid", current["device_type"])
		require.Equal(t, "DEV001", current["code"])
		require.Equal(t, "192.168.1.1", current["public_ip"])
		require.Equal(t, contributorPK, current["contributor_pk"])
		require.Equal(t, metroPK, current["metro_pk"])
	})
}

func TestLake_Serviceability_Store_ReplaceUsers(t *testing.T) {
	t.Parallel()

	t.Run("saves users to database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		userPK := testPK(1)
		ownerPubkey := testPK(2)
		devicePK := testPK(3)

		users := []User{
			{
				PK:          userPK,
				OwnerPubkey: ownerPubkey,
				Status:      "activated",
				Kind:        "ibrl",
				ClientIP:    net.IP{10, 0, 0, 1},
				DZIP:        net.IP{10, 0, 0, 2},
				DevicePK:    devicePK,
			},
		}

		err = store.ReplaceUsers(ctx, users)
		require.NoError(t, err)

		// Verify data was inserted using the dataset API
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		ds, err := NewUserDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(userPK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, ownerPubkey, current["owner_pubkey"])
		require.Equal(t, "activated", current["status"])
		require.Equal(t, "ibrl", current["kind"])
		require.Equal(t, "10.0.0.1", current["client_ip"])
		require.Equal(t, "10.0.0.2", current["dz_ip"])
		require.Equal(t, devicePK, current["device_pk"])
	})

	t.Run("persists onchain BGP status through to dz_users_current", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		userPK := testPK(10)

		users := []User{
			{
				PK:          userPK,
				OwnerPubkey: testPK(11),
				Status:      "activated",
				Kind:        "multicast",
				ClientIP:    net.IP{10, 0, 0, 1},
				DZIP:        net.IP{10, 0, 0, 2},
				DevicePK:    testPK(12),
				TunnelID:    505,
				BgpStatus:   "down",
			},
		}

		err = store.ReplaceUsers(ctx, users)
		require.NoError(t, err)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		// The dz_users_current view (recreated by the migration) must expose bgp_status.
		rows, err := conn.Query(ctx,
			"SELECT bgp_status FROM dz_users_current WHERE pk = ?", userPK)
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next(), "expected a dz_users_current row for the user")
		var bgpStatus string
		require.NoError(t, rows.Scan(&bgpStatus))
		require.Equal(t, "down", bgpStatus)
	})

	// Guards against the SCD-churn concern from review: only bgp_status is
	// ingested (not the ~6-hourly last_bgp_reported_at keepalive slot), so a
	// re-ingest with unchanged BGP state must NOT write a new history row. A
	// real bgp_status transition must.
	t.Run("bgp_status change detection does not churn history", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		ctx := t.Context()

		store, err := NewStore(StoreConfig{Logger: laketesting.NewLogger(), ClickHouse: db})
		require.NoError(t, err)

		userPK := testPK(20)
		mk := func(bgp string) []User {
			return []User{{
				PK: userPK, OwnerPubkey: testPK(21), Status: "activated", Kind: "multicast",
				ClientIP: net.IP{10, 0, 1, 1}, DZIP: net.IP{10, 0, 1, 2}, DevicePK: testPK(22),
				TunnelID: 506, BgpStatus: bgp,
			}}
		}

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		entityID := dataset.NewNaturalKey(userPK).ToSurrogate()
		historyRows := func() uint64 {
			r, err := conn.Query(ctx, "SELECT count() FROM dim_dz_users_history WHERE entity_id = ?", entityID)
			require.NoError(t, err)
			defer r.Close()
			require.True(t, r.Next())
			var n uint64
			require.NoError(t, r.Scan(&n))
			return n
		}

		require.NoError(t, store.ReplaceUsers(ctx, mk("up")))
		require.EqualValues(t, 1, historyRows())

		// Same BGP state re-ingested → no new row (attrs_hash unchanged).
		require.NoError(t, store.ReplaceUsers(ctx, mk("up")))
		require.EqualValues(t, 1, historyRows(), "unchanged bgp_status must not churn a new history row")

		// Real transition → exactly one new row.
		require.NoError(t, store.ReplaceUsers(ctx, mk("down")))
		require.EqualValues(t, 2, historyRows(), "bgp_status transition must write one new history row")
	})
}

func TestLake_Serviceability_Store_ReplaceLinks(t *testing.T) {
	t.Parallel()

	t.Run("saves links to database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		linkPK := testPK(1)
		contributorPK := testPK(2)
		sideAPK := testPK(3)
		sideZPK := testPK(4)

		links := []Link{
			{
				PK:                linkPK,
				Status:            "activated",
				Code:              "LINK001",
				TunnelNet:         "10.0.0.0/24",
				ContributorPK:     contributorPK,
				SideAPK:           sideAPK,
				SideZPK:           sideZPK,
				SideAIfaceName:    "eth0",
				SideZIfaceName:    "eth1",
				LinkType:          "WAN",
				CommittedRTTNs:    1000000,
				CommittedJitterNs: 50000,
				Bandwidth:         10000000000, // 10Gbps
			},
		}

		err = store.ReplaceLinks(ctx, links)
		require.NoError(t, err)

		// Verify data was inserted by querying the current table
		conn, err := db.Conn(ctx)
		require.NoError(t, err)

		ds, err := NewLinkDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(linkPK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "activated", current["status"])
		require.Equal(t, "LINK001", current["code"])
	})
}

func TestLake_Serviceability_Store_ReplaceMetros(t *testing.T) {
	t.Parallel()

	t.Run("saves metros to database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		metroPK := testPK(1)

		metros := []Metro{
			{
				PK:        metroPK,
				Code:      "NYC",
				Name:      "New York",
				Longitude: -74.0060,
				Latitude:  40.7128,
			},
		}

		err = store.ReplaceMetros(ctx, metros)
		require.NoError(t, err)

		// Verify data was inserted by querying the current table
		conn, err := db.Conn(ctx)
		require.NoError(t, err)

		ds, err := NewMetroDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(metroPK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "NYC", current["code"])
		require.Equal(t, "New York", current["name"])
	})
}

func TestLake_Serviceability_Store_GetCurrentContributor(t *testing.T) {
	t.Parallel()

	t.Run("gets current contributor from database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		contributorPK := testPK(1)

		contributors := []Contributor{
			{
				PK:   contributorPK,
				Code: "TEST",
				Name: "Test Contributor",
			},
		}

		err = store.ReplaceContributors(ctx, contributors)
		require.NoError(t, err)

		// Get current entity using the dataset API
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		ds, err := NewContributorDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(contributorPK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "TEST", current["code"])
		require.Equal(t, "Test Contributor", current["name"])
	})

	t.Run("returns nil for non-existent contributor", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)

		nonExistentPK := testPK(999)
		ds, err := NewContributorDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(nonExistentPK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.Nil(t, current)
	})
}

func TestLake_Serviceability_Store_GetCurrentDevice(t *testing.T) {
	t.Parallel()

	t.Run("gets current device from database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		devicePK := testPK(1)
		contributorPK := testPK(2)
		metroPK := testPK(3)

		devices := []Device{
			{
				PK:            devicePK,
				Status:        "activated",
				DeviceType:    "hybrid",
				Code:          "DEV001",
				PublicIP:      "192.168.1.1",
				ContributorPK: contributorPK,
				MetroPK:       metroPK,
				MaxUsers:      100,
			},
		}

		err = store.ReplaceDevices(ctx, devices)
		require.NoError(t, err)

		// Get current entity using the dataset API
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		ds, err := NewDeviceDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(devicePK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "activated", current["status"])
		require.Equal(t, "hybrid", current["device_type"])
		require.Equal(t, "DEV001", current["code"])
		require.Equal(t, "192.168.1.1", current["public_ip"])
		require.Equal(t, contributorPK, current["contributor_pk"])
		require.Equal(t, metroPK, current["metro_pk"])
		require.Equal(t, int32(100), current["max_users"])
	})
}

func TestLake_Serviceability_Store_GetCurrentUser(t *testing.T) {
	t.Parallel()

	t.Run("gets current user from database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		userPK := testPK(1)
		ownerPubkey := testPK(2)
		devicePK := testPK(3)

		users := []User{
			{
				PK:          userPK,
				OwnerPubkey: ownerPubkey,
				Status:      "activated",
				Kind:        "ibrl",
				ClientIP:    net.IP{10, 0, 0, 1},
				DZIP:        net.IP{10, 0, 0, 2},
				DevicePK:    devicePK,
				TunnelID:    42,
			},
		}

		err = store.ReplaceUsers(ctx, users)
		require.NoError(t, err)

		// Get current entity using the dataset API
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		ds, err := NewUserDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(userPK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, ownerPubkey, current["owner_pubkey"])
		require.Equal(t, "activated", current["status"])
		require.Equal(t, "ibrl", current["kind"])
		require.Equal(t, "10.0.0.1", current["client_ip"])
		require.Equal(t, "10.0.0.2", current["dz_ip"])
		require.Equal(t, devicePK, current["device_pk"])
		require.Equal(t, int32(42), current["tunnel_id"])
	})
}

func TestLake_Serviceability_Store_GetCurrentLink(t *testing.T) {
	t.Parallel()

	t.Run("gets current link from database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		linkPK := testPK(1)
		contributorPK := testPK(2)
		sideAPK := testPK(3)
		sideZPK := testPK(4)

		links := []Link{
			{
				PK:                  linkPK,
				Status:              "activated",
				Code:                "LINK001",
				TunnelNet:           "10.0.0.0/24",
				ContributorPK:       contributorPK,
				SideAPK:             sideAPK,
				SideZPK:             sideZPK,
				SideAIfaceName:      "eth0",
				SideZIfaceName:      "eth1",
				LinkType:            "WAN",
				CommittedRTTNs:      1000000,
				CommittedJitterNs:   50000,
				Bandwidth:           10000000000, // 10Gbps
				ISISDelayOverrideNs: 10,
			},
		}

		err = store.ReplaceLinks(ctx, links)
		require.NoError(t, err)

		// Get current entity using the dataset API
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		ds, err := NewLinkDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(linkPK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "activated", current["status"])
		require.Equal(t, "LINK001", current["code"])
		require.Equal(t, "10.0.0.0/24", current["tunnel_net"])
		require.Equal(t, contributorPK, current["contributor_pk"])
		require.Equal(t, sideAPK, current["side_a_pk"])
		require.Equal(t, sideZPK, current["side_z_pk"])
		require.Equal(t, "eth0", current["side_a_iface_name"])
		require.Equal(t, "eth1", current["side_z_iface_name"])
		require.Equal(t, "WAN", current["link_type"])
		require.Equal(t, int64(1000000), current["committed_rtt_ns"])
		require.Equal(t, int64(50000), current["committed_jitter_ns"])
		require.Equal(t, int64(10000000000), current["bandwidth_bps"])
		require.Equal(t, int64(10), current["isis_delay_override_ns"])
	})
}

func TestLake_Serviceability_Store_GetCurrentMetro(t *testing.T) {
	t.Parallel()

	t.Run("gets current metro from database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()
		ctx := t.Context()

		store, err := NewStore(StoreConfig{
			Logger:     log,
			ClickHouse: db,
		})
		require.NoError(t, err)

		metroPK := testPK(1)

		metros := []Metro{
			{
				PK:        metroPK,
				Code:      "NYC",
				Name:      "New York",
				Longitude: -74.0060,
				Latitude:  40.7128,
			},
		}

		err = store.ReplaceMetros(ctx, metros)
		require.NoError(t, err)

		// Get current entity using the dataset API
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		ds, err := NewMetroDataset(log)
		require.NoError(t, err)
		require.NotNil(t, ds)
		entityID := dataset.NewNaturalKey(metroPK).ToSurrogate()
		current, err := ds.GetCurrentRow(ctx, conn, entityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "NYC", current["code"])
		require.Equal(t, "New York", current["name"])
		require.InDelta(t, -74.0060, current["longitude"], 0.0001)
		require.InDelta(t, 40.7128, current["latitude"], 0.0001)
	})
}
