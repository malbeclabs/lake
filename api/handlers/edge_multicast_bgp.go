package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// The DZD column: what the DoubleZero device the publisher attaches to says about that publisher's
// BGP session, read from device telemetry rather than from the ledger.
//
// # Why this is not the ledger's bgp_status
//
// dz_users_current.bgp_status is an onchain field written by the client agent and read out of a
// ledger snapshot, so it is minutes old and it is one word. This is the device's own view, sampled
// on the telemetry cadence (30s fresh on mainnet), and it carries the two things the one word
// cannot: how long the session has been up, and how many times it has bounced. Both are on the
// line because a session that has flapped 266 times is a different call to action from one that
// came up once and stayed, and the ledger reports them identically as 'up'.
//
// The two are shown side by side rather than one replacing the other. They can legitimately
// disagree — different planes, different clocks — and the same reasoning that keeps BGP out of the
// publisher verdict keeps this out of it too.
//
// # The join
//
// User sessions live in the `vrf1` network instance as EXTERNAL peers, addressed on a link-local
// /31 that appears nowhere else in this schema. What makes them addressable is `description`,
// which the device sets to `USER-<tunnel_id>`. Keyed with the device pubkey, that is exactly the
// (device, tunnel) pair every publisher line already carries.
//
// # What this is NOT
//
// It is not latency. There is no client-to-device RTT anywhere in lake or in the telemetry mirror:
// fact_dz_device_link_latency is device-to-device across the backbone, fact_dz_internet_metro_latency
// is metro-to-metro over the public internet, and the telemetry tables carry interface, ISIS,
// transceiver and BGP state with no timing of the access path at all. Putting a last-mile RTT on
// this column needs the producer side to measure and export it first.

// edgeMulticastBGPUserPeerVRF is the network instance user sessions are peered in, and
// edgeMulticastBGPUserDescPrefix is the description the device writes for them.
const (
	edgeMulticastBGPUserPeerVRF    = "vrf1"
	edgeMulticastBGPUserDescPrefix = "USER-"
)

// EdgeMulticastBGPSession is the device's view of one publisher's BGP session.
type EdgeMulticastBGPSession struct {
	// State is the device's session state: 'ESTABLISHED', 'ACTIVE', 'CONNECT', 'IDLE'.
	State string `json:"state"`

	// Flaps is established_transitions, a monotonic count over the session's life on this
	// device. It is a total, never a rate — a large number on a session that has been up for
	// months says less than a small one on a session that came up this morning, which is why
	// EstablishedAt travels with it.
	Flaps uint64 `json:"flaps"`

	// EstablishedAt is when the session last came up, nil when the device has never established
	// it. Absent rather than zero: a session that never came up must not render as one that
	// came up at the Unix epoch.
	EstablishedAt *time.Time `json:"established_at,omitempty"`

	// ObservedAt is the telemetry sample this came from. Per row, because the sample is per
	// device and a device that stopped reporting must age on its own line rather than borrow
	// the freshness of the fleet.
	ObservedAt time.Time `json:"observed_at"`
}

// edgeMulticastBGPKey addresses one session the way the device does.
type edgeMulticastBGPKey struct {
	devicePK string
	tunnelID int32
}

// queryEdgeMulticastBGPSessions reads the device-side session for every publisher on the page.
//
// One round trip for the whole fleet, filtered to the user VRF. An absent telemetry mirror yields
// nothing rather than an error — the same contract every other additive signal on this page has,
// and the mirror is absent in local dev and in every test that does not create it.
func (a *API) queryEdgeMulticastBGPSessions(ctx context.Context) (map[edgeMulticastBGPKey]EdgeMulticastBGPSession, error) {
	tdb := quoteClickHouseIdent(TelemetryDatabaseForEnv(EnvFromContext(ctx)))

	var exists uint8
	if err := a.envDB(ctx).QueryRow(ctx, fmt.Sprintf("EXISTS TABLE %s.bgp_neighbors_latest", tdb)).Scan(&exists); err != nil {
		return nil, err
	}
	if exists != 1 {
		return nil, nil
	}

	// toInt32OrNull on the description suffix rather than a join against the ledger's tunnel
	// ids: the device is the authority on what it called the peer, and a description that is
	// not USER-<number> is not a user session and drops out here instead of matching a tunnel
	// by accident.
	query := fmt.Sprintf(`
		SELECT
			device_pubkey,
			toInt32OrNull(substring(description, %[2]d)) AS tunnel_id,
			session_state,
			established_transitions,
			last_established,
			timestamp
		FROM %[1]s.bgp_neighbors_latest
		WHERE network_instance = '%[3]s'
			AND peer_type = 'EXTERNAL'
			AND startsWith(description, '%[4]s')
			AND tunnel_id IS NOT NULL
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0`,
		tdb, len(edgeMulticastBGPUserDescPrefix)+1, edgeMulticastBGPUserPeerVRF, edgeMulticastBGPUserDescPrefix)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	metrics.RecordClickHouseQuery("edge_multicast_bgp_sessions", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[edgeMulticastBGPKey]EdgeMulticastBGPSession{}
	for rows.Next() {
		var devicePK string
		var tunnelID *int32
		var lastEstablished int64
		var s EdgeMulticastBGPSession
		if err := rows.Scan(&devicePK, &tunnelID, &s.State, &s.Flaps, &lastEstablished, &s.ObservedAt); err != nil {
			return nil, err
		}
		if tunnelID == nil {
			continue
		}
		// Nanoseconds since the Unix epoch, and zero for a session the device has never
		// brought up.
		if lastEstablished > 0 {
			at := time.Unix(0, lastEstablished).UTC()
			s.EstablishedAt = &at
		}
		s.ObservedAt = s.ObservedAt.UTC()
		out[edgeMulticastBGPKey{devicePK: devicePK, tunnelID: *tunnelID}] = s
	}
	return out, rows.Err()
}
