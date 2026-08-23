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
// # The RTT beside it
//
// The access-path latency does NOT come from here — the telemetry mirror carries no timing of the
// user tunnel. It comes from the serviceability User account, where the client agent writes the
// smoothed BGP TCP RTT it reads out of the kernel's tcp_info for the BGP socket, and lake keeps it
// in fact_dz_user_bgp_rtt (see the indexer migration for why a fact and not a dimension column).
//
// So the two halves of this column are two different reports of one session, from opposite ends:
// the device says whether the session is established and how often it has bounced, the client says
// how far away the device is. They age very differently — telemetry is ~30s, the onchain report is
// written on a status change or a ~6-hourly keepalive — which is why the RTT carries its own
// observation time and must be read as a property of the path rather than as a live signal.

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

// EdgeMulticastBGPRtt is the client agent's own report of the round trip to its device.
type EdgeMulticastBGPRtt struct {
	// Nanos is the smoothed BGP TCP RTT. Reported as zero by the contract when the session is
	// down, and such a report is not surfaced — see queryEdgeMulticastBGPRtt.
	Nanos uint64 `json:"nanos"`

	// ObservedAt is when the indexer first saw this report, not when the agent took the
	// measurement. The gap between them is bounded by the agent's keepalive, not by the poll
	// loop, so this can legitimately be hours old and the UI has to say so.
	ObservedAt time.Time `json:"observed_at"`

	// Status is the session state the same report carried, so a reader can tell a live
	// measurement from the last one taken before a flap.
	Status string `json:"status"`
}

// queryEdgeMulticastBGPRtt reads the newest onchain RTT report per user.
//
// Keyed by user pk rather than by (device, tunnel): this is the ledger's own account and the pk is
// what identifies it, where the telemetry side has only the device's free-text description.
//
// A report whose session was down carries a cleared RTT, and it is dropped here rather than shown
// as 0.00 ms. The fact keeps it — the report happened and the series should show the session
// going down — but a zero on this column would read as an impossibly fast path.
func (a *API) queryEdgeMulticastBGPRtt(ctx context.Context) (map[string]EdgeMulticastBGPRtt, error) {
	var exists uint8
	if err := a.envDB(ctx).QueryRow(ctx, "EXISTS TABLE dz_user_bgp_rtt_current").Scan(&exists); err != nil {
		return nil, err
	}
	if exists != 1 {
		return nil, nil
	}

	query := `
		SELECT user_pk, bgp_rtt_ns, bgp_status, event_ts
		FROM dz_user_bgp_rtt_current
		WHERE bgp_rtt_ns > 0
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	metrics.RecordClickHouseQuery("edge_multicast_bgp_rtt", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]EdgeMulticastBGPRtt{}
	for rows.Next() {
		var userPK string
		var r EdgeMulticastBGPRtt
		if err := rows.Scan(&userPK, &r.Nanos, &r.Status, &r.ObservedAt); err != nil {
			return nil, err
		}
		r.ObservedAt = r.ObservedAt.UTC()
		out[userPK] = r
	}
	return out, rows.Err()
}
