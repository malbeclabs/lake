package handlers

import (
	"bytes"
	"context"
	"net"
	"sort"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// Per-publisher detail for the edge multicast overview, and the two measurements the feed's
// verdict is built from.
//
// The group-level counts answer "is anyone sending". They cannot answer the question an operator
// actually asks about a sold feed — "is EVERY publisher sending, and are our own recorders all
// hearing it" — because both halves of that are per member and the rollup averages them away. A
// feed with one live publisher and one dead one reads identical to a feed with two live ones.
//
// So this file adds two per-member reads:
//
//   - PUBLISHER LINES, one row per (group, publisher), from the ledger LEFT JOINed onto the rate
//     view. From the ledger and not from the rate view so a publisher nothing measured still gets
//     a line — status 'unknown' — instead of silently leaving the list short.
//   - CAPTURE-NODE PARITY, from the application plane (see edge_multicast_lastheard.go), which is
//     the only plane with a per-(node, group) signal at all.
//
// # Why the recorder half cannot come from the counters
//
// The obvious implementation — compare each recorder's egress against the group's ingress — is
// unusable here, and not by a small margin. Interface counters are per tunnel, and a recorder
// subscribed to several groups from one tunnel reports the sum for all of them against each: on
// mainnet the Tokyo recorder reads 232 Mbps on edge-kalshi-perps-mbp, whose entire ingress is
// 3.6 Mbps, because the same tunnel also carries the sports feeds. Every Kalshi recorder is
// multi-group, so a counter-based comparison would be either permanently red or permanently
// unattributable on exactly the feeds this check exists for.
//
// The application plane has no such problem. kalshi_bbo_observations carries
// (measurement_node_id, capture source) and the shredder race summary carries (host, feed), so a
// count over the window is genuinely per node and per group. Recorders on one group all receive
// the same feed, so their sample counts should agree; comparing each node against the group's
// MEDIAN — not against the publisher's rate, which is a different unit on a different plane — is
// what makes "similar traffic" a checkable statement.

// edgeMulticastPublisherFloorBps is the rate a publisher has to clear before the feed can read
// healthy: 1 Kbps, measured on the publisher's own tunnel counter.
//
// It is a liveness floor, not a capacity target. The feeds on this page run three to four orders
// of magnitude above it (the Kalshi perps publishers sit at 2.4-2.6 Mbps), so anything at or
// under a kilobit is a tunnel carrying protocol overhead and no product — which the old verdict
// scored identically to a healthy publisher, because it only asked whether the counter was
// non-zero.
//
// It applies to EVERY publisher, which on the validator fan-in groups is a deliberate change of
// what the badge says: measured on mainnet, edge-solana-shreds has 767 publishers, 605 above the
// floor, 3 thin and 149 idle, so it reads 'thin' as its steady state where it used to read
// healthy. That is the true statement about those publishers. Softening it to a share threshold is
// a product decision, not a default to slip in here — the counts stay strict either way.
const edgeMulticastPublisherFloorBps = 1000

// edgeMulticastNodeParityFloor is how far below its peers a recording node may fall before the
// feed is called skewed: less than half the group's median sample count.
//
// Deliberately wide. Capture windows do not start together, a node that came up mid-window is
// legitimately short, and the Solana groups fold ten hosts racing at their own pace. A tight band
// would fire on all of that; half the median is a node that is missing most of the feed, which is
// the only version of this worth putting on a badge.
const edgeMulticastNodeParityFloor = 0.5

// edgeMulticastMinParityNodes is the smallest node count that can produce a parity verdict. One
// node has nothing to be compared against, and calling that feed skewed — or healthy — on the
// strength of a single sample would be a claim the data does not carry.
const edgeMulticastMinParityNodes = 2

// edgeMulticastPublisherLineCap bounds how many publisher lines one group returns.
//
// edge-solana-shreds has 768 publishers; enumerating them per group would multiply the payload
// (and the page-cache entry) by three orders of magnitude for a list nobody scrolls. The lines
// are sorted worst-first, so the cap keeps exactly the ones an operator opened the page for, and
// PublisherLinesTotal reports what was left out. The VERDICT is never affected: it is computed
// over every publisher before the cap is applied, and so is the sequence attribution.
//
// It does not reach the groups that carry a recorded sequence series: those are the market-by-price
// groups, two publishers each. The shreds groups, which are the only ones the cap bites on, run
// Turbine rather than the Edge wire protocol and have no series at all.
const edgeMulticastPublisherLineCap = 12

// The four states a publisher line can carry. 'thin' is the state this file exists to name: a
// publisher whose counter is non-zero and below the floor, which every earlier version of the
// page counted as active.
const (
	edgeMulticastPubPublishing = "publishing"
	edgeMulticastPubThin       = "thin"
	edgeMulticastPubIdle       = "idle"
	edgeMulticastPubUnknown    = "unknown"
)

// EdgeMulticastPublisher is one publisher of one group, as its own line.
type EdgeMulticastPublisher struct {
	UserPK     string `json:"user_pk"`
	ClientIP   string `json:"client_ip"`
	DZIP       string `json:"dz_ip,omitempty"`
	DeviceCode string `json:"device_code,omitempty"`
	TunnelID   int32  `json:"tunnel_id"`

	// Class is whose box it is, resolved the same way the subscriber split is — see
	// edge_multicast_class.go. 'customer' here carries the same weak claim it does there:
	// nobody has said otherwise.
	Class string `json:"class"`

	// Bps is the publisher's measured send rate, nil when nothing measured it. An upper bound
	// when MultiGroup is set: one tunnel, several groups, one counter.
	Bps *float64 `json:"bps"`

	// Status is 'publishing' (at or above the floor), 'thin' (non-zero and below it), 'idle'
	// (measured at zero) or 'unknown' (no counter row).
	Status string `json:"status"`

	// MultiGroup is true when this publisher feeds more than one group from this account, which
	// is what makes Bps un-attributable to this group alone.
	MultiGroup bool `json:"multi_group"`

	// devicePK is unexported: it is the join key for the device-side BGP session and has no
	// business on the wire, where DeviceCode is the readable form of the same thing.
	devicePK string

	// BGPSession is what the device itself says about this publisher's session, nil when the
	// telemetry mirror is absent or has no row for the (device, tunnel) pair. Distinct from
	// BGPStatus above, which is the ledger's word: see edge_multicast_bgp.go for why both.
	BGPSession *EdgeMulticastBGPSession `json:"bgp_session,omitempty"`

	// BGPRtt is the client agent's own report of the round trip to its device — the only
	// measurement of the access path that exists. Nil when no report carries a live RTT.
	BGPRtt *EdgeMulticastBGPRtt `json:"bgp_rtt,omitempty"`

	// MsgPerSec is what the recorders actually received from this path, per second over the
	// observation window, nil when no recorder saw it. Unlike Bps it is per group rather than
	// per tunnel, so it is the one delivery figure on the line that needs no caveat.
	MsgPerSec *float64 `json:"msg_per_sec,omitempty"`

	// PathParity is this publisher measured against the other paths of the same feed, nil when
	// there was no peer to compare it with. See edgeMulticastPathParity for why this reaches
	// feeds that capture-node parity structurally cannot.
	PathParity *EdgeMulticastPathParity `json:"path_parity,omitempty"`

	// Health is this publisher's own verdict, and the reason the group row no longer carries one:
	// a feed with one dead publisher and one live one rolls up to a single badge that describes
	// neither. Worst-of over the signals that belong to THIS member — see
	// edgeMulticastPublisherHealth for the ranking and for what is deliberately left out of it.
	Health string `json:"health,omitempty"`

	// BGPStatus is the ledger's view of this publisher's BGP session: 'up', 'down' or 'unknown'.
	//
	// On a PUBLISHER, 'down' is a fault and the line says so. That is not a contradiction of the
	// rule that the control-plane roll-up must not paint the row: what that rule rejects is a
	// worst-of over every MEMBER, where a customer with BGP down — which is the customer's own
	// problem and the steady state for dozens of them — turned every group red. A publisher with
	// no session cannot be sending the feed it is registered to send, and there is exactly one
	// line it belongs on.
	//
	// It deliberately does not move the group's verdict. What the counters measure is whether
	// traffic is flowing, and a publisher can read 'down' here while its tunnel is still moving
	// bytes in the last visible bucket — the ledger snapshot and the counter rollup are minutes
	// apart. So the two are reported side by side on the line and the reader gets both.
	BGPStatus string `json:"bgp_status,omitempty"`

	// ObservedAt is the bucket behind Bps. Per line rather than per group: one stale publisher
	// among fresh ones is a fact about that publisher.
	ObservedAt *time.Time `json:"observed_at,omitempty"`

	// Sequence is this publisher's own recorded sequence series — the grain a series has, since
	// one belongs to one path and two paths cannot share a counter. Nil when no recorder wrote
	// anything from this publisher's address, which is every group without a capture behind it.
	// See edge_multicast_sequence.go.
	Sequence *EdgeMulticastSequenceHealth `json:"sequence,omitempty"`
}

// EdgeMulticastCaptureNode is one recording node's view of one group on the application plane.
type EdgeMulticastCaptureNode struct {
	Node string `json:"node"`

	// Samples is what this node wrote down for this group inside the window: BBO observations
	// for a Kalshi feed, race rows for a Solana one. It is a comparison unit between nodes on
	// the same group and nothing else — never a rate, and never comparable between groups.
	Samples uint64 `json:"samples"`

	LastHeard time.Time `json:"last_heard"`

	// ShareOfMedian is Samples over the median of the group's nodes. 1.0 is the typical node;
	// the median rather than the max so one node running hot does not indict the rest.
	ShareOfMedian float64 `json:"share_of_median"`

	// Lagging is ShareOfMedian below edgeMulticastNodeParityFloor with enough peers to say so.
	Lagging bool `json:"lagging"`
}

// edgeMulticastPublisherStats is the floor check over every publisher of one group, taken before
// the line cap so the verdict cannot be changed by how many lines the payload carries.
type edgeMulticastPublisherStats struct {
	publishing int
	thin       int
	idle       int
	unknown    int
}

// measured is how many publishers the counter plane had anything to say about.
func (s edgeMulticastPublisherStats) measured() int {
	return s.publishing + s.thin + s.idle
}

// belowFloor is how many publishers exist that are not clearing the floor. Idle counts: zero is
// below a kilobit.
func (s edgeMulticastPublisherStats) belowFloor() int {
	return s.thin + s.idle
}

// edgeMulticastPublisherStatus grades one publisher's measured rate against the floor.
func edgeMulticastPublisherStatus(bps *float64) string {
	switch {
	case bps == nil:
		return edgeMulticastPubUnknown
	case *bps >= edgeMulticastPublisherFloorBps:
		return edgeMulticastPubPublishing
	case *bps > 0:
		return edgeMulticastPubThin
	}
	return edgeMulticastPubIdle
}

// edgeMulticastPublisherStatsOf tallies a group's lines.
func edgeMulticastPublisherStatsOf(lines []EdgeMulticastPublisher) edgeMulticastPublisherStats {
	var out edgeMulticastPublisherStats
	for _, l := range lines {
		switch l.Status {
		case edgeMulticastPubPublishing:
			out.publishing++
		case edgeMulticastPubThin:
			out.thin++
		case edgeMulticastPubIdle:
			out.idle++
		default:
			out.unknown++
		}
	}
	return out
}

// queryEdgeMulticastPublisherLines reads one row per (group, publisher) for the groups given.
//
// The ledger is the driving side and the rate view is joined onto it, which is what makes the
// list complete: a publisher with no counter row is a line with status 'unknown' rather than a
// missing line. Scoped to the page's own group PKs so this stays a bounded read — unscoped it
// would enumerate every multicast publisher in the ledger, and edge-solana-shreds alone has 768.
//
// Only mode 'P' rows are joined. A P+S row's rate is EGRESS — the view picks the direction by
// mode — so joining it here would report a member's receive rate as its publish rate, the same
// inversion queryEdgeMulticastRates documents at length. Those publishers land on 'unknown',
// which is the true statement about their send side.
//
// The has_rate sentinel carries "was there a row at all", and it cannot be replaced by a NULL
// check. ClickHouse fills an unmatched LEFT JOIN row with the column's DEFAULT rather than NULL —
// 0 for a Float64 — so an unmeasured publisher would arrive as a hard zero and be graded 'idle',
// which is the difference between "nothing measured this" and "this stopped sending". A literal 1
// in the joined subquery survives that: 0 back means no row, whatever the column types are.
//
// join_use_nulls = 1 would express the same thing in one line and MUST NOT be used here. The
// setting applies to the whole query including the joins inside health_multicast_user_rate, and
// that view joins on a has(...) expression over both sides, which the setting rejects outright:
// "LEFT JOIN ON expression ... contains column from left and right table, which is not supported
// with join_use_nulls" (code 403, reproduced against mainnet).
func (a *API) queryEdgeMulticastPublisherLines(ctx context.Context, groupPKs []string, classes multicastMemberClasses) (map[string][]EdgeMulticastPublisher, error) {
	out := map[string][]EdgeMulticastPublisher{}
	if len(groupPKs) == 0 {
		return out, nil
	}

	recorders := edgeMulticastKeySet(classes.recorderIPs)
	probes := edgeMulticastKeySet(classes.probeIPs)
	asserted := edgeMulticastKeySet(classes.assertedIPs)
	wallets := edgeMulticastKeySet(classes.operatorWallets)

	query := `
		WITH ledger AS (
			SELECT
				arrayJoin(JSONExtract(publishers, 'Array(String)')) AS group_pk,
				pk,
				client_ip,
				dz_ip,
				owner_pubkey,
				device_pk,
				tunnel_id,
				bgp_status,
				length(arrayDistinct(arrayConcat(
					JSONExtract(publishers, 'Array(String)'),
					JSONExtract(subscribers, 'Array(String)')
				))) AS group_span
			FROM dz_users_current
			WHERE status = 'activated' AND kind = 'multicast'
		),
		rates AS (
			SELECT
				multicast_group_pk,
				user_pk,
				1 AS has_rate,
				max(observed_bps_5m) AS bps,
				max(rate_bucket_ts) AS bucket_ts
			FROM health_multicast_user_rate
			WHERE mode = 'P'
			GROUP BY multicast_group_pk, user_pk
		)
		SELECT
			l.group_pk,
			l.pk,
			l.client_ip,
			l.dz_ip,
			l.owner_pubkey,
			l.device_pk,
			COALESCE(d.code, '') AS device_code,
			l.tunnel_id,
			l.bgp_status AS bgp_status,
			l.group_span > 1 AS multi_group,
			r.has_rate AS has_rate,
			r.bps AS bps,
			r.bucket_ts AS bucket_ts
		FROM ledger AS l
		LEFT JOIN rates AS r ON r.multicast_group_pk = l.group_pk AND r.user_pk = l.pk
		LEFT JOIN dz_devices_current AS d ON d.pk = l.device_pk
		WHERE l.group_pk IN (?)
		SETTINGS max_execution_time = 45, timeout_before_checking_execution_speed = 0
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, groupPKs)
	metrics.RecordClickHouseQuery("edge_multicast_publisher_lines", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var groupPK string
		var line EdgeMulticastPublisher
		var multiGroup bool
		var hasRate uint8
		var ownerPubkey string
		var devicePK string
		var bps *float64
		var bucket *time.Time
		if err := rows.Scan(&groupPK, &line.UserPK, &line.ClientIP, &line.DZIP, &ownerPubkey,
			&devicePK, &line.DeviceCode, &line.TunnelID, &line.BGPStatus, &multiGroup, &hasRate, &bps, &bucket); err != nil {
			return nil, err
		}
		line.MultiGroup = multiGroup
		line.devicePK = devicePK
		// No row, or a row the view itself marked no_data: both are "nothing measured this
		// publisher", and neither may be presented as a rate of zero.
		if hasRate == 1 {
			line.Bps = bps
		}
		line.Status = edgeMulticastPublisherStatus(line.Bps)
		line.Class = edgeMulticastClassOf(line.ClientIP, ownerPubkey, recorders, probes, asserted, wallets)
		// A zero bucket is a LEFT JOIN default rather than a reading, and must not be
		// rendered as "measured at the Unix epoch".
		if hasRate == 1 && bucket != nil && !bucket.IsZero() && bucket.Unix() > 0 {
			at := bucket.UTC()
			line.ObservedAt = &at
		}
		out[groupPK] = append(out[groupPK], line)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for pk := range out {
		sortEdgeMulticastPublisherLines(out[pk])
	}
	return out, nil
}

// The verdicts a publisher line can carry, worst-first. They reuse the group vocabulary where the
// meaning carries over; 'skewed' does not appear, because capture-node parity is a statement about
// the recorders of a group and no single publisher owns it.
const (
	edgeMulticastPubHealthSilent  = "silent"
	edgeMulticastPubHealthThin    = "thin"
	edgeMulticastPubHealthGapped  = "gapped"
	edgeMulticastPubHealthStalled = "stalled"
	edgeMulticastPubHealthBehind  = "behind"

	// edgeMulticastPubHealthUnrecorded is "sending, and nothing wrote down what it sent". It
	// exists because 'healthy' on this page means the floor AND an intact series, and a
	// publisher with no series has only half of that. See edgeMulticastPublisherHealth.
	edgeMulticastPubHealthUnrecorded = "unrecorded"
	edgeMulticastPubHealthUnknown    = "unknown"
	edgeMulticastPubHealthy          = "healthy"
)

// edgeMulticastPublisherHealth grades one publisher line.
//
// Ranking, worst-first: silent, thin, gapped, stalled, behind, unknown, unrecorded, healthy. A publisher moving
// no bytes outranks one moving too few, and both outrank a recorded gap: 'thin' says the tunnel is
// carrying overhead and no product, which is a larger failure than a series that lost some of a
// feed it is otherwise delivering. 'behind' sits last of the faults because it is the mildest
// statement of the four — the path is delivering, just less of the feed than its peer is.
//
// Two things are deliberately NOT folded in.
//
// BGP status. A publisher with no session cannot be sending, but the ledger snapshot and the rate
// bucket are minutes apart, so a publisher can read 'down' while its tunnel still moved bytes in
// the last visible bucket. Both are shown on the line and the reader gets both, which is the same
// call the group verdict made for the same reason.
//
// A series whose gaps were never counted. The top-of-book plane records no gap marker, so its
// instances arrive with GapsMeasured false and a zero gap count that is an absence rather than a
// reading. Such a series still reaches 'stalled', which is graded on staleness alone — but its
// zero gap count is not read as a pass, and 'healthy' over it is the weaker claim "nothing was
// found wrong in what was recorded", not "the series is intact". The badge's tooltip says which of
// the two it is, from GapsUnmeasured on the line's own series.
//
// It is deliberately not a separate verdict. There is no state between 'healthy' and a fault, and
// minting one would paint every top-of-book line permanently non-green over a property of the
// plane rather than of the path — a product decision, not a default to take here. What the page
// does instead is carry the distinction where it is measured: the Sequence column reads
// 'advancing' rather than 'ok' for those series.
//
// groupHasSeries closes the same hole one level out. 'healthy' here means the floor AND an intact
// series, so a publisher with NO series has only half of it, and returning 'healthy' anyway put a
// measured, gapping feed beside an unmeasured one and made the unmeasured one look the better of
// the two. It only applies where the group has series to be missing from: the shreds groups run
// Turbine and have no recorded wire protocol at all, so there is nothing to be uncovered by and
// 'healthy' is the whole truth there.
//
// The two tail states are not faults, and the difference between them matters: 'unknown' is no
// counter row at all, 'unrecorded' is a publisher clearing the floor that no recorder wrote a
// series for while its peers on the group have one.
//
// groupHasSubscribers is the third input, and it exists for one specific false positive. Clearing
// the floor is a statement about a TUNNEL, and a publisher that feeds several groups from one
// tunnel reports the sum against each of them (MultiGroup) — so on such a group the counter cannot
// attest that THIS group is being fed. Where the group also has no subscriber, nothing else can:
// there is no recorder, so no series and no recorded rate will ever arrive to settle it. Both
// together are 'unknown' in the sense the word already carries here — nothing measured this
// publisher on this group — and returning 'healthy' instead is a claim with no evidence under it.
//
// Measured on mainnet: edge-kalshi-elections-tob read 2/2 publishing and both lines healthy while
// its publishers sent that plane nothing at all (`[tob_perps] enabled = false` on both hosts, and
// only `feed="mbp-sports"` in their metrics); the whole ~18.6 Mbps belonged to the mbp group on the
// same two tunnels.
//
// Both conditions are load-bearing, and the second one still is now that the Solana groups are out
// of scope — the reason has changed, not gone. It was the shreds shape: 532 root publishers, all
// MultiGroup, no recorded wire protocol, legitimately green on counter-only evidence. What keeps it
// now is that "no application-plane signal" is otherwise a TRANSIENT state. The observations payload
// is a cache entry, and a newly-bumped key is empty until the refresh chain reaches it — so without
// the subscriber requirement a cold cache would turn every Kalshi publisher grey at once, since all
// of them are MultiGroup. A missing subscriber is structural: nobody receives this group, so nothing
// will ever settle it.
//
// A publisher that serves only this group HAS attributable bytes, so its 'healthy' stands even with
// no subscriber. Today the pair fires on edge-kalshi-elections-tob — the only group in scope with
// publishers and no subscriber at all.
func edgeMulticastPublisherHealth(line EdgeMulticastPublisher, groupHasSeries bool, groupHasSubscribers bool) string {
	switch line.Status {
	case edgeMulticastPubIdle:
		return edgeMulticastPubHealthSilent
	case edgeMulticastPubThin:
		return edgeMulticastPubHealthThin
	}
	// The recorded planes are read BEFORE the counter's own absence, because the ranking says so
	// and because they are independent measurements: a series gapping at a recorder is a finding
	// whether or not the rate view has a row for the tunnel that sent it. Returning 'unknown'
	// first hid exactly that case, and 'unknown' is excluded from Faulted(), so the collapsed
	// group's dot went grey over a gapping feed.
	if seq := line.Sequence; seq != nil {
		if seq.Gapped > 0 {
			return edgeMulticastPubHealthGapped
		}
		if seq.Stalled > 0 {
			return edgeMulticastPubHealthStalled
		}
	}
	// Both guards the check needs, and it is not Behind > 0: a path with no peer has no verdict,
	// and one failing pair out of thirty is an outlier rather than a finding. Recomputed from the
	// counts rather than read off PathParity.Faulted so that a line assembled anywhere — a test, a
	// future caller — cannot get a verdict the counts do not support by leaving a bool unset. The
	// payload field the page reads comes from this same function.
	if p := line.PathParity; p != nil && edgeMulticastPathParityFaulted(p.Behind, p.Compared) {
		return edgeMulticastPubHealthBehind
	}
	// Nothing measured the counter, and the recorded planes had nothing to say either. Ranked
	// below the faults and above 'healthy': not a fault, and not a clean bill of health.
	if line.Status == edgeMulticastPubUnknown {
		return edgeMulticastPubHealthUnknown
	}
	// 'unrecorded' is about a publisher that IS clearing the floor, so it sits below the unknown
	// check rather than above it: a publisher nothing measured is not "sending and unrecorded".
	if groupHasSeries && (line.Sequence == nil || len(line.Sequence.Instances) == 0) {
		return edgeMulticastPubHealthUnrecorded
	}
	// Ranked last of the tail states because it is the weakest reading of all: the counter measured
	// a tunnel this group shares, and nothing exists that could measure the group. The series and
	// rate checks are redundant under a zero subscriber count and kept anyway, so a line assembled
	// by another caller cannot reach this return while carrying evidence.
	noSignal := (line.Sequence == nil || len(line.Sequence.Instances) == 0) && line.MsgPerSec == nil
	if line.MultiGroup && !groupHasSubscribers && noSignal {
		return edgeMulticastPubHealthUnknown
	}
	return edgeMulticastPubHealthy
}

// edgeMulticastKeySet indexes a resolved class list for per-line lookups — client IPs for the
// two IP tiers, owner pubkeys for the wallet tier. The class resolution itself stays in
// edge_multicast_class.go; this is only the shape change from list to set.
func edgeMulticastKeySet(keys []string) map[string]struct{} {
	out := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		out[k] = struct{}{}
	}
	return out
}

// edgeMulticastClassOf applies the same precedence the group counts use: a recorder, then a
// probe, then an asserted row that said 'customer', then the operator wallet, then the default.
//
// The asserted check sits ahead of the wallet on purpose and is the only reason it exists here:
// a row asserting 'customer' over one of our own wallets is the escape hatch for a box we handed
// back, and without this it would fall through and be relabelled 'doublezero'. The final default
// is "nobody has said otherwise", which is why the payload also carries how many members are
// actually classified.
func edgeMulticastClassOf(clientIP, ownerPubkey string, recorders, probes, asserted, wallets map[string]struct{}) string {
	if _, ok := recorders[clientIP]; ok {
		return string(multicastMemberRecorder)
	}
	if _, ok := probes[clientIP]; ok {
		return string(multicastMemberProbe)
	}
	if _, ok := asserted[clientIP]; ok {
		return string(multicastMemberCustomer)
	}
	if _, ok := wallets[ownerPubkey]; ok {
		return string(multicastMemberDoubleZero)
	}
	return string(multicastMemberCustomer)
}

// sortEdgeMulticastPublisherLines orders lines worst-first. This is the SELECTION order, not the
// display order: it is what makes the line cap safe, because the publishers that fail the floor
// are the ones that survive truncation. The kept subset is re-sorted by client IP for display —
// see sortEdgeMulticastPublisherLinesByAddress.
//
// Within a status the lower rate sorts first, and client_ip breaks the remaining ties so the
// order is stable across refreshes — an unstable order on a 30s-polling page reshuffles rows
// under the reader's cursor.
func sortEdgeMulticastPublisherLines(lines []EdgeMulticastPublisher) {
	rank := map[string]int{
		edgeMulticastPubIdle:       0,
		edgeMulticastPubThin:       1,
		edgeMulticastPubUnknown:    2,
		edgeMulticastPubPublishing: 3,
	}
	sort.SliceStable(lines, func(i, j int) bool {
		if rank[lines[i].Status] != rank[lines[j].Status] {
			return rank[lines[i].Status] < rank[lines[j].Status]
		}
		bi, bj := 0.0, 0.0
		if lines[i].Bps != nil {
			bi = *lines[i].Bps
		}
		if lines[j].Bps != nil {
			bj = *lines[j].Bps
		}
		if bi != bj {
			return bi < bj
		}
		return lines[i].ClientIP < lines[j].ClientIP
	})
}

// edgeMulticastCaptureNodes turns one group's per-node observations into the parity view: sample
// counts, each node's share of the median, and which nodes are behind.
//
// Sorted by share ascending so the node an operator needs to look at is first, and so the cap
// applied upstream can never drop it.
func edgeMulticastCaptureNodes(obs []edgeMulticastNodeObs) []EdgeMulticastCaptureNode {
	if len(obs) == 0 {
		return nil
	}

	samples := make([]float64, 0, len(obs))
	for _, o := range obs {
		samples = append(samples, float64(o.samples))
	}
	median := edgeMulticastMedian(samples)

	out := make([]EdgeMulticastCaptureNode, 0, len(obs))
	for _, o := range obs {
		node := EdgeMulticastCaptureNode{
			Node:      o.node,
			Samples:   o.samples,
			LastHeard: o.at,
		}
		// A zero median means every node is silent. That is a feed-wide absence, not one
		// node lagging behind the others, and the app plane cannot tell it from a dead
		// publisher — so no share, and nothing marked.
		if median > 0 {
			node.ShareOfMedian = float64(o.samples) / median
			node.Lagging = len(obs) >= edgeMulticastMinParityNodes &&
				node.ShareOfMedian < edgeMulticastNodeParityFloor
		}
		out = append(out, node)
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ShareOfMedian != out[j].ShareOfMedian {
			return out[i].ShareOfMedian < out[j].ShareOfMedian
		}
		return out[i].Node < out[j].Node
	})
	return out
}

// edgeMulticastMedian is the median of an unsorted sample set. Copies before sorting: the caller
// owns its slice, and reordering a caller's data as a side effect of reading it is the kind of
// bug that surfaces three functions away.
func edgeMulticastMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

// edgeMulticastLaggingNodes counts the nodes behind their peers on one group.
func edgeMulticastLaggingNodes(nodes []EdgeMulticastCaptureNode) int {
	n := 0
	for _, node := range nodes {
		if node.Lagging {
			n++
		}
	}
	return n
}

// sortEdgeMulticastPublisherLinesByAddress puts the lines in the order a reader looks for them:
// by the box's own public address, ascending.
//
// Applied to the KEPT lines only, after the worst-first cap has chosen them. Selecting and
// displaying are two different orderings and conflating them would break one or the other — sort
// by address before the cap and truncation keeps an arbitrary twelve, with the faults as likely to
// be cut as not, while the notice underneath still claims everything dropped was above the floor.
//
// Compared as addresses and not as strings. Dotted-quad text sorts 148.51.120.152 before
// 148.51.120.6, which is exactly the pair of Kalshi publishers on one group, so a string sort
// would read as scrambled on the feeds this page exists for. Unparseable values keep a stable
// string order rather than bunching at one end.
func sortEdgeMulticastPublisherLinesByAddress(lines []EdgeMulticastPublisher) {
	sort.SliceStable(lines, func(i, j int) bool {
		a, b := net.ParseIP(lines[i].ClientIP), net.ParseIP(lines[j].ClientIP)
		if a == nil || b == nil {
			return lines[i].ClientIP < lines[j].ClientIP
		}
		if c := bytes.Compare(a.To16(), b.To16()); c != 0 {
			return c < 0
		}
		// One box can hold several users of one group. The tunnel keeps them apart, and keeps
		// the order stable across refreshes.
		return lines[i].TunnelID < lines[j].TunnelID
	})
}
