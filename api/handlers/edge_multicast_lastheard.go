package handlers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// Application-plane liveness for the edge multicast overview.
//
// The rest of the page measures the network: counters read off the devices, five minutes wide
// and minutes late. This file measures the product: a message that a recording node actually
// received and wrote down, which is the only signal on either plane fresh enough to answer
// "is this feed alive right now".
//
// Three things about it are load-bearing and easy to get wrong.
//
// RECEIVE-SIDE, NOT SEND-SIDE. Every timestamp here says a measurement node received something.
// A recorder outage and a publisher outage look identical. So a missing last-heard is reported
// as absent and never as silence, and it deliberately does NOT feed EdgeMulticastGroup.Silent —
// that flag stays sourced from the counters, which at least observe the publisher's own tunnel.
//
// GROUP GRAIN LOSES CAPTURE SOURCES. A Kalshi sports group carries ~33 league capture sources
// and the perps groups carry two redundant paths, all folded into one max(). A single dead league
// does not move the group's timestamp. LastHeardCaptureSources carries the fold factor so the UI
// can say so and point at /dz/kalshi/l2, which is the per-capture-source view.
//
// COVERAGE IS PARTIAL BY CONSTRUCTION. Only the Kalshi groups have a capture behind them. The lab
// and partner groups have no app-plane signal and never will, so the column is empty for them and
// the counter-derived numbers stay the whole story.
//
// There used to be a second leg here reading the shred race summary for the five Solana groups. It
// went with them when edgeMulticastExcludedPrefix took them off the page: `resolve` builds its
// index from the group list this page loaded, so every race row it returned resolved to "" and was
// dropped — a per-page-load query against the shredder database whose result reached nothing. Worse,
// it set the `available` flag, so with the Kalshi proxy absent the page would have rendered the
// column over a screen of blanks. `slot_feed_race_summary_v2` is still read by the edge scoreboard,
// which is where the shred race belongs.

// edgeMulticastLastHeard is one group's application-plane observation, folded over every capture
// source and every recording node that reported it.
type edgeMulticastLastHeard struct {
	at time.Time

	// table is the proxied table the winning timestamp came out of, so a reader can tell which
	// capture plane answered. Not a capture source id: several capture sources fold into one of
	// these.
	table          string
	captureSources int

	// srcIDs is the distinct capture sources folded in, which is what captureSources counts.
	// Kept as a set rather than a counter because the rows arrive per (capture source, node):
	// counting rows would report one capture source once per node and turn the fold factor
	// into a node count.
	srcIDs map[string]struct{}

	// nodes is the per-node breakdown, keyed by node id and summed across that node's capture
	// sources.
	// It is the only per-(node, group) signal either plane has — see the file header of
	// edge_multicast_publishers.go for why the counters cannot supply it.
	nodes map[string]edgeMulticastNodeObs
}

// edgeMulticastNodeObs is what one recording node wrote down for one group inside the window.
// Samples are comparable between nodes on the same group and nowhere else — they count whatever
// that group's capture writes down, today a BBO observation.
type edgeMulticastNodeObs struct {
	node    string
	samples uint64
	at      time.Time
}

// nodeObs returns the group's nodes in a stable order. Sorted by node id here; the parity view
// re-sorts by share once the median is known.
func (l edgeMulticastLastHeard) nodeObs() []edgeMulticastNodeObs {
	out := make([]edgeMulticastNodeObs, 0, len(l.nodes))
	for _, o := range l.nodes {
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].node < out[j].node })
	return out
}

// edgeMulticastCaptureSourceMap resolves a capture source id to a multicast group PK.
//
// Built from the group list the fetch already loaded rather than from a table of instances: the
// Kalshi capture source ids are the ledger group code with the plane suffix hoisted to the front
// (edge-kalshi-sports-mbp -> mbp_edge_kalshi_sports, then a league suffix), a convention infra
// established on purpose. Encoding the convention means a new league, or a whole new venue feed,
// maps itself the day it appears. A hardcoded list of capture source ids would instead go quietly
// stale, which on this page reads as "that feed is fine" — the failure it exists to catch.
type edgeMulticastCaptureSourceMap struct {
	exact    map[string]string // full capture source id -> group pk
	prefixes []edgeMulticastCaptureSourcePrefix

	// byMulticastIP resolves a group from its destination address. The top-of-book sequence
	// leg carries that address on every row (raw_meta.multicast_group), which is a stronger
	// key than any capture source name: it is what the datagrams were addressed to, where the
	// name is a convention that has been renamed once already.
	byMulticastIP map[string]string
}

type edgeMulticastCaptureSourcePrefix struct {
	prefix  string
	groupPK string
}

// newEdgeMulticastCaptureSourceMap registers, for every group, the ways a capture source can
// name it.
func newEdgeMulticastCaptureSourceMap(groups []MulticastDeliveryGroup) edgeMulticastCaptureSourceMap {
	m := edgeMulticastCaptureSourceMap{exact: map[string]string{}, byMulticastIP: map[string]string{}}
	for _, g := range groups {
		if g.MulticastIP != "" {
			m.byMulticastIP[g.MulticastIP] = g.PK
		}
		// A capture source may name its group outright.
		m.exact[g.Code] = g.PK

		// Driven off edgeMulticastPlanes so a plane added there is understood here too: the
		// capture source ids hoist the same suffix to the front of the code.
		for plane := range edgeMulticastPlanes {
			suffix := "-" + plane
			if !strings.HasSuffix(g.Code, suffix) {
				continue
			}
			base := strings.TrimSuffix(g.Code, suffix)
			m.prefixes = append(m.prefixes, edgeMulticastCaptureSourcePrefix{
				prefix:  plane + "_" + strings.ReplaceAll(base, "-", "_"),
				groupPK: g.PK,
			})
		}
	}
	// Longest prefix first so a more specific capture source never loses to a shorter one that
	// happens to be a prefix of it.
	sort.Slice(m.prefixes, func(i, j int) bool { return len(m.prefixes[i].prefix) > len(m.prefixes[j].prefix) })
	return m
}

// resolve returns the group pk a capture source id belongs to, or "" when nothing claims it. An
// unclaimed capture source is dropped rather than bucketed: a capture source with no group is not
// a group, and this page has no row to show it in.
// resolveMulticastIP returns the group pk that owns a destination address, or "" for an address
// no group on this page carries. Callers fall back to resolve() on the capture source name: an
// older recorder payload may not carry the address at all.
func (m edgeMulticastCaptureSourceMap) resolveMulticastIP(ip string) string {
	return m.byMulticastIP[ip]
}

func (m edgeMulticastCaptureSourceMap) resolve(captureSource string) string {
	if pk, ok := m.exact[captureSource]; ok {
		return pk
	}
	for _, p := range m.prefixes {
		if captureSource == p.prefix || strings.HasPrefix(captureSource, p.prefix+"_") {
			return p.groupPK
		}
	}
	return ""
}

// queryEdgeMulticastLastHeard collects the application-plane timestamps for every group it can.
//
// Probe-guarded. The proxied feeds table is absent in local dev and in every test that does not
// create it, and an absent table has to leave the rest of the page intact — so a leg that cannot
// run yields nothing and the caller carries on. The returned bool reports whether the leg was
// queryable, so the UI can drop the column entirely rather than render a screen of blanks. It is
// kept as a bool rather than folded into len(out) because a queryable table with no rows is a
// reading — every capture is down — where an absent one is not.
func (a *API) queryEdgeMulticastLastHeard(ctx context.Context, captureSources edgeMulticastCaptureSourceMap) (map[string]edgeMulticastLastHeard, bool, error) {
	out := map[string]edgeMulticastLastHeard{}
	available := false

	kalshiOK, err := a.kalshiObservationsTableExists(ctx)
	if err != nil {
		return nil, false, err
	}
	if kalshiOK {
		available = true
		if err := a.collectKalshiLastHeard(ctx, captureSources, out); err != nil {
			return nil, false, err
		}
	}

	return out, available, nil
}

// collectKalshiLastHeard reads the newest BBO observation per DoubleZero capture source.
//
// The window predicate is doing real work. kalshi_bbo_observations sorts by
// (measurement_node_id, symbol, source_ts_ms, source, recv_ts_ns), so it is the source_ts_ms
// bound — a venue clock in a key position — that lets the index skip granules; recv_ts_ns is
// last in the key and prunes nothing. The recv_ts_ns bound is still there because the recorder
// clock is what "last heard" means, and it is the narrower of the two: source_ts_ms is given
// three times the slack so a feed with a skewed venue clock is not pruned away and reported
// absent, which on this page would read as a fault.
func (a *API) collectKalshiLastHeard(ctx context.Context, captureSources edgeMulticastCaptureSourceMap, out map[string]edgeMulticastLastHeard) error {
	q := fmt.Sprintf(`
		SELECT source AS capture_source, measurement_node_id, count() AS samples, max(recv_ts_ns) AS last_recv_ts_ns
		FROM `+"`%s`"+`.kalshi_bbo_observations
		WHERE source_ts_ms >= toUInt64(toUnixTimestamp64Milli(now64(3) - toIntervalMinute(15)))
			AND recv_ts_ns >= toUInt64(toUnixTimestamp64Nano(now64(9) - toIntervalMinute(5)))
			AND %s
		GROUP BY capture_source, measurement_node_id
		SETTINGS max_execution_time = 20, timeout_before_checking_execution_speed = 0
	`, a.FeedsDB, kalshiIsDZSQL("source"))

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, q)
	metrics.RecordClickHouseQuery("edge_multicast_last_heard_kalshi", time.Since(start), err)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var captureSource, node string
		var samples, lastNs uint64
		if err := rows.Scan(&captureSource, &node, &samples, &lastNs); err != nil {
			return err
		}
		at := time.Unix(0, int64(lastNs)).UTC()
		mergeEdgeMulticastLastHeard(out, captureSources.resolve(captureSource), captureSource,
			at, "kalshi_bbo_observations",
			edgeMulticastNodeObs{node: node, samples: samples, at: at})
	}
	return rows.Err()
}

// mergeEdgeMulticastLastHeard folds one (capture source, node) row into its group: the newest
// timestamp wins, the capture source joins the fold set, and the node's samples accumulate across
// its capture sources. A zero-time row (a table default rather than a real observation) is
// dropped so it cannot masquerade as a reading.
//
// srcID is the capture source and obs.node is the box that heard it. Both are needed and they are
// not interchangeable: the fold factor counts capture sources, parity counts nodes, and one node
// reports every capture source of a group it captures.
func mergeEdgeMulticastLastHeard(out map[string]edgeMulticastLastHeard, groupPK, srcID string, at time.Time, table string, obs edgeMulticastNodeObs) {
	if groupPK == "" || at.IsZero() || at.Unix() <= 0 {
		return
	}
	cur, ok := out[groupPK]
	if !ok {
		cur = edgeMulticastLastHeard{
			at:     at,
			table:  table,
			srcIDs: map[string]struct{}{},
			nodes:  map[string]edgeMulticastNodeObs{},
		}
	}
	if at.After(cur.at) {
		cur.at = at
		cur.table = table
	}
	cur.srcIDs[srcID] = struct{}{}
	cur.captureSources = len(cur.srcIDs)
	if obs.node != "" {
		node := cur.nodes[obs.node]
		node.node = obs.node
		node.samples += obs.samples
		if obs.at.After(node.at) {
			node.at = obs.at
		}
		cur.nodes[obs.node] = node
	}
	out[groupPK] = cur
}
