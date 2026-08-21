package handlers

import (
	"context"
	"log/slog"
	"net"
	"sort"
	"strings"
)

// Classifying a multicast group member as one of DoubleZero's own boxes or as a customer.
//
// There is no flag for this in the ledger, and the obvious inference is wrong. The first cut of
// this page called a subscriber "operator-owned" when its owner wallet also published into a
// feed-backed group — which sounds tight until you notice that solana-shreds-full IS a
// feed-backed group, so every validator publishing shreds qualified: 515 wallets on mainnet, and
// groups like mbone came out 6-for-6 false positives. Owner identity cannot carry this.
//
// What replaced it has two tiers, and neither pretends to be more than it is:
//
//   - DERIVED. edgeNodeIPs is the repo's only host→IP map, the ten Solana shred-capture boxes the
//     edge scoreboard already resolves for geoip. Those IPs all appear in dz_users_current.client_ip,
//     so they classify exactly. It covers the capture fleet and nothing else — the Kalshi and
//     Hyperliquid recorders are identified in the feeds tables by measurement_node_id, a hostname
//     with no IP anywhere in that schema and no table in lake to resolve it through. There is no
//     lossy-but-usable join to fall back on for them.
//
//   - ASSERTED. An operator row in the Postgres table multicast_member_class, keyed by client_ip.
//     This is how the market-data recorders get classified at all, and how a wrong derivation gets
//     corrected without a deploy.
//
// An asserted row always wins, including when it says 'customer' over a derived recorder: the
// derived tier is a Go literal that only a deploy can change, the asserted tier is what an
// operator can fix at 3am. A member neither tier names is counted as a customer, which is
// "nobody has said otherwise" rather than a verified fact — hence ClassAsserted / ClassDerived
// on the payload, so the UI can show how much of the split is actually known.

// multicastMemberClass is the classification of one member.
type multicastMemberClass string

const (
	multicastMemberRecorder multicastMemberClass = "recorder"
	multicastMemberProbe    multicastMemberClass = "internal_probe"
	multicastMemberCustomer multicastMemberClass = "customer"
)

// multicastMemberClasses is the resolved classification set: the two IP lists the membership
// query needs, plus the provenance lists behind them.
type multicastMemberClasses struct {
	// recorderIPs and probeIPs are post-precedence: derived recorders that an operator row
	// overrode are already removed.
	recorderIPs []string
	probeIPs    []string

	// assertedIPs is every enabled override row regardless of class; derivedIPs is the derived
	// recorder set minus anything an override already speaks for. They partition the members
	// whose classification is known from the ones that merely defaulted.
	assertedIPs []string
	derivedIPs  []string
}

// loadMulticastMemberClasses reads the operator-asserted rows and folds them together with the
// derived recorder set.
//
// Zero configured rows is not an error — it is the deliberate unseeded state and yields the
// derived tier alone. A genuine load failure (query, scan, iteration) returns an error rather
// than degrading to "nothing asserted": the page-cache worker must not overwrite a good payload
// with one that silently reclassifies every recorder as a customer. Logged at WARN, never ERROR,
// which pages on-call. Same contract as loadKalshiScoreboardEntries.
func (a *API) loadMulticastMemberClasses(ctx context.Context) (multicastMemberClasses, error) {
	asserted := map[string]multicastMemberClass{}

	if a.PgPool != nil {
		rows, err := a.PgPool.Query(ctx, `
			SELECT client_ip, class FROM multicast_member_class WHERE enabled`)
		if err != nil {
			slog.Warn("multicast member class load failed", "error", err)
			return multicastMemberClasses{}, err
		}
		defer rows.Close()
		for rows.Next() {
			var ip, class string
			if err := rows.Scan(&ip, &class); err != nil {
				slog.Warn("multicast member class scan failed", "error", err)
				return multicastMemberClasses{}, err
			}
			// The IP is inlined into a ClickHouse IN list, so anything that is not an
			// address is dropped rather than trusted. The CHECK constraint bounds the
			// length; this bounds the shape.
			if net.ParseIP(ip) == nil {
				slog.Warn("multicast member class skipped: unparseable client_ip", "client_ip", ip)
				continue
			}
			switch multicastMemberClass(class) {
			case multicastMemberRecorder, multicastMemberProbe, multicastMemberCustomer:
				asserted[ip] = multicastMemberClass(class)
			default:
				// Unreachable while the CHECK holds, and still handled: an old binary
				// must not silently count a class it does not understand as a recorder.
				slog.Warn("multicast member class skipped: unknown class", "client_ip", ip, "class", class)
			}
		}
		if err := rows.Err(); err != nil {
			slog.Warn("multicast member class iteration failed", "error", err)
			return multicastMemberClasses{}, err
		}
	}

	return resolveMulticastMemberClasses(edgeNodeRecorderIPs(), asserted), nil
}

// resolveMulticastMemberClasses applies the precedence rule. Split out from the load so the
// precedence is testable without a database.
func resolveMulticastMemberClasses(derived []string, asserted map[string]multicastMemberClass) multicastMemberClasses {
	out := multicastMemberClasses{}

	for _, ip := range derived {
		if _, overridden := asserted[ip]; overridden {
			continue
		}
		out.recorderIPs = append(out.recorderIPs, ip)
		out.derivedIPs = append(out.derivedIPs, ip)
	}
	for ip, class := range asserted {
		out.assertedIPs = append(out.assertedIPs, ip)
		switch class {
		case multicastMemberRecorder:
			out.recorderIPs = append(out.recorderIPs, ip)
		case multicastMemberProbe:
			out.probeIPs = append(out.probeIPs, ip)
		case multicastMemberCustomer:
			// Asserting 'customer' is the point of the escape hatch: it removes a box from
			// the derived recorder set. Nothing to add.
		}
	}

	// Sorted so the generated SQL is stable across calls — map iteration order would otherwise
	// change the query text every refresh and defeat ClickHouse's query cache.
	sort.Strings(out.recorderIPs)
	sort.Strings(out.probeIPs)
	sort.Strings(out.assertedIPs)
	sort.Strings(out.derivedIPs)
	return out
}

// multicastMemberIPPredicate renders an IP-set test for inlining into ClickHouse SQL. Every
// address reaching it has already passed net.ParseIP (asserted rows) or is a repo literal
// (derived rows), so there is nothing to escape — but it re-checks anyway rather than trusting
// its callers, and renders an empty set as a false constant, since `IN ()` is a syntax error.
func multicastMemberIPPredicate(col string, ips []string) string {
	quoted := make([]string, 0, len(ips))
	for _, ip := range ips {
		if net.ParseIP(ip) == nil {
			continue
		}
		quoted = append(quoted, "'"+ip+"'")
	}
	if len(quoted) == 0 {
		return "0"
	}
	return col + " IN (" + strings.Join(quoted, ",") + ")"
}

// edgeNodeRecorderIPs is the derived recorder set: the public IPs of the Solana shred-capture
// hosts. Read from edgeNodeIPs rather than copied so the two cannot drift — that map is
// maintained for the edge scoreboard's geoip enrichment, which is what makes it a usable source
// here instead of a second literal to keep in sync.
func edgeNodeRecorderIPs() []string {
	out := make([]string, 0, len(edgeNodeIPs))
	for _, ip := range edgeNodeIPs {
		out = append(out, ip)
	}
	sort.Strings(out)
	return out
}
