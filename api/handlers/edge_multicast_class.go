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
// groups like mbone came out 6-for-6 false positives. What that ruled out is INFERRING ownership
// from what a wallet does; a named list of wallets is a different thing, and is tier three below.
//
// What replaced it has three tiers, and none pretends to be more than it is:
//
//   - DERIVED, by capture host. edgeNodeIPs is the repo's only host→IP map, the ten Solana
//     shred-capture boxes the edge scoreboard already resolves for geoip. Those IPs all appear in
//     dz_users_current.client_ip, so they classify exactly, and they classify as 'recorder' —
//     recording is what those boxes are for. It covers the capture fleet and nothing else: the
//     Kalshi and Hyperliquid recorders are identified in the feeds tables by measurement_node_id,
//     a hostname with no IP anywhere in that schema and no table in lake to resolve it through.
//     There is no lossy-but-usable join to fall back on for them.
//
//   - DERIVED, by operator wallet. doubleZeroOperatorWallets is an explicit allow-list of the
//     wallets DoubleZero itself runs boxes under. This is NOT the inference that was removed: that
//     one asked whether a member's owner published into any feed-backed group, which every shreds
//     validator does. This one names specific wallets, so it is an assertion in a Go literal, the
//     same kind of thing edgeNodeIPs is. It is also why this tier can only say 'doublezero' and
//     never 'recorder': one wallet holds recorders, probes and lab boxes at once, so the wallet
//     establishes whose box it is and nothing more. Measured on mainnet the two derived tiers
//     agree where they overlap — the wallet list covers all ten edgeNodeIPs hosts — and it is what
//     puts a DoubleZero label on the Kalshi receivers, which the host map structurally cannot.
//
//   - ASSERTED. An operator row in the Postgres table multicast_member_class, keyed by client_ip.
//     This is the only tier that can name a market-data box as a 'recorder' specifically, and the
//     only one that corrects a wrong derivation without a deploy.
//
// An asserted row always wins, including when it says 'customer' over a derived recorder: the
// derived tiers are Go literals that only a deploy can change, the asserted tier is what an
// operator can fix at 3am. Between the two derived tiers the capture-host map wins, because it is
// the more specific claim. A member no tier names is counted as a customer, which is "nobody has
// said otherwise" rather than a verified fact — hence ClassAsserted / ClassDerived on the payload,
// so the UI can show how much of the split is actually known.

// multicastMemberClass is the classification of one member.
type multicastMemberClass string

const (
	multicastMemberRecorder multicastMemberClass = "recorder"
	multicastMemberProbe    multicastMemberClass = "internal_probe"
	multicastMemberCustomer multicastMemberClass = "customer"

	// multicastMemberDoubleZero is "this is one of ours, and nothing has said which kind". It is
	// produced by the operator-wallet tier alone and is deliberately not an accepted value in
	// multicast_member_class: an operator who knows a box is ours knows whether it records, so
	// there is no reason to let a row assert the vaguer thing.
	multicastMemberDoubleZero multicastMemberClass = "doublezero"
)

// doubleZeroOperatorWallets are the ledger owner pubkeys DoubleZero runs its own boxes under.
//
// Named wallets, not a rule over wallets. The vanity 'DZ' prefix is not the test — a prefix is
// unenforced and a customer can mint one — and neither is any inference from what the wallet
// publishes or owns, which is the shape that produced 515 false positives before.
//
// Verified against mainnet when written (2026-08-22): these wallets own every one of the ten
// edgeNodeIPs capture hosts, all four Kalshi publishers, and the Kalshi receivers that had been
// reading as customers, while matching nothing on the customer groups (mbone, scottsdale,
// sentrynet all came back zero). A wallet that stops being ours has to be removed here.
var doubleZeroOperatorWallets = []string{
	// The capture and market-data fleet: the shred-capture hosts, the Kalshi venue publishers,
	// and the Kalshi recording boxes.
	"DZfHfcCXTLwgZeCRKQ1FL1UuwAwFAZM93g86NMYpfYan",
	// Owner of the edge-* and rebop multicast groups, and of receivers on them. Creating those
	// groups is what makes it ours; owning receivers on them is not on its own.
	"DZjZR4QB7woRRywaCab9hU5D8QHAtVnbGjFXtK9QFuam",
	// Deliberately absent: DZipSzRi9zLL4TMUn3djFC7GeowNYtTtx3ZFX3mmmkgm, which owns exactly one
	// receiver (104.131.172.104, on the two Kalshi MBP groups) and nothing else. The only thing
	// arguing for it is the vanity prefix, which is not the test. If that box is ours, an
	// asserted row is the right place to say so — and it can then say which kind it is.
}

// multicastMemberClasses is the resolved classification set: the IP lists the membership query
// needs, the wallet list behind the second derived tier, and the provenance lists behind them.
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

	// operatorWallets is the second derived tier. It stays a wallet list rather than being
	// resolved to addresses here: owner_pubkey sits on the same ledger row the membership and
	// publisher-line queries already read, so matching it costs nothing and never goes stale
	// against a rebuilt tunnel. It is applied last — a member an IP tier already speaks for is
	// not reclassified by it.
	operatorWallets []string
}

// loadMulticastMemberClasses reads the operator-asserted rows and folds them together with the
// two derived tiers.
//
// Zero configured rows is not an error — it is the deliberate unseeded state and yields the
// derived tiers alone. A genuine load failure (query, scan, iteration) returns an error rather
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

	return resolveMulticastMemberClasses(edgeNodeRecorderIPs(), asserted, doubleZeroOperatorWallets), nil
}

// resolveMulticastMemberClasses applies the precedence rule. Split out from the load so the
// precedence is testable without a database.
func resolveMulticastMemberClasses(derived []string, asserted map[string]multicastMemberClass, wallets []string) multicastMemberClasses {
	out := multicastMemberClasses{operatorWallets: append([]string(nil), wallets...)}

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
	sort.Strings(out.operatorWallets)
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

// multicastMemberWalletPredicate renders an owner-pubkey set test for inlining into ClickHouse
// SQL, and is the wallet-tier counterpart of multicastMemberIPPredicate. Every pubkey reaching it
// is a repo literal, so there is nothing to escape — and, like the IP version, it re-checks the
// shape rather than trusting that, and renders an empty set as a false constant.
func multicastMemberWalletPredicate(col string, wallets []string) string {
	quoted := make([]string, 0, len(wallets))
	for _, w := range wallets {
		if !isBase58Pubkey(w) {
			continue
		}
		quoted = append(quoted, "'"+w+"'")
	}
	if len(quoted) == 0 {
		return "0"
	}
	return col + " IN (" + strings.Join(quoted, ",") + ")"
}

// isBase58Pubkey reports whether s is shaped like a Solana address. Length bounds plus the base58
// alphabet, which excludes the quote and backslash characters that would matter here.
func isBase58Pubkey(s string) bool {
	if len(s) < 32 || len(s) > 44 {
		return false
	}
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	for _, r := range s {
		if !strings.ContainsRune(alphabet, r) {
			return false
		}
	}
	return true
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
