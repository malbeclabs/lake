package handlers

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// The recorder plane: the Sequence column's third source, and the only one that can subtract the
// recorder's own loss before reporting a number.
//
// # What this reads, and why it is not another view of the same thing
//
// fact_edge_recorder_sequence_gap and fact_edge_recorder_segment_coverage are lake's own tables,
// written by an edge-feed recorder's analysis tier from the datagram headers it archived. One gap
// row is a contiguous run of sequence numbers nobody delivered to that recorder; one coverage row
// is a segment of the archive, per channel instance, carrying what the recorder was asked to join
// and what its capture ring dropped.
//
// The two sources this column already folds cannot say the same thing, and the difference is not a
// matter of precision:
//
//   - kalshi_l2_coverage.go counts holes in `per_instrument_seq` on a decoded level-grain table.
//     It is a real loss count, and it is UNATTRIBUTED: a datagram the recorder's own capture ring
//     dropped is missing from that table exactly the way one the publisher never sent is. Comparing
//     recorders catches a loss one recorder has alone and is structurally blind to one they share,
//     which is what a load spike on a shared host produces.
//   - edge_multicast_tob_sequence.go cannot count loss at all: its table holds one row per change
//     to the top of the book, so the numbering it could reconstruct has structural holes.
//
// The recorder writes its own admitted drop as a NUMBER, per datagram, and the gap deriver
// subtracts it: `unexplained_count` is `missing_count` less what we admit. That residue is the only
// magnitude on this page that has had the observer's own loss taken out of it, which is why this
// fold wins wherever it has rows — see foldEdgeMulticastRecorderSequence.
//
// # drop_scope decides whether the subtraction was allowed to happen at all
//
// The recorder runs in one of two capture modes and they take DIFFERENT ARITHMETIC, not the same
// arithmetic at two grains:
//
//   - `port-role`: a socket-mode accumulator per port role, so the drop is attributable to one
//     channel instance and the deriver's per-gap subtraction is valid. `unexplained_count` is then
//     a number this page may report as the publisher's.
//   - `capture-handle`: an AF_PACKET ring counts frames dropped BEFORE demultiplexing, so a delta
//     caused by market-data frames rides on the next reference-data datagram that gets through. The
//     number belongs to the handle and to NO channel instance. Summing admitted drops per instance
//     is measurably wrong: a ring dropped forty market-data datagrams, the delta landed on a
//     reference-data datagram, and the per-instance sum reported forty unexplained against
//     market-data — a false publisher finding while the handle had admitted all forty.
//
// So at handle scope the recorder can only ever exonerate ITSELF, and only when its total over the
// window is zero. That is the common case and the interesting one: a recorder admitting nothing
// turns every gap into someone else's with evidence. Where the handle admitted anything at all, the
// instance is unverifiable for recorder attribution, and this fold withholds the magnitude rather
// than letting a number that may be the recorder's own be printed as the feed's. The verdict still
// says loss happened — data was lost either way — which is the same bound GapNodes documents for
// the single-vantage case.
//
// # Why this reads a cache and never queries
//
// Same contract as the other two legs, for the reason edge_multicast_sequence.go documents at
// length: /dz/edge/multicast polls every 30 seconds and the Sequence column folds cached refresher
// payloads so that no page can disagree with another about one feed, and so that a slow or failing
// read costs a column rather than the page. These tables are cheap — they are derived rows, tens of
// bytes each, sorted by the channel instance inside a monthly partition — but "cheap" is not the
// reason the other legs are cached, and putting this one on the request path would reintroduce the
// disagreement the caching exists to prevent. The query lives in StartKalshiBackgroundRefresher
// beside the two it joins.
//
// A cache miss is the normal state in local dev, before the refresher's first run, and everywhere
// the recorder's analysis tier is not loading rows yet. It yields no opinion from this fold and is
// never an error.

// edgeMulticastRecorderSequenceCacheKey is the page-cache key written by
// StartKalshiBackgroundRefresher.
//
// Versioned, and the version MUST be bumped in the same commit as any change to the payload's
// shape. The entry lives in Postgres and outlives the deploy, so without a bump the new binary
// unmarshals a row the old one wrote — see the same note on kalshiL2CoverageCacheKey, which
// records what that cost the whole SPA the once it happened.
const edgeMulticastRecorderSequenceCacheKey = "edge_multicast_recorder_sequence:v1"

// edgeMulticastRecorderSequenceWindowMinutes matches kalshiL2WindowMinutes and
// edgeMulticastObservationsWindowMinutes so that all three legs of the Sequence column describe the
// same span. A leg reading a different width would make the column's own arithmetic — received
// against missing, one path against its peer — a comparison between two different questions.
const edgeMulticastRecorderSequenceWindowMinutes = 15

// The two capture modes the recorder declares on every row it writes. They are carried as data
// rather than configured here because one fleet legitimately runs both, and a handler that assumed
// either would be silently wrong on half of it.
const (
	edgeMulticastDropScopePortRole      = "port-role"
	edgeMulticastDropScopeCaptureHandle = "capture-handle"
)

// EdgeMulticastRecorderSequenceSeries is one channel instance as ONE recorder saw it, over the
// window.
//
// The recording site is never folded, which is the same rule the other two legs keep and the same
// one the tables' own sort keys keep: two vantages of one instance are two observations, and
// merging them hides a recorder that is missing the feed. `env` is in the grain for the same
// reason one level down — these are the RECORDER's environments, not lake's, and one site may host
// a mainnet and a testnet recorder under one recorder name.
//
// The destination port IS folded, and only here at the reader, exactly as the tables' header
// documents: a sequence number is minted per port role, so the counters below are summed across the
// three ports one publisher serves a channel on rather than split into three rows that each look
// like they are missing something.
type EdgeMulticastRecorderSequenceSeries struct {
	// MulticastGroup is the destination address, and the only way a recorder row finds its group
	// on this page. It is a column on the gap row for exactly this reason, and it is recovered
	// from the coverage row's roles_joined for an instance that had no gap at all.
	MulticastGroup string `json:"multicast_group,omitempty"`

	// PublisherSourceIP is `source_addr`: the address the datagrams came from, matched against
	// the ledger's dz_ip to find the publisher line. The same join both other legs make.
	PublisherSourceIP string `json:"publisher_source_ip,omitempty"`

	ChannelID uint8 `json:"channel_id"`

	// Feed is the recorder's own spec name for the stream. It stands in for the capture source
	// id the other two legs carry and it is a DIFFERENT namespace: a capture source id is the
	// ledger group code with the plane suffix hoisted to the front, this is what the recorder
	// was configured to record. Carried so an instance this fold contributes on its own is not
	// nameless, never matched against a capture source id.
	Feed string `json:"feed,omitempty"`

	// Node is `recorder` and LocationCode is `site` — the recording vantage, on the two axes the
	// rest of the page already reads. Env completes the row's identity at rest and is not
	// rendered: the payload is per environment already.
	Node         string `json:"node"`
	LocationCode string `json:"location_code,omitempty"`
	Env          string `json:"env,omitempty"`

	// Unexplained is the magnitude, summed over the window: `unexplained_count`, which is
	// `missing_count` less what the recorder admits losing. This is the number that reaches the
	// column, and Missing is carried beside it only so the two are readable against each other —
	// how much was lost, and how much of it the recorder has taken responsibility for.
	//
	// Reporting Missing would be reporting the recorder's own drops as the publisher's, which is
	// the one failure the whole plane exists to prevent.
	Unexplained uint64 `json:"unexplained"`
	Missing     uint64 `json:"missing"`

	// ReferenceSeqs is what the loss is a share of: the sequence numbers this site should have
	// seen over the window. It is MAX-ed across a port's gap rows and SUMMED across the ports of
	// an instance — it is a per-window figure repeated on every gap row of that window, so
	// summing it within a port would multiply the denominator by the row count and silently
	// divide the rate by it.
	//
	// Zero on an instance with no gap rows, which is not a missing denominator: an instance that
	// lost nothing received everything it should have, and Datagrams is then the reference.
	ReferenceSeqs uint64 `json:"reference_seqs"`

	// Datagrams is what the recorder archived for this instance in the window, summed over its
	// coverage segments. It is the denominator for a clean instance and the fallback for a gap
	// row written without a reference count.
	//
	// A segment that spans the window's edge contributes its whole count, so this can overstate
	// the denominator on the first segment of a window. That dilutes a rate rather than
	// inflating it, which is the direction this page prefers to be wrong in.
	Datagrams uint64 `json:"datagrams"`

	// Gaps is discontinuities: one per gap row, which is one per contiguous run of missing
	// sequence numbers. This is what a "gaps per hour" is built from and it is NOT an episode
	// count on a time axis — see SeqGapEvents on EdgeMulticastChannelInstance, which carries the
	// same distinction from the market-by-price leg.
	Gaps uint64 `json:"gaps"`

	// MaxRun is the worst single run of missing sequence numbers and P99Run the same figure with
	// one outlier unable to speak for the window. Both are in SEQUENCE VALUES, which is the unit
	// this whole plane measures in.
	MaxRun uint64  `json:"max_run,omitempty"`
	P99Run float64 `json:"p99_run,omitempty"`

	// DropScope is the capture mode this recorder declared, and it decides whether Unexplained
	// may be read as the publisher's at all. See the header.
	DropScope string `json:"drop_scope,omitempty"`

	// HandleAdmitted is the capture handle's OWN drop over the window: the delta of a cumulative
	// counter across every coverage row of this (site, recorder, env), not of this instance's
	// rows. It is a handle-wide figure on purpose — at capture-handle scope a drop belongs to the
	// handle and to no single instance, so the only question that can be asked of it is whether
	// it is zero.
	HandleAdmitted uint64 `json:"handle_admitted,omitempty"`

	// LastSeen is the newest coverage segment end for this instance: how far the recorder's own
	// archive has advanced. Graded against this payload's own clock and never against wall
	// clock, for the reason edgeMulticastSequenceStatus documents.
	LastSeen time.Time `json:"last_seen"`
}

// verifiable reports whether this series' magnitude may be read as loss the recorder did not cause.
//
// Three cases, and the third is the one that is easy to get wrong:
//
//   - port-role scope: the deriver's subtraction was per instance and valid. Verifiable.
//   - capture-handle scope: the handle's drops belong to no instance, so the recorder can only
//     exonerate itself, and only by having admitted NOTHING over the whole window. A handle that
//     admitted anything at all makes every instance under it unverifiable — the forty dropped
//     market-data datagrams whose delta landed on a reference-data datagram is the measured case.
//   - nothing was missing: there is no loss to attribute, so the scope does not matter and a
//     measured zero stands whatever mode the recorder was in. Without this case a handle that
//     dropped one frame would withhold the clean bill of health from every instance it carries,
//     which is not caution, it is losing the reading.
//
// An unrecognised scope is treated as unverifiable. A scope this handler does not know is not a
// scope it may subtract at, and a new capture mode arriving as a string it has never seen must fail
// towards saying less rather than towards a publisher finding.
func (s EdgeMulticastRecorderSequenceSeries) verifiable() bool {
	if s.Missing == 0 {
		return true
	}
	switch s.DropScope {
	case edgeMulticastDropScopePortRole:
		return true
	case edgeMulticastDropScopeCaptureHandle:
		return s.HandleAdmitted == 0
	default:
		return false
	}
}

// EdgeMulticastRecorderSequenceResponse is what the refresher caches.
type EdgeMulticastRecorderSequenceResponse struct {
	GeneratedAt   time.Time                             `json:"generated_at"`
	WindowMinutes int                                   `json:"window_minutes"`
	Series        []EdgeMulticastRecorderSequenceSeries `json:"series"`
}

// edgeMulticastRecorderSequenceTablesExist reports whether both recorder tables are queryable.
//
// Both, and not either: coverage without gaps would report every instance clean, and gaps without
// coverage would have no roster, no drop scope and no way to tell a handle that admitted nothing
// from one nobody asked. A half-present plane is worse than an absent one here, because the half
// that is present is the half that says "fine".
//
// Absent is the ordinary state in local dev and anywhere the recorder's analysis tier is not
// loading yet, and it must cost this leg and nothing else — the same contract every other additive
// signal on this page has.
func (a *API) edgeMulticastRecorderSequenceTablesExist(ctx context.Context) (bool, error) {
	for _, table := range []string{"fact_edge_recorder_sequence_gap", "fact_edge_recorder_segment_coverage"} {
		var exists uint8
		if err := a.envDB(ctx).QueryRow(ctx, "EXISTS TABLE "+table).Scan(&exists); err != nil {
			return false, err
		}
		if exists != 1 {
			return false, nil
		}
	}
	return true, nil
}

// FetchEdgeMulticastRecorderSequence aggregates the recorder's own sequence-loss rows over the
// window, one row per channel instance per recording vantage.
//
// # FINAL, on both tables
//
// Both are ReplacingMergeTree on `ingested_at`, and reprocessing after an analyser fix writes the
// row again rather than mutating it. Collapsing happens at merge time only, so between a reprocess
// and its merge the same gap is present twice and a bare sum reports twice the loss. FINAL is what
// the tables' own header prescribes for a reader that must not see a superseded row, and it is
// cheap here for the reason it names: these are derived rows, tens of bytes against a 1232-byte
// datagram, and the scan is a key range inside a partition.
//
// # Why the aggregation is two-stage
//
// The port is folded by the READER, and the two counters fold differently across it. Loss is
// additive — the three port roles of a channel are three sequence spaces and values lost in each
// are values lost — but `reference_seqs` is a per-window total repeated on every gap row of that
// window, so it is MAX-ed within a port and only then summed across the ports. Doing it in one
// stage would multiply the denominator by the number of gap rows, which shows up as a loss rate
// quietly divided by however bad the window was: the worse the feed, the healthier it reads.
//
// # The roster is coverage, not gaps
//
// The outer FROM is the coverage table and the gaps LEFT JOIN onto it, which fixes the direction
// of two different mistakes at once. An instance with coverage and no gaps is a MEASURED ZERO and
// has to be reported, or the fold can never say a feed is clean. An instance with no coverage row
// in the window is one nothing here can speak for — the gap tables' own `unverifiable` verdict
// names that case — and it produces no row, so the column falls through to whatever the other legs
// say instead of inheriting a claim from a window this recorder cannot vouch for.
func (a *API) FetchEdgeMulticastRecorderSequence(ctx context.Context) (*EdgeMulticastRecorderSequenceResponse, error) {
	out := &EdgeMulticastRecorderSequenceResponse{
		GeneratedAt:   time.Now().UTC(),
		WindowMinutes: edgeMulticastRecorderSequenceWindowMinutes,
		Series:        []EdgeMulticastRecorderSequenceSeries{},
	}

	exists, err := a.edgeMulticastRecorderSequenceTablesExist(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return out, nil
	}

	q := fmt.Sprintf(`
		WITH
		gap_port AS (
			SELECT
				source_addr, channel_id, dst_port, site, recorder, env,
				max(group_addr)               AS group_addr,
				sum(unexplained_count)        AS unexplained,
				sum(missing_count)            AS missing,
				-- Per-window, repeated per row: max, never sum. See the doc comment.
				max(reference_seqs)           AS reference_seqs,
				count()                       AS gaps,
				max(missing_count)            AS max_run,
				quantile(0.99)(missing_count) AS p99_run
			FROM fact_edge_recorder_sequence_gap FINAL
			WHERE before_ts >= now64(9) - toIntervalMinute(%[1]d)
			GROUP BY source_addr, channel_id, dst_port, site, recorder, env
		),
		gap_inst AS (
			SELECT
				source_addr, channel_id, site, recorder, env,
				max(group_addr)     AS group_addr,
				sum(unexplained)    AS unexplained,
				sum(missing)        AS missing,
				sum(reference_seqs) AS reference_seqs,
				sum(gaps)           AS gaps,
				max(max_run)        AS max_run,
				max(p99_run)        AS p99_run
			FROM gap_port
			GROUP BY source_addr, channel_id, site, recorder, env
		),
		cov_port AS (
			SELECT
				source_addr, channel_id, dst_port, site, recorder, env,
				max(toString(feed))       AS feed,
				max(toString(drop_scope)) AS drop_scope,
				sum(datagram_count)       AS datagrams,
				max(end_ts)               AS last_seen,
				-- The destination address for THIS port role, out of the manifest's
				-- roles_joined. arrayFilter over the two subcolumns rather than an index
				-- lookup: a role the recorder was not asked to join yields an empty array
				-- and an empty string, where arrayElement at index 0 is an error.
				max(arrayElement(arrayFilter((g, p) -> p = dst_port,
					roles_joined.group_addr, roles_joined.port), 1)) AS group_addr
			FROM fact_edge_recorder_segment_coverage FINAL
			WHERE end_ts >= now64(9) - toIntervalMinute(%[1]d)
			GROUP BY source_addr, channel_id, dst_port, site, recorder, env
		),
		cov_inst AS (
			SELECT
				source_addr, channel_id, site, recorder, env,
				max(feed)        AS feed,
				max(drop_scope)  AS drop_scope,
				max(group_addr)  AS group_addr,
				sum(datagrams)   AS datagrams,
				max(last_seen)   AS last_seen
			FROM cov_port
			GROUP BY source_addr, channel_id, site, recorder, env
		),
		-- The handle's own drops, at the handle's own grain and at no finer one. Cumulative
		-- counters, so only the delta says anything about this window; greatest(..., 0) because
		-- a host that restarted mid-window restarts the counter, and ClickHouse promotes
		-- UInt64 - UInt64 to a signed type whose negative would fail the scan.
		handle AS (
			SELECT
				site, recorder, env,
				toUInt64(greatest(max(capture_drop_total) - min(capture_drop_total), 0)) AS admitted
			FROM fact_edge_recorder_segment_coverage FINAL
			WHERE end_ts >= now64(9) - toIntervalMinute(%[1]d)
			GROUP BY site, recorder, env
		)
		SELECT
			c.source_addr,
			c.channel_id,
			c.recorder,
			c.site,
			c.env,
			c.feed,
			c.drop_scope,
			if(c.group_addr != '', c.group_addr, g.group_addr) AS group_addr,
			c.datagrams,
			c.last_seen,
			g.unexplained,
			g.missing,
			g.reference_seqs,
			g.gaps,
			g.max_run,
			g.p99_run,
			h.admitted
		FROM cov_inst AS c
		LEFT JOIN gap_inst AS g
			ON c.source_addr = g.source_addr AND c.channel_id = g.channel_id
			AND c.site = g.site AND c.recorder = g.recorder AND c.env = g.env
		LEFT JOIN handle AS h
			ON c.site = h.site AND c.recorder = h.recorder AND c.env = h.env
		SETTINGS max_execution_time = 60, timeout_before_checking_execution_speed = 0`,
		edgeMulticastRecorderSequenceWindowMinutes)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, q)
	metrics.RecordClickHouseQuery("edge_multicast_recorder_sequence", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s EdgeMulticastRecorderSequenceSeries
		if err := rows.Scan(&s.PublisherSourceIP, &s.ChannelID, &s.Node, &s.LocationCode, &s.Env,
			&s.Feed, &s.DropScope, &s.MulticastGroup, &s.Datagrams, &s.LastSeen,
			&s.Unexplained, &s.Missing, &s.ReferenceSeqs, &s.Gaps, &s.MaxRun, &s.P99Run,
			&s.HandleAdmitted); err != nil {
			return nil, err
		}
		s.LastSeen = s.LastSeen.UTC()
		out.Series = append(out.Series, s)
	}
	return out, rows.Err()
}

// edgeMulticastRecorderLoss reads one series' magnitude the way the column has to render it:
// what was lost, and what it is a share of.
//
// received is derived from the denominator rather than counted, and that is deliberate.
// `reference_seqs` is the sequence numbers this site SHOULD have seen, which is exactly the
// `expected` the consumer already computes as received + missing, so feeding it as
// (reference - missing) puts the recorder's rate on the one threshold the page already has —
// SEQUENCE_LOSS_MIN_UPDATES — rather than inventing a second one for this plane that would then
// have to be kept in step with it.
//
// Two cases fall back to the recorder's archived datagram count:
//
//   - an instance with no gap rows carries no reference at all, because the reference lives on the
//     gap row. It lost nothing, so what it received IS what it should have seen.
//   - a reference that does not exceed the loss is not a denominator. A gap row written before the
//     cross-site join could fill it in, or a window read across the deriver's own boundary, can
//     produce one; using it would report a negative or zero received, and a zero received is read
//     downstream as "nothing measured this" — the false absence this column keeps refusing.
func edgeMulticastRecorderLossFor(s EdgeMulticastRecorderSequenceSeries) (received, missing uint64) {
	missing = s.Unexplained
	if s.ReferenceSeqs > missing {
		return s.ReferenceSeqs - missing, missing
	}
	return s.Datagrams, missing
}

// edgeMulticastRecorderRegrade re-decides the LOSS half of an instance's verdict from the
// recorder's magnitude, and leaves the TIME half alone.
//
// The loss half is re-decided because the recorder's number is the better one: it is
// `unexplained_count`, the residue after the recorder's own admitted drops are subtracted, and
// neither other leg can subtract them. So a recorder that has taken responsibility for the whole of
// a loss clears a 'gapped' the level-grain leg raised on the same window — that leg counted the
// same datagrams and had no way to know whose they were.
//
// It is not re-decided away from a gap MARKER. gapBooks is a recovery state, not a loss count: it
// says a book was left un-anchored and could not be trusted until a snapshot re-anchored it, which
// happened whoever dropped the datagram. A marker written is a fault observed and this fold has
// nothing to say about it.
//
// The time half is left exactly as it was because this fold cannot re-decide it honestly. Staleness
// is measured against the payload's OWN clock — see edgeMulticastSequenceStatus — and the instance
// was graded against a different payload's. Re-running the staleness test here with this payload's
// clock and that payload's last-seen would mix two refresher runs' lag into one verdict, which is
// the exact mistake that comment exists to prevent. So 'stalled' survives a clean recorder reading:
// a series that stopped is not a series that lost nothing.
func edgeMulticastRecorderRegrade(prev string, gapBooks, missing uint64, attributionWithheld bool) string {
	// Loss outranks staleness, the same order edgeMulticastSequenceStatus puts them in. Withheld
	// attribution counts as loss and not as silence: the datagrams did not arrive, and all the
	// scope decides is whether the page may name a culprit.
	if missing > 0 || attributionWithheld || gapBooks > 0 {
		return edgeMulticastSeqGapped
	}
	// No unexplained loss and no marker. A 'gapped' that came from a loss count this fold has now
	// replaced does not survive; a 'stalled' does, and so does an 'ok'.
	if prev == edgeMulticastSeqGapped {
		return edgeMulticastSeqOK
	}
	return prev
}

// clampUint32 saturates a sequence-value run into the uint32 the instance carries it in.
//
// MaxGapMessages is a uint32 because the market-by-price leg's runs are single digits; a recorder
// row is a run of sequence numbers and an era that restarted badly could in principle exceed it.
// Saturating beats wrapping: a truncated run reads as an implausibly small break, which is the one
// direction this page must not be wrong in.
func clampUint32(v uint64) uint32 {
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
