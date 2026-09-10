# Repointing the loss strip at the recorder's own rows

**Status:** implemented for the loss strip; the badge legs are unchanged
**Date:** 2026-09-03
**Applies to:** the Sequence column of `/dz/edge/multicast` — the per-recording-node strip under a
publisher line, not the badge above it
**Authority:** `edge-multicast-ref`'s
[`2026-08-31-sequence-loss-and-conformance-rows-design.md`](https://github.com/malbeclabs/edge-multicast-ref/blob/main/docs/superpowers/specs/2026-08-31-sequence-loss-and-conformance-rows-design.md),
whose consumer this is, and
[`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md) for vocabulary

---

## What changes

The strip under a publisher line's Sequence cell — one row per recording node, plus the row
underneath that says whether a loss was one node's branch — is fed by the recorder's own
sequence-loss detection when the recorder rows are present, and by today's peer comparison over
`kalshi_bbo_observations` when they are not.

Nothing else on the page moves. The badge (`gapped` / `stalled` / `advancing`), the instance list,
`gap_nodes`, `gaps_unmeasured` and the `book` row of the strip still come from the two capture-table
legs (`kalshi_l2_coverage.go`, `edge_multicast_tob_sequence.go`). Why they are left alone is under
**Deliberately out of scope**.

## Why the recorder rows are a better source, in one sentence each

- **The reference is the publisher's own numbering, not what someone recorded.** The peer comparison
  measures each node against the union of what the nodes received, so a datagram *nobody* received
  is in nobody's set and cannot be reported. That is the loss an operator most wants to see, and it
  is the one thing today's strip is structurally unable to say.
- **Our own drops are subtracted rather than inferred.** `admitted_recorder` with `admitted_scope`
  makes "this was ours" a subtraction; comparing nodes catches a loss one node has alone and misses
  one they share, and a load spike reaches them all.
- **Each run carries a verdict.** `recorder` / `upstream` / `path` / `unverifiable` / `publisher`
  replaces the `2+` row's inference with an attribution.
- **A feed with no decoder can be covered.** The counters come from the datagram header, so a feed
  is measurable on the day it is first recorded rather than on the day a book-builder learns to fold
  its book. Realising that on this page is a follow-up — see below.

## The contract

Two tables, proxied per table into the feeds database (`--feeds-db`, `a.FeedsDB`) exactly as
`kalshi_*` and `hyperliquid_*` are, and created out of band. They are the upstream spec's
`recorder.sequence_gap` and `recorder.segment_coverage`, column for column, flattened into that
database's namespace:

| lake reads | upstream table |
| --- | --- |
| `<feeds-db>.recorder_sequence_gap` | `recorder.sequence_gap` |
| `<feeds-db>.recorder_segment_coverage` | `recorder.segment_coverage` |

Four things lake requires of the rows, none of which the spec settles:

1. **`env` carries lake's own environment name** — `mainnet-beta` or `testnet` (`mainnet` is
   tolerated on read). The recorder's `env` is free-form config, and this page's folded payloads are
   mainnet-only while the group key is a multicast address: both networks allocate out of the same
   `233.84.178.0/24` and testnet has had a Solana group sitting on a mainnet Kalshi address for
   weeks. Without the filter that collision renders one network's loss on the other's row.
2. **`recorder` is the node label and `site` the location**, matching `measurement_node_id` and
   `location_code` on the capture tables — the strip labels rows by site and names the node in the
   tooltip, and the live and historical halves of this page must not disagree about what a node is.
3. **`source_addr` is the publisher's tunnel address**, which is what the ledger carries as `dz_ip`
   and what every publisher line on this page is keyed by.
4. **`reference_seqs` is per (instance, site) over the window, repeated on each of that instance's
   gap rows.** It is therefore `max()`-ed per instance and never summed — summing it would multiply
   the denominator by the number of runs and report a real 1% loss as 0.05%. Summing across the
   *instances* of one line is correct and is what the fold does, the same as today.

`segment_coverage` is not optional and not belt-and-braces: **a clean node emits no gap row**, so
the gap table alone cannot list it. The strip's whole claim rests on the clean line — "was lost 267"
means nothing without "cmh lost 0" beside it — and a node that vanishes when it is healthy is worse
than no strip at all. Coverage supplies the (instance × node) universe and the datagram count; the
gap rows supply the losses.

That table has no `group_addr` of its own, so the group is read out of `roles_joined` by matching
`dst_port`. If the loader ever adds `group_addr` there — as the spec added it to `sequence_gap`, and
for the same reason — the `arrayFirst` goes away and nothing else changes.

## How a node's row is built

Per `(group, source_addr, channel_id, dst_port, recorder)` over the same fifteen-minute window the
other folded columns use:

| Strip field | From |
| --- | --- |
| `missing` | `sum(unexplained_count)` — **not** `missing_count`; the recorder's own admitted loss comes off first |
| `missing_raw` | `sum(missing_count)`, so the tooltip can show what was subtracted |
| `admitted` | `sum(admitted_recorder)` |
| `reference_seqs` | `max(reference_seqs)` per instance, summed over the line's instances |
| `runs` | `count()` — one gap row is one contiguous run of sequence numbers |
| `verdicts` | `sum(unexplained_count)` per `verdict` |
| `episodes` | one mark per run, placed at `ifNull(sent_from_ts, before_ts)` |
| `datagrams` | `sum(datagram_count)` from coverage |
| `coverage_complete` | `segment_seq` dense over the window |

**A mark is a placement, never a duration.** A run of missing sequence numbers has no length in
time: at fifty datagrams a second a three-second hole is a hundred and fifty missing and on a
channel that only heartbeats it is three, so a figure in seconds measures how busy the feed was as
much as what was lost. One mark per run at its start second, and the count in the tooltip — which is
the divergence the upstream spec explicitly left to this consumer to settle.

The timestamp preferred for that placement is `sent_from_ts`, the publisher's own send stamp
recovered from a site that *did* record the datagram, falling back to `before_ts`. A site has no
clock reading for something it never received, so its own bracket is the weaker of the two.

**`coverage_complete` false makes a clean node `unverifiable`, not clean.** With a hole in
`segment_seq` the absence of gap rows is an absence of evidence: the object that would have carried
the loss is the object we do not have. The dense check also reads a recorder restart inside the
window as a hole, because `segment_seq` renumbers per run and nothing in these two tables names the
run — which errs toward `unverifiable`, the safe direction.

## The row underneath

Today it is `2+`: seconds where two or more recorders lost at once, which is as close as the peer
comparison gets to naming a loss upstream of the recorders, and it can never fire when every node
lost — the message would be in nobody's reference.

On the recorder leg it becomes `pub`: the runs whose verdict is `publisher`, absent from every site
with no recorder overflow anywhere and coverage intact. It is the claim `2+` was reaching for,
without the ceiling. The two are separate payload fields with separate labels and tooltips, because
they are different claims and rendering them as one row would let a reader carry the weaker one's
caveats onto the stronger one.

## Fallback, and how each state reads

| State | Strip | Label |
| --- | --- | --- |
| Recorder rows present | recorder leg, `pub` row | `recorder rows` |
| Tables absent (local dev, unproxied environment) | peer comparison, `2+` row | `peer comparison` |
| Tables present, query failed | peer comparison, plus a note | `peer comparison` + `recorder rows unavailable` |
| Neither available | today's "not measured" / "no peer to compare" | — |

The source is named on the strip rather than inferred from its shape. The two legs measure against
different references and one of them can see a loss every node shared, so a reader who cannot tell
which is on screen cannot tell what an empty strip means.

A failed recorder query does **not** blank the strip and does not raise the existing
`recorder_loss_unavailable`: the peer comparison still measured something, and "not measured" over a
strip that has marks on it is a worse answer than a stale label. It is a WARN and a note.

## Deliberately out of scope

- **The badge and the instance rows.** Those are per capture source — a market, in Kalshi's case —
  and the recorder rows carry `feed` and the channel instance, with no capture-source name anywhere
  in them. Mapping one onto the other is not a fold, it is a new join, and it changes what the
  column's verdict asserts. Until it exists the top-of-book badge keeps reading `advancing` even
  where the recorder rows can count that plane's loss absolutely.
- **A publisher line with recorder rows and no decoded series.** The strip hangs off a line's
  sequence health, which is built from the capture-table instances, so a feed with no book-builder
  still gets no strip. Fixing it means minting a health entry with no instances, and the group row's
  `publishers` / `publishers_gapped` tally is derived from those entries — a line counted there with
  no series would make the group cell lie. Separate change.
- **`conformance_finding` and the `datagram` table.** Nothing on this page consumes them.

## What has to exist before any of this renders

None of it is live: as of this writing the recorder is deployed nowhere, the loaders in the upstream
spec's "what this needs that does not exist yet" are unbuilt, and prod ClickHouse holds no
`recorder_*` table. The handler is gated on `EXISTS TABLE` and every environment therefore keeps the
peer comparison until the proxies are created. What this change buys before then is the contract
above, pinned as code and tests, so the loader is written against a consumer that exists.
