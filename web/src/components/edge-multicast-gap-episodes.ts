import type {
  EdgeMulticastChannelInstance,
  EdgeMulticastSequenceHealth,
  GapEpisode,
} from '@/lib/api'

/**
 * Union of the gap episodes across one publisher line's channel instances.
 *
 * A publisher line can carry MANY instances — the sports market-by-price publisher carries one per
 * league, about thirty — so a strip per instance would be thirty strips. The union answers the
 * question the line is actually asked instead: was this path losing at this second, on anything.
 *
 * Instances with `gaps_measured` false contribute nothing, and must not: they come from a plane with
 * no gap marker, so their absence of episodes is an absence of measurement rather than a clean run.
 */
export function mergeGapEpisodes(instances: EdgeMulticastChannelInstance[]): GapEpisode[] {
  const sorted = instances
    .filter((i) => i.gaps_measured)
    .flatMap((i) => i.gap_episodes ?? [])
    .slice()
    .sort((a, b) => a.start - b.start)

  const out: GapEpisode[] = []
  for (const e of sorted) {
    const last = out[out.length - 1]
    // Touching counts as contiguous, not just overlapping: two runs that meet at a second
    // boundary are one outage, and drawing them as two would inflate the episode count.
    if (last && e.start <= last.start + last.seconds) {
      last.seconds = Math.max(last.start + last.seconds, e.start + e.seconds) - last.start
      continue
    }
    out.push({ ...e })
  }
  return out
}

/** What a line's episodes say about the window they were measured over. */
export type GapEpisodeStats = {
  /** Episodes in the window. */
  episodes: number
  /** Seconds of the window with at least one gap-marked message. */
  lostSeconds: number
  /** Share of the window's seconds with no gap at all, 0-1. */
  gapFree: number
  /** Episodes per hour, extrapolated from the window. */
  perHour: number
  /** Seconds from the end of the last episode to the end of the window, or undefined when the
   *  window held no episode — "never" and "just now" must not render as the same number. */
  sinceLastSeconds?: number
  /** Longest episode, in seconds. A book stays un-anchored until a snapshot re-anchors it, so this
   *  is the worst recovery time the window saw, not a count of anything. */
  worstRecoverySeconds: number
}

/**
 * Derives the operational read of a line's timeline.
 *
 * Every figure here comes from the episodes and the window alone — no query, no extra payload. What
 * it deliberately does NOT derive is a packet-loss rate: an episode is a stretch of time a book was
 * un-anchored, not a count of datagrams that failed to arrive, and dividing one by the other would
 * invent a denominator the recorder never measured.
 */
export function gapEpisodeStats(episodes: GapEpisode[], windowSecs: number, windowEnd: number): GapEpisodeStats {
  const lostSeconds = episodes.reduce((n, e) => n + e.seconds, 0)
  const last = episodes[episodes.length - 1]
  return {
    episodes: episodes.length,
    lostSeconds,
    // Clamped: episodes are capped at one entry per second of the window, but a payload whose
    // clock and window disagree could still push the sum past it, and a negative share is worse
    // than a saturated one.
    gapFree: windowSecs > 0 ? Math.max(0, 1 - lostSeconds / windowSecs) : 0,
    perHour: windowSecs > 0 ? (episodes.length * 3600) / windowSecs : 0,
    // Floored, never rounded. windowEnd carries milliseconds, and rounding it up puts the end of
    // the window after where it is, which prints a longer quiet stretch since the last episode
    // than was measured. The digit shown must never be better than the measurement.
    sinceLastSeconds: last
      ? Math.max(0, Math.floor(windowEnd / 1000) - (last.start + last.seconds))
      : undefined,
    worstRecoverySeconds: episodes.reduce((n, e) => Math.max(n, e.seconds), 0),
  }
}

/** Update loss over a publisher line, summed across its channel instances. */
export type SequenceLoss = {
  received: number
  missing: number
  events: number
  /** Loss as parts per million of the updates that should have arrived. */
  ppm: number
  /** Missing updates per minute of the window. */
  perMinute: number
  /** Worst single break, in messages, and the same with one outlier unable to speak for the
   *  window. The p99 is the MAX of the instances' own p99s, not a percentile over percentiles:
   *  a line is as bad as its worst series, and re-deriving a true p99 would need the raw breaks. */
  maxGap: number
  p99Gap: number
}

/**
 * Sums a line's per-instrument sequence loss.
 *
 * Returns undefined when nothing on the line measured it — no denominator, so no rate. That is a
 * different statement from a measured zero, and the two must not render alike: top-of-book series
 * carry no per-instrument sequence at all, and reporting 0 ppm for them would be the same false
 * clean bill of health the gap timeline refuses to give.
 */
export function sequenceLoss(
  instances: EdgeMulticastChannelInstance[],
  windowSecs: number,
): SequenceLoss | undefined {
  const measured = instances.filter((i) => (i.updates_received ?? 0) > 0)
  if (measured.length === 0) {
    return undefined
  }
  const received = measured.reduce((n, i) => n + (i.updates_received ?? 0), 0)
  const missing = measured.reduce((n, i) => n + (i.updates_missing ?? 0), 0)
  const expected = received + missing
  return {
    received,
    missing,
    events: measured.reduce((n, i) => n + (i.seq_gap_events ?? 0), 0),
    ppm: expected > 0 ? (missing / expected) * 1e6 : 0,
    perMinute: windowSecs > 0 ? missing / (windowSecs / 60) : 0,
    maxGap: measured.reduce((n, i) => Math.max(n, i.max_gap_messages ?? 0), 0),
    p99Gap: measured.reduce((n, i) => Math.max(n, i.p99_gap_messages ?? 0), 0),
  }
}

/** What a group delivered, as one number plus the time it had no redundancy. */
export type Completeness = {
  /** Updates lost per million of what should have arrived, or undefined when nothing measured it —
   *  no denominator, so no rate. Distinct from a measured zero. */
  ppm?: number
  missing: number
  expected: number
  /** Seconds in which EVERY path of the feed was losing at once. The only figure here that means
   *  the feed itself lost data rather than one of its paths. */
  unprotectedSeconds: number
}

/**
 * Rolls a group's sequence health into the two numbers that answer "how is this feed doing".
 *
 * NOT the same measurement as the per-day completeness view on the Kalshi L2 page (#798), which
 * asks whether the level-grain record for a whole DAY was captured, over a fourteen-day window.
 * This one is live: message loss inside the current fifteen-minute window, on the multicast page.
 * The two can disagree without either being wrong — a day can be captured end to end and still
 * have lost updates inside it — so they are deliberately reported in different units, ppm here
 * against a per-day percentage there.
 *
 * The page could already say WHERE something broke — which path, which recorder, which book — and
 * could not say whether the feed delivered what it should have. These are the pieces that were
 * already measured and scattered: per-instrument sequence loss, which is the only counter on the
 * page with a real denominator, and the seconds every path lost together.
 *
 * Both come from the group roll-up, so this is arithmetic over the existing payload and costs no
 * query. ppm is undefined rather than zero when nothing measured it: a top-of-book series carries no
 * per-instrument sequence, so a feed recorded only on that plane has no completeness figure at all,
 * and printing 0 ppm would be the false clean bill of health this page keeps refusing.
 */
export function completeness(sequence?: EdgeMulticastSequenceHealth): Completeness {
  const instances = sequence?.instances ?? []
  const received = instances.reduce((n, i) => n + (i.updates_received ?? 0), 0)
  const missing = instances.reduce((n, i) => n + (i.updates_missing ?? 0), 0)
  const expected = received + missing
  return {
    ppm: expected > 0 ? (missing / expected) * 1e6 : undefined,
    missing,
    expected,
    unprotectedSeconds: (sequence?.all_paths_gapped ?? []).reduce((n, e) => n + e.seconds, 0),
  }
}

/**
 * Updates a series must have carried in the window before a loss RATE is worth printing.
 *
 * A ratio over a thin channel is noise wearing a percentage. Measured over six hours of mainnet,
 * ncaamb ch15 received 45,189 updates and lost 546 — 11,938 ppm, twenty times the rate of the
 * fleet-wide events that actually mattered — and ncaawb ch116 read 7,475 ppm off 4,647 updates.
 * Neither is a worse feed than tennis at 470 ppm over 28M updates; they are small denominators.
 *
 * The count is always shown. Only the rate is withheld, which is the same trade
 * edgeMulticastPathParityMinMessages makes on the parity check, and the reason the monitoring
 * products in this space report absolute counts and per-second rates rather than ratios.
 */
export const SEQUENCE_LOSS_MIN_UPDATES = 500

/** How the Sequence cell should read: a magnitude where there is one, a word where there is not. */
export type SequenceVerdict = {
  /** What the badge says. A count of values lost wherever the plane can count them. */
  label: string
  tone: 'good' | 'bad' | 'warn' | 'muted'
  /** The figure beside the badge. Empty when there is nothing worth putting there. */
  detail: string
}

/** 1,234 → "1,234"; 45,189 → "45.2k"; 4,476,494 → "4.48M". Beside a badge, three digits is the budget. */
export function formatUpdateCount(n: number): string {
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M`
  if (n >= 1e4) return `${(n / 1e3).toFixed(1)}k`
  return n.toLocaleString()
}

/**
 * Grades one publisher line's Sequence cell in the unit the wire protocol actually carries:
 * sequence values that never arrived.
 *
 * The badge used to name a state and size it by `gap_books`, which is an instrument count and not a
 * loss count — it saturates at the channel's book count, so measured over six hours of mainnet
 * perps read 13 books against 3,439 lost updates while ncaaf read 1,934 books against 2,693. The
 * ranking it produced was not the ranking of loss.
 *
 * Values lost is also the only figure here the two redundant paths AGREE on. Over the same window
 * the paths of one feed differed by up to 15.6x on gap-marked messages — a time measure, driven by
 * when the next snapshot happened to arrive — and by at most 7% on values lost, 0.06% in the worst
 * fifteen minutes of the day. Two independent paths cannot lose the same datagrams by chance, so
 * the number they agree on is the one measuring the feed.
 *
 * Three things deliberately keep a word instead of a number:
 *
 *   - `stalled`, because a series carrying no new values has no count to report, and "0 lost" over
 *     nothing is the clean bill of health this column exists to withhold. Time still decides WHEN
 *     to call a series dead; it just never sizes the loss.
 *   - `not counted`, for the top-of-book plane. Its wire sequence is dense, but the recorder
 *     persists one row per change to the top of the book, so the numbering reconstructed from the
 *     stored rows has structural holes — measured at 1,292 on perps ch1 at each of three
 *     independent recorders, which is the proof they are not loss.
 *   - `gapped` with no count, for the case a gap marker was written on a series that carried no
 *     level updates to count holes in.
 */
export function sequenceVerdict(
  sequence: EdgeMulticastSequenceHealth,
  windowSecs: number,
): SequenceVerdict {
  const total = sequence.instances.length
  const loss = sequenceLoss(sequence.instances, windowSecs)

  // Read before the loss counters: a series that stopped is not a series that lost nothing, and a
  // count over a dead window is the false clean this column refuses to print.
  if (sequence.status === 'stalled') {
    return { label: 'stalled', tone: 'warn', detail: `${sequence.stalled}/${total}` }
  }

  if (loss === undefined) {
    // A marker with no countable numbering behind it. Rare, and still a fault.
    if (sequence.status === 'gapped') {
      return { label: 'gapped', tone: 'bad', detail: `${sequence.gapped}/${total}` }
    }
    return { label: 'not counted', tone: 'muted', detail: total > 1 ? `×${total}` : '' }
  }

  const expected = loss.received + loss.missing
  const rate =
    expected >= SEQUENCE_LOSS_MIN_UPDATES
      ? `${loss.ppm.toFixed(loss.ppm >= 100 ? 0 : 1)} ppm`
      : `of ${formatUpdateCount(expected)}`

  if (loss.missing > 0) {
    return { label: `${loss.missing.toLocaleString()} lost`, tone: 'bad', detail: rate }
  }
  // Zero holes and a marker still standing is not a clean series. edgeMulticastRecorderRegrade
  // clears the count when the recorder admits the datagrams were its own, and deliberately does not
  // clear the marker: a book was left un-anchored whoever dropped them. Painting that green would
  // hide the one fault the backend is still asserting, so the badge keeps the gapped tone and the
  // count moves into the tooltip, which is where '0 lost, 1 book un-anchored' can be said in full.
  if (sequence.status === 'gapped') {
    return { label: 'gapped', tone: 'bad', detail: `${sequence.gapped}/${total}` }
  }
  // A measured zero is the common state — eight of twenty-four quarter-hours on mainnet — so it has
  // to read as a reading rather than as a blank. The denominator is what makes it one.
  return { label: '0 lost', tone: 'good', detail: `${formatUpdateCount(loss.received)} upd` }
}
