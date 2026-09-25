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
 * The floor is where a ratio stops being a reading at all: under 500 updates one hole is 2,000 ppm
 * or more, so the figure is set by the denominator and moves in steps a reader cannot interpret.
 * It is the same trade edgeMulticastPathParityMinMessages makes on the parity check.
 *
 * It is NOT what keeps a thin channel from ranking above a busy one — no floor can, because those
 * channels clear any floor worth having. Measured over six hours of mainnet, ncaamb ch15 read
 * 11,938 ppm off 45,189 updates and ncaawb ch116 7,475 ppm off 4,647, neither of them a worse feed
 * than tennis at 470 ppm over 28M. That is why the COUNT is the badge's headline and the rate is
 * only the detail beside it, which is also how the monitoring products in this space report loss.
 * The count is always shown; only the rate is ever withheld.
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
 * Values lost is also the steadiest figure here between the two redundant paths. Over the same
 * window the paths of one feed differed by up to 15.6x on gap-marked messages — a time measure,
 * driven by when the next snapshot happened to arrive — while their values-lost totals sat within
 * 7% per feed, and 0.06% in the worst quarter hour. A duration that swings by an order of
 * magnitude between two paths carrying one feed is describing the vantage; a magnitude that holds
 * is describing how much the feed cost.
 *
 * That is a statement about the SIZE of each path's own loss, and deliberately not about the two
 * paths losing the same thing. They do not: kalshi_l2_coverage.go records that the per-instrument
 * holes DIFFER wherever there is loss — perps at 36 against 9 over one window — which is what
 * independent per-path loss looks like, and is the test frame_sequence failed by reporting
 * identical holes on both paths. Identical counts across independent observers are evidence of a
 * numbering artifact, which is the argument the top-of-book plane rests on; close-but-different
 * totals over hours are two paths losing independently at similar rates. Each path is therefore
 * graded on its own loss, which is why the badge is per line and the feed's own loss lives in the
 * all-paths intersection on the group row.
 *
 * Four things deliberately keep a word instead of a number:
 *
 *   - `stalled`, because a series carrying no new values has no count to report, and "0 lost" over
 *     nothing is the clean bill of health this column exists to withhold. Time still decides WHEN
 *     to call a series dead; it just never sizes the loss.
 *   - `advancing`, for a line every instance of which went unchecked for loss. The top-of-book
 *     capture plane is the case: its wire sequence is dense, but the recorder persists one row per
 *     change to the top of the book, so the numbering reconstructed from the stored rows has
 *     structural holes — measured at 1,292 on perps ch1 at each of three independent recorders,
 *     which is the proof they are not loss.
 *   - `ok`, for a line that WAS checked and has no magnitude to report: the recorder's own gap
 *     markers cover the top-of-book series they wrote, and a marker is a fault count with no
 *     per-instrument numbering under it to turn into a figure.
 *   - `gapped` with no count, for the case such a marker was written.
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
    const detail = total > 1 ? `×${total}` : ''
    // **The count was supposed to exist and does not.** A failed per-instrument loss query
    // leaves every market-by-price lane at 0/0 with its gap markers intact, so the verdict falls
    // back to the marker alone and this branch would otherwise print the greenest thing on the
    // page over the one state where nothing was measured — the marker-only false negative the
    // unit change exists to end. It outranks both words below: those describe what a plane can
    // measure, this is a plane that can and this time did not.
    if (sequence.instances.some((i) => i.loss_unavailable)) {
      return { label: 'not counted', tone: 'muted', detail }
    }
    // **No magnitude is not the same as no reading**, and collapsing the two was a regression:
    // every top-of-book line read muted, including the ones the recorder's own gap markers had
    // checked and found clean. Those instances carry gaps_measured: true and no update counters,
    // because that plane has a marker and no per-instrument numbering to count holes in — they
    // were checked, they just cannot be sized. gaps_unmeasured is what separates them, and it is
    // the same rule this badge has always used: 'advancing' only where EVERY instance behind it
    // went unchecked, since a mixed set is still reporting a real zero for the half that was.
    if (total > 0 && (sequence.gaps_unmeasured ?? 0) >= total) {
      return { label: 'advancing', tone: 'muted', detail }
    }
    return { label: 'ok', tone: 'good', detail }
  }

  const expected = loss.received + loss.missing
  const rate =
    expected >= SEQUENCE_LOSS_MIN_UPDATES
      ? `${loss.ppm.toFixed(loss.ppm >= 100 ? 0 : 1)} ppm`
      : `of ${formatUpdateCount(expected)}`

  if (loss.missing > 0) {
    return { label: `${loss.missing.toLocaleString()} lost`, tone: 'bad', detail: rate }
  }
  // Zero holes and a marker still standing is not a clean series: a gap marker can be written at a
  // reset boundary the sequence partition already separated, so the numbering shows no hole while a
  // book was still left un-anchored. Painting that green would hide the one fault the backend is
  // still asserting, so the badge keeps the gapped tone and the count moves into the tooltip, which
  // is where '0 lost, 1 book un-anchored' can be said in full.
  if (sequence.status === 'gapped') {
    return { label: 'gapped', tone: 'bad', detail: `${sequence.gapped}/${total}` }
  }
  // A measured zero is the common state — eight of twenty-four quarter-hours on mainnet — so it has
  // to read as a reading rather than as a blank. The denominator is what makes it one.
  return { label: '0 lost', tone: 'good', detail: `${formatUpdateCount(loss.received)} upd` }
}
