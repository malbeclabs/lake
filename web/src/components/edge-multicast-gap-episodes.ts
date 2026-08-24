import type { EdgeMulticastChannelInstance, GapEpisode } from '@/lib/api'

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
    sinceLastSeconds: last ? Math.max(0, Math.round(windowEnd / 1000) - (last.start + last.seconds)) : undefined,
    worstRecoverySeconds: episodes.reduce((n, e) => Math.max(n, e.seconds), 0),
  }
}
