import { describe, it, expect } from 'vitest'
import {
  completeness,
  gapEpisodeStats,
  mergeGapEpisodes,
  sequenceLoss,
  sequenceVerdict,
} from './edge-multicast-gap-episodes'
import type {
  EdgeMulticastChannelInstance,
  EdgeMulticastSequenceHealth,
} from '@/lib/api'

function instance(
  over: Partial<EdgeMulticastChannelInstance> = {},
): EdgeMulticastChannelInstance {
  return {
    capture_source: 'mbp_edge_kalshi_perps',
    channel_id: 1,
    node: 'aws-cmh-mn-recorder1',
    messages: 1000,
    gap_books: 0,
    resets: 0,
    snapshot_cycles: 0,
    last_seen: '2026-08-24T14:57:00Z',
    status: 'ok',
    gaps_measured: true,
    ...over,
  }
}

describe('mergeGapEpisodes', () => {
  it('unions the instances of one line', () => {
    // The sports publisher carries one instance per league. A strip per instance would be thirty
    // strips, so the line is asked the single question the union answers.
    expect(
      mergeGapEpisodes([
        instance({ gap_episodes: [{ start: 100, seconds: 2 }] }),
        instance({ channel_id: 2, gap_episodes: [{ start: 500, seconds: 1 }] }),
      ]),
    ).toEqual([
      { start: 100, seconds: 2 },
      { start: 500, seconds: 1 },
    ])
  })

  it('folds overlapping and touching runs into one outage', () => {
    expect(
      mergeGapEpisodes([
        instance({ gap_episodes: [{ start: 100, seconds: 3 }] }),
        // Starts inside the first, and ends past it.
        instance({ channel_id: 2, gap_episodes: [{ start: 102, seconds: 3 }] }),
        // Starts exactly where that one ended: contiguous, not a second outage.
        instance({ channel_id: 3, gap_episodes: [{ start: 105, seconds: 1 }] }),
      ]),
    ).toEqual([{ start: 100, seconds: 6 }])
  })

  it('keeps a clean second between two losses', () => {
    // The hole is the recovery. Folding across it would report one outage twice as long as any
    // that happened.
    expect(
      mergeGapEpisodes([instance({ gap_episodes: [{ start: 100, seconds: 1 }] }), instance({ channel_id: 2, gap_episodes: [{ start: 102, seconds: 1 }] })]),
    ).toEqual([
      { start: 100, seconds: 1 },
      { start: 102, seconds: 1 },
    ])
  })

  it('ignores a plane that measures no gaps', () => {
    // Top-of-book has no gap marker. Its empty episode list is an absence of measurement, and
    // letting it through would put a clean run on a series nothing checked.
    expect(
      mergeGapEpisodes([
        instance({ gaps_measured: false, gap_episodes: [{ start: 100, seconds: 1 }] }),
      ]),
    ).toEqual([])
  })

  it('is empty for a line that lost nothing', () => {
    expect(mergeGapEpisodes([instance()])).toEqual([])
  })

  it('does not mutate the episodes it was given', () => {
    // The page holds the query payload; folding in place would corrupt it for the next render.
    const first = instance({ gap_episodes: [{ start: 100, seconds: 3 }] })
    const second = instance({ channel_id: 2, gap_episodes: [{ start: 101, seconds: 5 }] })
    mergeGapEpisodes([first, second])
    expect(first.gap_episodes).toEqual([{ start: 100, seconds: 3 }])
    expect(second.gap_episodes).toEqual([{ start: 101, seconds: 5 }])
  })
})

describe('gapEpisodeStats', () => {
  // A 900s window ending at this instant, so "since last gap" is checkable by hand.
  const windowEnd = 1_756_000_000_000
  const endSec = windowEnd / 1000

  it('reports a clean window as fully gap-free', () => {
    const s = gapEpisodeStats([], 900, windowEnd)
    expect(s.gapFree).toBe(1)
    expect(s.perHour).toBe(0)
    expect(s.worstRecoverySeconds).toBe(0)
    // Never gapped and just recovered must not render the same. Undefined is the difference.
    expect(s.sinceLastSeconds).toBeUndefined()
  })

  it('derives the operational figures from the episodes and the window alone', () => {
    const s = gapEpisodeStats(
      [
        { start: endSec - 600, seconds: 5 },
        { start: endSec - 120, seconds: 3 },
      ],
      900,
      windowEnd,
    )
    expect(s.episodes).toBe(2)
    expect(s.lostSeconds).toBe(8)
    // 8 of 900 seconds carried a gap.
    expect(s.gapFree).toBeCloseTo(1 - 8 / 900, 6)
    // Two episodes in a quarter hour extrapolates to eight an hour.
    expect(s.perHour).toBeCloseTo(8, 6)
    // The window ends 120s after the last episode started, and it ran 3s.
    expect(s.sinceLastSeconds).toBe(117)
    // A book stays un-anchored until a snapshot re-anchors it, so the longest episode is the
    // worst recovery the window saw.
    expect(s.worstRecoverySeconds).toBe(5)
  })

  // The window end carries milliseconds and the seconds conversion FLOORS them. Rounding up moves
  // the end of the window past where it is, which prints a longer quiet stretch since the last
  // episode than was measured — the one direction an age must never err in.
  it('floors a fractional window end rather than rounding it up', () => {
    const endSec = 1_800_000_000
    const episodes = [{ start: endSec - 120, seconds: 3 }]
    // .900 of a second past the boundary: rounding would report 118s since the last episode.
    expect(gapEpisodeStats(episodes, 900, endSec * 1000 + 900).sinceLastSeconds).toBe(117)
    // .100 past it floors to the same second, so the two agree.
    expect(gapEpisodeStats(episodes, 900, endSec * 1000 + 100).sinceLastSeconds).toBe(117)
  })

  it('never reports a negative gap-free share', () => {
    // The payload's clock and its window can disagree. A saturated 0 is wrong; a negative
    // percentage on the page is worse.
    const s = gapEpisodeStats([{ start: endSec - 100, seconds: 500 }], 100, windowEnd)
    expect(s.gapFree).toBe(0)
  })

  it('is inert on a zero-width window', () => {
    const s = gapEpisodeStats([{ start: endSec, seconds: 1 }], 0, windowEnd)
    expect(s.gapFree).toBe(0)
    expect(s.perHour).toBe(0)
  })
})

describe('sequenceLoss', () => {
  it('sums a line across its instances and derives the rates', () => {
    const s = sequenceLoss(
      [
        instance({ updates_received: 900_000, updates_missing: 90, seq_gap_events: 30, max_gap_messages: 4, p99_gap_messages: 3 }),
        instance({ channel_id: 2, updates_received: 100_000, updates_missing: 10, seq_gap_events: 5, max_gap_messages: 6, p99_gap_messages: 5 }),
      ],
      900,
    )
    expect(s?.received).toBe(1_000_000)
    expect(s?.missing).toBe(100)
    expect(s?.events).toBe(35)
    // Parts per million of what should have arrived: received + missing, not received.
    expect(s?.ppm).toBeCloseTo((100 / 1_000_100) * 1e6, 3)
    // 100 missing over a fifteen-minute window.
    expect(s?.perMinute).toBeCloseTo(100 / 15, 6)
    // A line is as bad as its worst series.
    expect(s?.maxGap).toBe(6)
    expect(s?.p99Gap).toBe(5)
  })

  it('is undefined when nothing measured it', () => {
    // Top-of-book rows carry no per-instrument sequence. Reporting 0 ppm for them would be the
    // false clean bill of health the whole column refuses to give.
    expect(sequenceLoss([instance()], 900)).toBeUndefined()
    expect(sequenceLoss([instance({ updates_received: 0, updates_missing: 0 })], 900)).toBeUndefined()
  })

  it('reports a measured clean line as zero rather than absent', () => {
    const s = sequenceLoss([instance({ updates_received: 500, updates_missing: 0 })], 900)
    expect(s?.ppm).toBe(0)
    expect(s?.missing).toBe(0)
  })

  it('ignores an instance with no denominator while keeping one that has it', () => {
    const s = sequenceLoss(
      [instance(), instance({ channel_id: 2, updates_received: 100, updates_missing: 1 })],
      900,
    )
    expect(s?.received).toBe(100)
    expect(s?.missing).toBe(1)
  })
})

describe('completeness', () => {
  it('rolls the group up into ppm lost and unprotected seconds', () => {
    const c = completeness({
      status: 'gapped',
      gapped: 1,
      stalled: 0,
      instances: [
        instance({ updates_received: 900_000, updates_missing: 90 }),
        instance({ channel_id: 2, updates_received: 100_000, updates_missing: 10 }),
      ],
      all_paths_gapped: [
        { start: 100, seconds: 4 },
        { start: 500, seconds: 2 },
      ],
    })
    // Parts per million of what should have ARRIVED: received + missing, not received.
    expect(c.ppm).toBeCloseTo((100 / 1_000_100) * 1e6, 3)
    expect(c.missing).toBe(100)
    expect(c.expected).toBe(1_000_100)
    expect(c.unprotectedSeconds).toBe(6)
  })

  it('has no ppm when nothing measured it', () => {
    // Top-of-book carries no per-instrument sequence, so a feed recorded only there has no
    // completeness figure. Zero would read as a clean bill of health that nothing established.
    const c = completeness({ status: 'ok', gapped: 0, stalled: 0, instances: [instance()] })
    expect(c.ppm).toBeUndefined()
    expect(c.unprotectedSeconds).toBe(0)
  })

  it('reports a measured clean feed as zero, not as absent', () => {
    const c = completeness({
      status: 'ok',
      gapped: 0,
      stalled: 0,
      instances: [instance({ updates_received: 500, updates_missing: 0 })],
    })
    expect(c.ppm).toBe(0)
  })

  it('is empty for a group with no sequence at all', () => {
    const c = completeness(undefined)
    expect(c.ppm).toBeUndefined()
    expect(c.missing).toBe(0)
    expect(c.unprotectedSeconds).toBe(0)
  })
})

describe('sequenceVerdict', () => {
  const health = (
    instances: EdgeMulticastChannelInstance[],
    over: Partial<EdgeMulticastSequenceHealth> = {},
  ): EdgeMulticastSequenceHealth => ({
    status: 'ok',
    gapped: 0,
    stalled: 0,
    instances,
    ...over,
  })

  it('reports the values lost, not the books they were lost from', () => {
    // Measured on mainnet over six hours: perps read 13 gapped books — its whole instrument count —
    // against 3,439 updates lost, while ncaaf read 1,934 books against 2,693. The badge used to
    // rank those two the wrong way round.
    const v = sequenceVerdict(
      health(
        [
          instance({
            gap_books: 13,
            updates_received: 6851764,
            updates_missing: 3439,
            seq_gap_events: 63,
            status: 'gapped',
          }),
        ],
        { status: 'gapped', gapped: 1 },
      ),
      900,
    )
    expect(v.label).toBe('3,439 lost')
    expect(v.tone).toBe('bad')
    expect(v.detail).toBe('502 ppm')
  })

  it('calls a measured zero a reading, with its denominator', () => {
    // Eight of twenty-four quarter-hours on mainnet held no loss at all, so this is the common
    // state and it has to read as a measurement rather than as a blank.
    const v = sequenceVerdict(
      health([instance({ updates_received: 4476494, updates_missing: 0 })]),
      900,
    )
    expect(v.label).toBe('0 lost')
    expect(v.tone).toBe('good')
    expect(v.detail).toBe('4.48M upd')
  })

  it('withholds the rate under the volume floor and still shows the count', () => {
    // A ppm over a thin channel is noise wearing a percentage: ncaawb ch116 read 7,475 ppm off
    // 4,647 updates, which is not a worse feed than tennis at 470 ppm over 28M.
    const v = sequenceVerdict(
      health([instance({ updates_received: 100, updates_missing: 3, status: 'gapped' })], {
        status: 'gapped',
        gapped: 1,
      }),
      900,
    )
    expect(v.label).toBe('3 lost')
    expect(v.detail).toBe('of 103')
    expect(v.detail).not.toContain('ppm')
  })

  it('says the top-of-book plane was not counted rather than reporting a zero', () => {
    // Its stored rows hold one entry per change to the top of the book, so the numbering
    // reconstructed from them has structural holes — 1,292 on perps ch1 at each of three
    // independent recorders, which is what proves they are not loss.
    const v = sequenceVerdict(
      health([
        instance({ gaps_measured: false, updates_received: undefined }),
        instance({ gaps_measured: false, updates_received: undefined, channel_id: 101 }),
      ]),
      900,
    )
    expect(v.label).toBe('not counted')
    expect(v.tone).toBe('muted')
    expect(v.detail).toBe('×2')
  })

  it('reads a stall before it reads the counters', () => {
    // A series carrying no new values has no count to report, and "0 lost" over a dead window is
    // the false clean bill of health this column exists to withhold.
    const v = sequenceVerdict(
      health([instance({ updates_received: 1000, updates_missing: 0, status: 'stalled' })], {
        status: 'stalled',
        stalled: 1,
      }),
      900,
    )
    expect(v.label).toBe('stalled')
    expect(v.tone).toBe('warn')
    expect(v.detail).toBe('1/1')
  })

  it('keeps a gap marker that had no countable numbering behind it', () => {
    const v = sequenceVerdict(
      health([instance({ gap_books: 2, status: 'gapped' })], { status: 'gapped', gapped: 1 }),
      900,
    )
    expect(v.label).toBe('gapped')
    expect(v.tone).toBe('bad')
  })
})
