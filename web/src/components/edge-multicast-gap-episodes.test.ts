import { describe, it, expect } from 'vitest'
import { mergeGapEpisodes } from './edge-multicast-gap-episodes'
import type { EdgeMulticastChannelInstance } from '@/lib/api'

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
