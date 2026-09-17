import { describe, expect, it } from 'vitest'

import type { EdgeScoreboardLeader, EdgeScoreboardSlotRace } from '@/lib/api'
import { leaderName, leaderRuns, slotWinRatesVsTurbine } from './shreds-leader-runs'

function race(slot: number, host: string, feed: string, win_pct: number): EdgeScoreboardSlotRace {
  return { host, slot, feed, shreds_won: 0, win_pct }
}

function leader(pubkey: string, name?: string): EdgeScoreboardLeader {
  return { pubkey, name }
}

describe('slotWinRatesVsTurbine', () => {
  it('uses the dz_edge aggregate instead of its parts, never both', () => {
    // Counting dz_edge AND dz/dz_root/dz_retransmit doubles DoubleZero's share
    // and pushes every slot to a fabricated ~100%.
    const w = slotWinRatesVsTurbine([
      race(1, 'a', 'dz_edge', 75),
      race(1, 'a', 'dz', 50),
      race(1, 'a', 'dz_root', 25),
      race(1, 'a', 'turbine', 25),
    ])
    expect(w.get(1)).toBeCloseTo(75, 6)
  })

  it('falls back to the parts when the payload carries no dz_edge row', () => {
    // This is the live shape: recent_slots has dz, dz_root, dz_retransmit, turbine.
    const w = slotWinRatesVsTurbine([
      race(1, 'a', 'dz', 60),
      race(1, 'a', 'dz_root', 20),
      race(1, 'a', 'turbine', 20),
    ])
    expect(w.get(1)).toBeCloseTo(80, 6)
  })

  it('sums across every node that saw the slot', () => {
    const w = slotWinRatesVsTurbine([
      race(1, 'a', 'dz', 100),
      race(1, 'a', 'turbine', 0),
      race(1, 'b', 'dz', 0),
      race(1, 'b', 'turbine', 100),
    ])
    expect(w.get(1)).toBeCloseTo(50, 6)
  })

  it('omits a slot nothing was recorded for', () => {
    expect(slotWinRatesVsTurbine([race(1, 'a', 'dz', 0), race(1, 'a', 'turbine', 0)]).has(1)).toBe(false)
  })
})

describe('leaderRuns', () => {
  const races = (slots: number[]) => slots.flatMap((s) => [race(s, 'a', 'dz', 90), race(s, 'a', 'turbine', 10)])

  it('groups a leader consecutive slots into one run', () => {
    const runs = leaderRuns(races([10, 11, 12, 13]), {
      10: leader('AAA'), 11: leader('AAA'), 12: leader('AAA'), 13: leader('AAA'),
    })
    expect(runs).toHaveLength(1)
    expect(runs[0].slots.map((s) => s.slot)).toEqual([10, 11, 12, 13])
    expect(runs[0].winPct).toBeCloseTo(90, 6)
  })

  it('starts a new run when the leader changes', () => {
    const runs = leaderRuns(races([10, 11, 12, 13]), {
      10: leader('AAA'), 11: leader('AAA'), 12: leader('BBB'), 13: leader('BBB'),
    })
    expect(runs.map((r) => r.pubkey)).toEqual(['AAA', 'BBB'])
  })

  it('splits the same leader across a gap rather than fusing two turns', () => {
    // A leader scheduled again later in the buffer is a second run, not one
    // long card spanning everyone in between.
    const runs = leaderRuns(races([10, 11, 40, 41]), {
      10: leader('AAA'), 11: leader('AAA'), 40: leader('AAA'), 41: leader('AAA'),
    })
    expect(runs).toHaveLength(2)
    expect(runs[1].slots[0].slot).toBe(40)
  })

  it('drops a slot with no known leader without splitting the run around it', () => {
    // A missing entry in the leader map is a hole in the map, not a change of
    // leader — slots 10 and 12 are still one turn.
    const runs = leaderRuns(races([10, 11, 12]), { 10: leader('AAA'), 12: leader('AAA') })
    expect(runs).toHaveLength(1)
    expect(runs[0].slots.map((s) => s.slot)).toEqual([10, 12])
  })

  it('does not fuse two leaders that a dropped slot sits between', () => {
    const runs = leaderRuns(races([10, 11, 12]), { 10: leader('AAA'), 12: leader('BBB') })
    expect(runs.map((r) => r.pubkey)).toEqual(['AAA', 'BBB'])
  })

  it('averages only the slots that had a reading', () => {
    const mixed = [
      race(10, 'a', 'dz', 100), race(10, 'a', 'turbine', 0),
      race(11, 'a', 'dz', 0), race(11, 'a', 'turbine', 0),
    ]
    const runs = leaderRuns(mixed, { 10: leader('AAA'), 11: leader('AAA') })
    expect(runs[0].slots.map((s) => s.winPct)).toEqual([100, null])
    expect(runs[0].winPct).toBe(100)
  })

  it('is empty when the payload carries no leader map', () => {
    expect(leaderRuns(races([10, 11]), undefined)).toEqual([])
  })
})

describe('leaderName', () => {
  const run = (pubkey: string, name?: string) =>
    leaderRuns([race(1, 'a', 'dz', 90), race(1, 'a', 'turbine', 10)], { 1: leader(pubkey, name) })[0]

  it('prefers the published name', () => {
    expect(leaderName(run('AAAA1111BBBB2222', 'Jupiter'))).toBe('Jupiter')
  })

  it('falls back to a short pubkey, including for a blank name', () => {
    expect(leaderName(run('AAAA1111BBBB2222'))).toBe('AAAA…2222')
    expect(leaderName(run('AAAA1111BBBB2222', '   '))).toBe('AAAA…2222')
  })
})
