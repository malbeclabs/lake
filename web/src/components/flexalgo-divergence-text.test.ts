import { describe, expect, it } from 'vitest'
import { divergenceHeadline, excludedForText } from './flexalgo-divergence-text'
import type { AlgoDivergenceResponse } from '@/lib/api'

function summary(
  over: Partial<AlgoDivergenceResponse['summary']> = {}
): AlgoDivergenceResponse['summary'] {
  return {
    // Plausible figures, not a record of what an environment returned. These are
    // live counts on a network that changes daily: links deactivate and metros
    // leave the graph, so a fixture chased against the endpoint goes stale within
    // a day and its drift then reads like a regression. What the tests below pin
    // is how the sentence is assembled from a summary, never today's numbers.
    activatedLinks: 164,
    excludedLinks: 3,
    multicastPairs: 378,
    divergingPairs: 33,
    unreachablePairs: 0,
    maxDeltaMs: 77.11,
    ...over,
  }
}

describe('divergenceHeadline', () => {
  it('reports the cost, not just the count of untagged links', () => {
    const out = divergenceHeadline(summary())
    expect(out).toContain('3 of 164 activated links')
    expect(out).toContain('33 of 378 metro pairs')
    expect(out).toContain('77.11 ms')
  })

  it('says the network is consistent when nothing is excluded', () => {
    const out = divergenceHeadline(summary({ excludedLinks: 0 }))
    expect(out).toContain('same path everywhere')
    // No alarm wording when there is nothing wrong.
    expect(out).not.toContain('outside')
  })

  it('does not claim a cost when excluded links move no pair', () => {
    // A link can sit outside the topology and still change nothing, because
    // no pair's best path went through it.
    const out = divergenceHeadline(summary({ divergingPairs: 0, maxDeltaMs: 0 }))
    expect(out).toContain('no metro pair changes its best path')
    expect(out).not.toContain('up to')
  })

  it('calls out pairs unicast cannot reach at all', () => {
    const out = divergenceHeadline(summary({ unreachablePairs: 2 }))
    expect(out).toContain('2 of them with no unicast path at all')
  })
})

describe('excludedForText', () => {
  it('states a plain age for a link that was once in the topology', () => {
    expect(excludedForText({ everIncluded: true, excludedFor: '89d' })).toBe('89d')
  })

  // The API dates such a link from the oldest snapshot it holds, not from the
  // moment it left, so the age is bounded by retention. A bare "18h" beside
  // "never in the topology" reads as a regression from this morning.
  it('marks the age as a floor when the link was never in the topology', () => {
    expect(excludedForText({ everIncluded: false, excludedFor: '18h' })).toBe('at least 18h')
  })

  it('reports no age at all rather than an empty cell', () => {
    expect(excludedForText({ everIncluded: false, excludedFor: '' })).toBe('—')
  })
})
