import { describe, expect, it } from 'vitest'
import { divergenceHeadline } from './flexalgo-divergence-text'
import type { AlgoDivergenceResponse } from '@/lib/api'

function summary(
  over: Partial<AlgoDivergenceResponse['summary']> = {}
): AlgoDivergenceResponse['summary'] {
  return {
    activatedLinks: 165,
    excludedLinks: 3,
    multicastPairs: 378,
    divergingPairs: 49,
    unreachablePairs: 0,
    maxDeltaMs: 76.85,
    ...over,
  }
}

describe('divergenceHeadline', () => {
  it('reports the cost, not just the count of untagged links', () => {
    const out = divergenceHeadline(summary())
    expect(out).toContain('3 of 165 activated links')
    expect(out).toContain('49 of 378 metro pairs')
    expect(out).toContain('76.85 ms')
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
