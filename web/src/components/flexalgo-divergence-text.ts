import type { AlgoDivergenceResponse } from '@/lib/api'

// Kept out of flexalgo-divergence.tsx so that file exports only components:
// mixing component and non-component exports breaks Fast Refresh.

/**
 * States the finding in one sentence, because the number that matters is not
 * how many links are untagged but how far they move real routes.
 */
export function divergenceHeadline(s: AlgoDivergenceResponse['summary']): string {
  if (s.excludedLinks === 0) {
    return `Every one of the ${s.activatedLinks} activated links is in the unicast topology. Unicast and multicast take the same path everywhere.`
  }

  const links = `${s.excludedLinks} of ${s.activatedLinks} activated links sit outside the unicast topology`

  if (s.divergingPairs === 0) {
    return `${links}, and no metro pair changes its best path because of them.`
  }

  const parts = [
    `${links}. They move ${s.divergingPairs} of ${s.multicastPairs} metro pairs`,
  ]
  if (s.maxDeltaMs > 0) {
    parts.push(`, by up to ${s.maxDeltaMs.toFixed(2)} ms`)
  }
  if (s.unreachablePairs > 0) {
    parts.push(
      `, and leave ${s.unreachablePairs} of them with no unicast path at all`
    )
  }
  return `${parts.join('')}.`
}
